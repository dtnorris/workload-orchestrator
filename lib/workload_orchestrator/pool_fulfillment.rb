# frozen_string_literal: true

require_relative "rpof_capacity_client"
require_relative "paid_budget_lifecycle"
require "time"

module WorkloadOrchestrator
  # Capacity is consumed inside the block while the original guardian budget
  # and heartbeat lifecycle remain active. A deliberate retained outcome may be
  # reattached only while that same budget, deadline and capacity remain valid.
  class PoolFulfillment
    VERSION = "wlo-execution-pool-handoff/v0.1"
    Outcome = Struct.new(:value, :retain_capacity, keyword_init: true)

    def initialize(pool_plan:, client:, output_dir:, cleanup_wait_seconds: nil, sleeper: ->(seconds) { sleep(seconds) })
      @plan = pool_plan
      @client = client
      @output = File.expand_path(output_dir)
      @cleanup_wait_seconds = cleanup_wait_seconds
      @sleeper = sleeper
    end

    def with_capacity(authorize_paid: false, resume: false)
      raise Error, "capacity scope requires a block" unless block_given?
      raise Error, "paid fulfillment requires explicit authorize_paid: true" unless authorize_paid == true
      prepare_output!(resume)
      lifecycle = PaidBudgetLifecycle.new(budget: @plan.budget, client: @client,
                                          binding_path: File.join(@output, "budget-binding.json"))
      completed = false
      retained = false
      outcome = nil
      begin
        bounds = lifecycle.start!
        handoffs = resume ? resume_handoffs(lifecycle, bounds) : fulfill_handoffs(lifecycle, bounds)
        outcome = yield(handoffs, lifecycle)
        outcome = Outcome.new(value: outcome, retain_capacity: false) unless outcome.is_a?(Outcome)
        lifecycle.check_for!(budget: @plan.budget, client: @client)
        completed = true
        retained = outcome.retain_capacity == true && outcome.value.to_s == "paused"
        lifecycle.suspend! if retained
        outcome.value
      ensure
        teardown = if lifecycle&.suspended?
                     false
                   else
                     lifecycle&.finish!(reason: completed ? "wlo_#{outcome&.value || 'complete'}" : "wlo_capacity_scope_failed")
                   end
        error = $!
        disposition = if retained
                        { "phase" => "retained_for_pause", "provider_state" => "ARMED" }
                      elsif teardown
                        observe_teardown(lifecycle)
                      else
                        { "phase" => "request_failed", "error" => lifecycle&.last_error&.message }
                      end
        write_session("completed" => completed, "capacity_retained" => retained,
                      "teardown_requested" => teardown == true, "disposition" => disposition,
                      "error" => error&.message, "cleanup_error" => lifecycle&.last_error&.message)
        if !retained && disposition["phase"] != "verified_provider_absence" && !error
          raise Error, "terminal provider cleanup #{disposition['phase']}; original guardian/budget remains authoritative"
        end
      end
    end

    # Called only from the active capacity scope. The provider reuses the same
    # budget and fleet handle; the frozen plan is never replaced or re-armed.
    def admit_worker(pool_id:, handoff:, lifecycle:)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      request = @plan.request(pool_id)
      original = JSON.parse(File.read(File.join(@output, "capacity.json"))).fetch(pool_id)
      raise Error, "capacity changed before admission" unless original == handoff
      snapshot = lifecycle.check_for!(budget: @plan.budget, client: @client)
      verify_handoff!(pool_id, handoff, { "deadline_at_utc" => snapshot.fetch("deadline_at_utc") })
      current = handoff.dig("capacity", "final_workers")
      desired = request.dig("capacity", "desired_workers")
      raise Error, "desired capacity already reached" if current >= desired
      raise Error, "current capacity ownership changed" unless
        verify_ownership!(request, handoff.fetch("target"), snapshot) == handoff.fetch("hourly_rate_usd")
      # Include every active pool and pending reservation, even when the
      # provider ledger contains resources outside this execution's pool.
      assert_hourly_headroom!(request, handoff, snapshot)
      result = @client.fulfill_pool(
        pool_plan: @plan, pool_id: pool_id, lifecycle: lifecycle, authorize_paid: true,
        target_workers: current + 1, expected_initial_workers: current,
        output_dir: pool_output(pool_id, "admission-#{current + 1}")
      )
      unless result.document["ready"] == true && result.document.dig("capacity", "final_workers") == current + 1
        raise Error, "RPOF did not prepare exactly one ready worker"
      end
      target = verify_readiness!(pool_id, request, result.document)
      unless target.fetch("fleet_key") == handoff.dig("target", "fleet_key") &&
             target.fetch("expected_fleet_id") == handoff.dig("target", "expected_fleet_id") &&
             target.fetch("worker_indices") == (1..current + 1).to_a
        raise Error, "admitted worker changed the original fleet identity or worker prefix"
      end
      snapshot = lifecycle.check_for!(budget: @plan.budget, client: @client)
      rate = verify_ownership!(request, target, snapshot)
      sample = [Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, 0.001].max
      updated = handoff.merge("target" => target, "hourly_rate_usd" => rate,
                              "bootstrap_samples_seconds" => handoff.fetch("bootstrap_samples_seconds") + [sample],
                              "status" => result.document.fetch("status"),
                              "capacity" => request.fetch("capacity").merge(
                                "initial_workers" => 0, "final_workers" => current + 1
                              ))
      all = JSON.parse(File.read(File.join(@output, "capacity.json")))
      raise Error, "capacity changed during admission" unless all.fetch(pool_id) == original
      all[pool_id] = updated
      write_json("capacity.json", all)
      updated
    rescue JSON::ParserError, KeyError, SystemCallError => e
      raise Error, "admission evidence invalid: #{e.message}"
    end

    private

    def observe_teardown(lifecycle)
      first = lifecycle.teardown_snapshot
      deadline = Time.iso8601(first.fetch("deadline_at_utc"))
      reserve = @plan.budget.document.fetch("teardown_reserve_seconds")
      limit = [deadline, Time.now.utc + (@cleanup_wait_seconds || reserve)].min
      loop do
        remaining = limit - Time.now.utc
        snapshot = lifecycle.teardown_status(timeout_seconds: [[remaining, 0.001].max, 30].min)
        phase = snapshot.fetch("teardown_phase")
        unless %w[requested in_progress verified_provider_absence].include?(phase)
          raise Error, "invalid terminal provider teardown phase #{phase.inspect}"
        end
        if phase == "verified_provider_absence"
          active = snapshot.fetch("owned_resources").values.any? { |row| row["status"] == "active" }
          pending = snapshot.fetch("reservations").values.any? { |row| row["status"] == "pending" }
          unless snapshot["state"] == "CLOSED" && snapshot["provider_absence_verified_at_utc"] && !active && !pending
            raise Error, "provider absence claim lacks closed, liability-free evidence"
          end
        elsif snapshot["state"] != "TEARDOWN_REQUIRED"
          raise Error, "provider teardown phase is inconsistent with budget state"
        end
        evidence = snapshot.slice("state", "teardown_phase", "teardown_reason", "teardown_required_at_utc",
                                  "teardown_started_at_utc", "provider_absence_verified_at_utc", "closed_at_utc",
                                  "deadline_at_utc", "owned_resources", "reservations")
        return { "phase" => phase, "provider" => evidence } if phase == "verified_provider_absence" || Time.now.utc >= limit
        @sleeper.call([@plan.budget.document.fetch("guardian_poll_seconds"), [limit - Time.now.utc, 0].max].min)
      end
    rescue StandardError => e
      { "phase" => "observation_failed", "error" => e.message,
        "request" => first && first.slice("state", "teardown_reason", "teardown_required_at_utc") }
    end

    def prepare_output!(resume)
      if resume
        unless File.directory?(@output) && File.file?(File.join(@output, "capacity.json"))
          raise Error, "resume requires the original capacity evidence; re-fulfillment is forbidden"
        end
        sessions = [File.join(@output, "session.json"), *Dir.glob(File.join(@output, "sessions", "session-*.json"))]
        latest = sessions.select { |path| File.file?(path) }
                         .max_by { |path| path[%r{session-(\d+)\.json\z}, 1]&.to_i || 1 }
        if latest && JSON.parse(File.read(latest)).fetch("disposition", {})["phase"] != "retained_for_pause"
          raise Error, "terminal capacity disposition forbids paid resume or a fresh budget in this execution"
        end
        return
      end

      FileUtils.mkdir_p(File.dirname(@output))
      Dir.mkdir(@output)
      write_json("intent.json", @plan.preview)
      preflight!
    rescue Errno::EEXIST
      raise Error, "capacity output already exists; use resume to evaluate the original budget/capacity"
    end

    def fulfill_handoffs(lifecycle, bounds)
      handoffs = @plan.requests.keys.to_h do |pool_id|
        [pool_id, fulfill_one(pool_id, lifecycle, bounds)]
      end
      write_json("capacity.json", handoffs)
      handoffs
    end

    def resume_handoffs(lifecycle, bounds)
      handoffs = JSON.parse(File.read(File.join(@output, "capacity.json")))
      unless handoffs.is_a?(Hash) && handoffs.keys.sort == @plan.requests.keys.sort
        raise Error, "persisted capacity handoffs do not match the execution pools"
      end
      handoffs.each do |pool_id, handoff|
        verify_handoff!(pool_id, handoff, bounds)
        target = verify_readiness!(pool_id, @plan.request(pool_id), handoff.fetch("target"))
        raise Error, "persisted capacity target identity changed" unless target == handoff.fetch("target")
        snapshot = lifecycle.check_for!(budget: @plan.budget, client: @client)
        rate = verify_ownership!(@plan.request(pool_id), target, snapshot)
        unless rate == handoff.fetch("hourly_rate_usd")
          raise Error, "persisted capacity hourly rate changed"
        end
      end
      handoffs
    rescue JSON::ParserError, KeyError, SystemCallError => e
      raise Error, "invalid persisted capacity handoff: #{e.message}"
    end

    def verify_handoff!(pool_id, handoff, bounds)
      expected = {
        "contract_version" => VERSION, "pool_id" => pool_id,
        "plan_sha256" => @plan.plan_sha256, "profile_sha256" => @plan.profile_sha256,
        "budget" => @plan.budget.identity, "deadline_at_utc" => bounds.fetch("deadline_at_utc"),
        "requirements" => @plan.request(pool_id).fetch("requirements")
      }
      expected.each do |key, value|
        raise Error, "persisted capacity #{key} mismatch" unless handoff[key] == value
      end
      raise Error, "persisted capacity is not ready" unless %w[ready partial_ready].include?(handoff["status"])
      request_capacity = @plan.request(pool_id).fetch("capacity")
      capacity = handoff["capacity"]
      unless capacity.is_a?(Hash) && request_capacity.all? { |key, value| capacity[key] == value }
        raise Error, "persisted capacity bounds changed"
      end
      final = capacity["final_workers"]
      minimum = capacity.fetch("minimum_workers")
      desired = capacity.fetch("desired_workers")
      unless final.is_a?(Integer) && final.between?(minimum, desired)
        raise Error, "persisted capacity worker count is invalid"
      end
      rate = handoff["hourly_rate_usd"]
      unless rate.is_a?(Numeric) && rate.finite? && rate.positive? &&
             rate <= request_capacity.fetch("max_pool_hourly_usd")
        raise Error, "persisted capacity hourly rate is invalid"
      end
      samples = handoff["bootstrap_samples_seconds"]
      unless samples.is_a?(Array) && samples.length == final - minimum + 1 &&
             samples.all? { |value| value.is_a?(Numeric) && value.finite? && value.positive? }
        raise Error, "persisted bootstrap measurements are invalid"
      end
    end

    def preflight!
      @plan.requests.each_key do |pool_id|
        result = @client.plan_pool(pool_plan: @plan, pool_id: pool_id, output_dir: pool_output(pool_id, "preflight"))
        document = result.document
        unless document["status"] == "planned" && document.dig("capacity", "initial_workers") == 0
          raise Error, "pool #{pool_id} is unavailable or already has capacity; no paid fulfillment started"
        end
      end
    end

    def fulfill_one(pool_id, lifecycle, bounds)
      request = @plan.request(pool_id)
      starter = request.dig("capacity", "minimum_workers")
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = @client.fulfill_pool(pool_plan: @plan, pool_id: pool_id, lifecycle: lifecycle,
                                    output_dir: pool_output(pool_id, "fulfillment"), authorize_paid: true,
                                    target_workers: starter, expected_initial_workers: 0)
      unless result.document["ready"] == true && result.document.dig("capacity", "initial_workers") == 0
        raise Error, "pool #{pool_id} did not produce fresh ready capacity"
      end
      target = verify_readiness!(pool_id, request, result.document)
      snapshot = lifecycle.check_for!(budget: @plan.budget, client: @client)
      hourly_rate = verify_ownership!(request, target, snapshot)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      {
        "contract_version" => VERSION, "pool_id" => pool_id,
        "plan_sha256" => @plan.plan_sha256, "profile_sha256" => @plan.profile_sha256,
        "budget" => @plan.budget.identity, "deadline_at_utc" => bounds.fetch("deadline_at_utc"),
        "target" => target, "hourly_rate_usd" => hourly_rate,
        "status" => result.document.fetch("status"),
        "bootstrap_samples_seconds" => [[elapsed / starter, 0.001].max],
        "capacity" => request.fetch("capacity").merge("initial_workers" => 0, "final_workers" => starter),
        "requirements" => request.fetch("requirements")
      }
    end

    def verify_readiness!(pool_id, request, result)
      target = if result.key?("execution_handle")
                 { "fleet_key" => result.fetch("execution_handle"),
                   "worker_selector" => { "mode" => "indices", "indices" => result.fetch("worker_indices") } }
               else
                 { "fleet_key" => result.fetch("fleet_key"),
                   "worker_selector" => { "mode" => "indices", "indices" => result.fetch("worker_indices") } }
               end
      proof = @plan.readiness(pool_id, target).check(@client)
      write_json(next_readiness_name(request.fetch("pool_id")), proof)
      raise Error, "fulfilled capacity failed fresh readiness check" unless proof["ready"] == true
      { "fleet_key" => target.fetch("fleet_key"), "expected_fleet_id" => proof.fetch("fleet_id"),
        "worker_indices" => proof.fetch("selected_worker_indices") }
    end

    def verify_ownership!(request, target, snapshot)
      resources = snapshot.fetch("owned_resources").values.select do |row|
        row.fetch("status") == "active" && row.fetch("fleet_key") == target.fetch("fleet_key")
      end
      expected = target.fetch("worker_indices").map { |index| "burst_#{index}" }.sort
      actual = resources.map { |row| row.fetch("logical_resource_id") }.sort
      pending = snapshot.fetch("reservations").values.any? { |row| row.fetch("status") == "pending" }
      raise Error, "ready capacity is not exclusively committed to the original budget" if actual != expected || pending
      rate = resources.sum do |row|
        value = row.fetch("hourly_rate_usd")
        unless value.is_a?(Numeric) && value.finite? && value.positive?
          raise Error, "invalid budget-owned worker rate"
        end
        value
      end
      unless rate.finite? && rate <= request.dig("capacity", "max_pool_hourly_usd")
        raise Error, "fulfilled pool exceeds its hourly ceiling"
      end
      total = snapshot.fetch("owned_resources").values.select { |row| row["status"] == "active" }
                      .sum { |row| Float(row.fetch("hourly_rate_usd")) }
      unless total.finite? && total <= request.dig("capacity", "max_total_hourly_usd")
        raise Error, "aggregate owned worker rate exceeds original hourly ceiling"
      end
      rate
    rescue KeyError, NoMethodError => e
      raise Error, "invalid budget resource ownership evidence: #{e.message}"
    end

    def assert_hourly_headroom!(request, handoff, snapshot)
      resources = snapshot.fetch("owned_resources").values.select { |row| row["status"] == "active" }
      pending = snapshot.fetch("reservations").values.select { |row| row["status"] == "pending" }
      raise Error, "pending paid reservations block new admission" unless pending.empty?
      current_pool = resources.select { |row| row["fleet_key"] == handoff.dig("target", "fleet_key") }
      pool_rate = current_pool.sum { |row| Float(row.fetch("hourly_rate_usd")) }
      total_rate = resources.sum { |row| Float(row.fetch("hourly_rate_usd")) }
      raise Error, "invalid paid resource rates" unless pool_rate.finite? && total_rate.finite? &&
                                                        resources.all? { |row| Float(row.fetch("hourly_rate_usd")).positive? }
      candidate = request.dig("capacity", "max_pool_hourly_usd") /
                  request.dig("capacity", "desired_workers").to_f
      # RPOF repeats these checks against the live price and reserves maximum
      # liability atomically before creating the worker.
      if pool_rate + candidate > request.dig("capacity", "max_pool_hourly_usd") ||
         total_rate + candidate > request.dig("capacity", "max_total_hourly_usd") ||
         snapshot.fetch("remaining_uncommitted_budget_usd") <= 0
        raise Error, "admission exceeds original pool, aggregate, or cumulative budget headroom"
      end
    rescue ArgumentError, TypeError, KeyError => e
      raise Error, "invalid rate or reservation evidence: #{e.message}"
    end

    def pool_output(pool_id, phase)
      File.join(@output, @plan.request(pool_id).fetch("pool_id"), phase)
    end

    def next_readiness_name(pool_id)
      base = "#{pool_id}-readiness"
      return "#{base}.json" unless File.exist?(File.join(@output, "#{base}.json"))

      index = 2
      index += 1 while File.exist?(File.join(@output, "#{base}-#{index}.json"))
      "#{base}-#{index}.json"
    end

    def write_session(document)
      path = File.join(@output, "session.json")
      if File.exist?(path)
        directory = File.join(@output, "sessions")
        FileUtils.mkdir_p(directory)
        index = 2
        index += 1 while File.exist?(File.join(directory, "session-#{index}.json"))
        path = File.join(directory, "session-#{index}.json")
      end
      write_json(path, document, absolute: true)
    end

    def write_json(name, document, absolute: false)
      path = absolute ? name : File.join(@output, name)
      Tempfile.create(["wlo-capacity-evidence-", ".json"], File.dirname(path)) do |file|
        file.write(JSON.pretty_generate(document) + "\n")
        file.flush
        file.fsync
        File.rename(file.path, path)
      end
    end
  end
end
