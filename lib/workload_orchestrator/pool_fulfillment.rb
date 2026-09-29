# frozen_string_literal: true

require_relative "rpof_capacity_client"
require_relative "paid_budget_lifecycle"

module WorkloadOrchestrator
  # Capacity is consumed inside the block while the original guardian budget
  # and heartbeat lifecycle remain active. A deliberate retained outcome may be
  # reattached only while that same budget, deadline and capacity remain valid.
  class PoolFulfillment
    VERSION = "wlo-execution-pool-handoff/v0.1"
    Outcome = Struct.new(:value, :retain_capacity, keyword_init: true)

    def initialize(pool_plan:, client:, output_dir:)
      @plan = pool_plan
      @client = client
      @output = File.expand_path(output_dir)
    end

    def with_capacity(authorize_paid: false, resume: false)
      raise Error, "capacity scope requires a block" unless block_given?
      raise Error, "paid fulfillment requires explicit authorize_paid: true" unless authorize_paid == true
      prepare_output!(resume)
      lifecycle = PaidBudgetLifecycle.new(budget: @plan.budget, client: @client,
                                          binding_path: File.join(@output, "budget-binding.json"))
      completed = false
      retained = false
      begin
        bounds = lifecycle.start!
        handoffs = resume ? resume_handoffs(lifecycle, bounds) : fulfill_handoffs(lifecycle, bounds)
        outcome = yield(handoffs, lifecycle)
        outcome = Outcome.new(value: outcome, retain_capacity: false) unless outcome.is_a?(Outcome)
        lifecycle.check_for!(budget: @plan.budget, client: @client)
        completed = true
        retained = outcome.retain_capacity == true
        lifecycle.suspend! if retained
        outcome.value
      ensure
        teardown = if lifecycle&.suspended?
                     false
                   else
                     lifecycle&.finish!(reason: completed ? "wlo_capacity_scope_complete" : "wlo_capacity_scope_failed")
                   end
        error = $!
        write_session("completed" => completed, "capacity_retained" => retained,
                      "teardown_requested" => teardown == true, "error" => error&.message,
                      "cleanup_error" => lifecycle&.last_error&.message)
        if !teardown && !retained && !error
          raise Error, "budget teardown was not acknowledged; guardian fallback remains active"
        end
      end
    end

    private

    def prepare_output!(resume)
      if resume
        unless File.directory?(@output) && File.file?(File.join(@output, "capacity.json"))
          raise Error, "resume requires the original capacity evidence; re-fulfillment is forbidden"
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
      result = @client.fulfill_pool(pool_plan: @plan, pool_id: pool_id, lifecycle: lifecycle,
                                    output_dir: pool_output(pool_id, "fulfillment"), authorize_paid: true)
      unless result.document["ready"] == true && result.document.dig("capacity", "initial_workers") == 0
        raise Error, "pool #{pool_id} did not produce fresh ready capacity"
      end
      target = verify_readiness!(pool_id, request, result.document)
      snapshot = lifecycle.check_for!(budget: @plan.budget, client: @client)
      hourly_rate = verify_ownership!(request, target, snapshot)
      {
        "contract_version" => VERSION, "pool_id" => pool_id,
        "plan_sha256" => @plan.plan_sha256, "profile_sha256" => @plan.profile_sha256,
        "budget" => @plan.budget.identity, "deadline_at_utc" => bounds.fetch("deadline_at_utc"),
        "target" => target, "hourly_rate_usd" => hourly_rate,
        "status" => result.document.fetch("status"), "capacity" => result.document.fetch("capacity"),
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
      rate
    rescue KeyError, NoMethodError => e
      raise Error, "invalid budget resource ownership evidence: #{e.message}"
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
