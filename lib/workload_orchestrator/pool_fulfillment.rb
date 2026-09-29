# frozen_string_literal: true

require_relative "rpof_capacity_client"
require_relative "paid_budget_lifecycle"

module WorkloadOrchestrator
  # Capacity is valid only inside the block while the original guardian budget
  # and heartbeat lifecycle remain active. This does not dispatch jobs.
  class PoolFulfillment
    VERSION = "wlo-execution-pool-handoff/v0.1"

    def initialize(pool_plan:, client:, output_dir:)
      @plan = pool_plan
      @client = client
      @output = File.expand_path(output_dir)
    end

    def with_capacity(authorize_paid: false)
      raise Error, "capacity scope requires a block" unless block_given?
      raise Error, "paid fulfillment requires explicit authorize_paid: true" unless authorize_paid == true
      FileUtils.mkdir_p(File.dirname(@output))
      Dir.mkdir(@output)
      write_json("intent.json", @plan.preview)
      preflight!
      lifecycle = PaidBudgetLifecycle.new(budget: @plan.budget, client: @client,
                                          binding_path: File.join(@output, "budget-binding.json"))
      completed = false
      begin
        bounds = lifecycle.start!
        handoffs = @plan.requests.keys.to_h do |pool_id|
          [pool_id, fulfill_one(pool_id, lifecycle, bounds)]
        end
        write_json("capacity.json", handoffs)
        value = yield(handoffs, lifecycle)
        lifecycle.check_for!(budget: @plan.budget, client: @client)
        completed = true
        value
      ensure
        teardown = lifecycle.finish!(reason: completed ? "wlo_capacity_scope_complete" : "wlo_capacity_scope_failed")
        error = $!
        write_json("session.json", "completed" => completed, "teardown_requested" => teardown,
                                  "error" => error&.message, "cleanup_error" => lifecycle.last_error&.message)
        raise Error, "budget teardown was not acknowledged; guardian fallback remains active" if !teardown && !error
      end
    rescue Errno::EEXIST
      raise Error, "capacity output already exists; automatic retry/resume is not supported"
    end

    private

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
      target = { "fleet_key" => result.fetch("execution_handle"),
                 "worker_selector" => { "mode" => "indices", "indices" => result.fetch("worker_indices") } }
      proof = @plan.readiness(pool_id, target).check(@client)
      write_json("#{request.fetch('pool_id')}-readiness.json", proof)
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

    def write_json(name, document)
      path = File.join(@output, name)
      Tempfile.create(["wlo-capacity-evidence-", ".json"], File.dirname(path)) do |file|
        file.write(JSON.pretty_generate(document) + "\n")
        file.flush
        file.fsync
        File.rename(file.path, path)
      end
    end
  end
end
