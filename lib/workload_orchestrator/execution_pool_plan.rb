# frozen_string_literal: true

require_relative "plan"
require_relative "execution_profile"
require_relative "paid_budget"
require_relative "rpof_readiness"

module WorkloadOrchestrator
  # Frozen join of domain intent and execution policy. Never imports AFW models.
  class ExecutionPoolPlan
    REQUEST_VERSION = "wlo-rpof-execution-pool-fulfill-request/v0.1"
    attr_reader :budget, :plan_sha256, :profile_sha256, :requests

    def initialize(plan:, profile:, budget:)
      plan = Plan.new(plan.bytes.dup)
      profile = ExecutionProfile.new(profile.bytes)
      profile.bind(plan)
      unless profile.rpof? && budget.identity["plan_sha256"] == plan.sha256 &&
             budget.execution_profile_sha256 == profile.sha256
        raise Error, "fulfillment requires a budget bound to this exact logical plan and execution profile"
      end
      @budget = budget
      @pools = plan.pools.to_h { |pool| [pool.id, pool] }
      @bindings = profile.document.fetch("pools").to_h { |row| [row.fetch("pool_id"), row] }
      @plan_sha256 = plan.sha256.freeze
      @profile_sha256 = profile.sha256.freeze
      rows = profile.document.fetch("pools").select { |row| row.fetch("backend") == "rpof" }
      total = rows.sum { |row| row.fetch("max_hourly_rate_usd") }
      if !total.finite? || total > budget.document.fetch("max_hourly_rate_usd")
        raise Error, "simultaneous pool hourly ceilings exceed the aggregate budget ceiling"
      end
      @requests = rows.to_h { |row| [row.fetch("pool_id"), build_request(plan, row)] }
      handles = @requests.values.map { |request| execution_handle(request) }
      raise Error, "provider execution handle collision" unless handles.uniq == handles
      deep_freeze(@requests)
      freeze
    end

    def request(pool_id)
      requests.fetch(pool_id) { raise Error, "unknown RPOF pool #{pool_id.inspect}" }
    end

    def preview
      { "plan_sha256" => plan_sha256, "profile_sha256" => profile_sha256,
        "bounds" => budget.bounds, "requests" => requests }
    end

    def readiness(pool_id, target)
      binding = @bindings.fetch(pool_id).merge("target" => target)
      RpofReadiness.new(@pools.fetch(pool_id), binding)
    end

    # Explicit compatibility with RPOF 2f2d094; logical IDs stay in WLO evidence.
    def execution_handle(request)
      "ep-#{request.fetch('pool_id')}-#{plan_sha256[0, 10]}"
    end

    private

    def build_request(plan, row)
      raise Error, "fulfillment cannot adopt a profile target; use read-only readiness for existing fleets" if row.key?("target")
      pool = plan.pool(row.fetch("pool_id"))
      unless pool.required_labels.empty?
        raise Error, "RPOF fulfillment cannot prove arbitrary logical pool labels"
      end
      model = pool.ollama_requirement
      raise Error, "RPOF fulfillment requires exact Ollama model/digest requirements" unless model
      name = model.fetch("model")
      unless name.is_a?(String) && !name.strip.empty? && name.length <= 256 && !name.include?("\0")
        raise Error, "RPOF model name must be nonempty NUL-free text of at most 256 characters"
      end
      context = model["required_context_length"]
      unless context.is_a?(Integer) && context.positive? && model["require_fully_gpu_resident"] == true
        raise Error, "RPOF pool #{row.fetch('pool_id')} requires plan context length and require_fully_gpu_resident=true"
      end
      raise Error, "RPOF fulfillment protocol cannot constrain required_gpu_id" if model.key?("required_gpu_id")
      if budget.identity.fetch("budget_id").length > 256
        raise Error, "RPOF budget_id must be at most 256 characters"
      end
      identity = [budget.identity.fetch("budget_id"), plan_sha256, profile_sha256, row.fetch("pool_id")]
      {
        "contract_version" => REQUEST_VERSION, "plan_sha256" => plan_sha256,
        "budget" => budget.document,
        "pool_id" => Digest::SHA256.hexdigest(JSON.generate(identity))[0, 16],
        "requirements" => {
          "ollama_model" => name, "pull_model" => name,
          "expected_digest" => model.fetch("expected_digest"),
          "required_context_length" => context, "require_fully_gpu_resident" => true
        },
        "capacity" => {
          "desired_workers" => row.fetch("desired_workers"), "minimum_workers" => row.fetch("min_workers"),
          "max_pool_hourly_usd" => row.fetch("max_hourly_rate_usd"),
          "max_total_hourly_usd" => budget.document.fetch("max_hourly_rate_usd")
        }
      }
    end

    def deep_freeze(value)
      case value
      when Hash then value.each { |key, item| deep_freeze(key); deep_freeze(item) }
      when Array then value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end
  end
end
