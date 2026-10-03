# frozen_string_literal: true

require "delegate"
require "digest"
require "json"

module WorkloadOrchestrator
  # Placement policy is separate from workload intent.
  class ExecutionProfile
    CONTRACT_VERSION = "wlo-execution-profile/v0.1"
    BACKENDS = %w[local fixed_remote rpof].freeze
    BUDGET_KEYS = %w[max_hourly_rate_usd max_total_cost_usd max_runtime_seconds].freeze
    COMMON_POOL_KEYS = %w[pool_id backend max_concurrency].freeze

    class BoundPlan < SimpleDelegator
      attr_reader :execution_profile, :pools

      def initialize(plan, profile, pools)
        super(plan)
        @execution_profile = profile
        @pools = pools.freeze
      end

      def pool(id)
        pools.find { |candidate| candidate.id == id }
      end
    end

    attr_reader :bytes, :sha256, :document

    def self.load(path)
      new(File.binread(File.expand_path(path)))
    rescue Errno::ENOENT => e
      raise Error, e.message
    end

    def initialize(bytes)
      @bytes = bytes.dup.freeze
      @sha256 = Digest::SHA256.hexdigest(@bytes)
      @document = JSON.parse(@bytes)
      keys!(document, %w[contract_version pools], "profile", optional: ["budget"])
      unless document["contract_version"] == CONTRACT_VERSION
        raise Error, "execution profile contract must be #{CONTRACT_VERSION}"
      end
      rows = document["pools"]
      raise Error, "profile pools must be a non-empty array" unless rows.is_a?(Array) && !rows.empty?

      rows.each { |row| validate_pool!(row) }
      ids = rows.map { |row| row.fetch("pool_id") }
      raise Error, "duplicate profile pool_id" unless ids.uniq == ids

      validate_budget!
      deep_freeze(document)
    rescue JSON::ParserError => e
      raise Error, "invalid execution-profile JSON: #{e.message}"
    end

    def rpof?
      document.fetch("pools").any? { |row| row.fetch("backend") == "rpof" }
    end

    def binding_for(pool_id)
      document.fetch("pools").find { |row| row.fetch("pool_id") == pool_id }
    end

    # Historical profiles remain inspectable but cannot own production capacity.
    def ensure_runnable!
      raise Error, "historical RPOF execution is retired; use a v0.3 dynamic worker source" if rpof?

      self
    end

    def bind(plan)
      if plan.priority_scheduling?
        raise Error,
              "wlo-execution-plan/v0.3 uses the provider-neutral dynamic worker registry; " \
              "execution profiles are legacy v0.2 compatibility only"
      end
      unless plan.contract_version == Plan::LOGICAL_CONTRACT_VERSION && !plan.execution_profile
        raise Error, "execution profiles require an unbound wlo-execution-plan/v0.2 plan"
      end
      rows = document.fetch("pools").to_h { |row| [row.fetch("pool_id"), row] }
      unless rows.keys.sort == plan.pools.map(&:id).sort
        raise Error, "execution profile must map every logical pool exactly once, with no extra pools"
      end

      pools = plan.pools.map do |pool|
        row = rows.fetch(pool.id)
        Plan::Pool.new(
          id: pool.id,
          worker_names: row.fetch("worker_names", []).freeze,
          required_labels: (pool.required_labels + row.fetch("required_labels", [])).uniq.freeze,
          ollama_requirement: pool.ollama_requirement,
          max_concurrency: row.fetch("max_concurrency")
        ).freeze
      end
      BoundPlan.new(plan, self, pools)
    end

    private

    def validate_pool!(row)
      raise Error, "profile pool must be an object" unless row.is_a?(Hash)

      backend = row["backend"]
      raise Error, "unsupported profile backend #{backend.inspect}" unless BACKENDS.include?(backend)

      if backend == "rpof"
        keys!(row, COMMON_POOL_KEYS + %w[min_workers desired_workers max_hourly_rate_usd], "rpof pool",
              optional: ["target"])
        validate_target!(row["target"]) if row.key?("target")
        %w[min_workers desired_workers].each { |key| positive_integer!(row[key], key) }
        unless row["min_workers"] <= row["desired_workers"]
          raise Error, "min_workers cannot exceed desired_workers"
        end
        positive_number!(row["max_hourly_rate_usd"], "pool max_hourly_rate_usd")
        capacity = row["desired_workers"]
      else
        keys!(row, COMMON_POOL_KEYS + %w[worker_names], "fixed pool", optional: ["required_labels"])
        names = row["worker_names"]
        strings!(names, "worker_names")
        raise Error, "worker_names must be non-empty and unique" if names.empty? || names.uniq != names

        strings!(row.fetch("required_labels", []), "required_labels")
        capacity = names.length
      end
      unless row["pool_id"].is_a?(String) && row["pool_id"].match?(Plan::ID_PATTERN)
        raise Error, "invalid profile pool_id"
      end
      positive_integer!(row["max_concurrency"], "max_concurrency")
      raise Error, "max_concurrency exceeds pool capacity" if row["max_concurrency"] > capacity
    end

    def validate_target!(target)
      keys!(target, %w[fleet_key worker_selector], "rpof target")
      RpofContract.text!(target["fleet_key"], "fleet_key", max: 64, pattern: RpofContract::ID)
      selector = target["worker_selector"]
      raise Error, "worker_selector must be an object" unless selector.is_a?(Hash)

      case selector["mode"]
      when "all"
        keys!(selector, %w[mode], "worker_selector")
      when "indices"
        keys!(selector, %w[mode indices], "worker_selector")
        RpofContract.indices!(selector["indices"])
      else
        raise Error, "worker_selector.mode must be all or indices"
      end
    end

    def validate_budget!
      unless rpof?
        raise Error, "budget is only supported for rpof declarations" if document.key?("budget")

        return
      end
      budget = document["budget"]
      keys!(budget, BUDGET_KEYS, "budget")
      %w[max_hourly_rate_usd max_total_cost_usd].each { |key| positive_number!(budget[key], key) }
      positive_integer!(budget["max_runtime_seconds"], "max_runtime_seconds")
      document.fetch("pools").select { |row| row["backend"] == "rpof" }.each do |row|
        if row["max_hourly_rate_usd"] > budget["max_hourly_rate_usd"]
          raise Error, "pool hourly ceiling exceeds aggregate budget hourly ceiling"
        end
      end
    end

    def keys!(data, required, label, optional: [])
      raise Error, "#{label} must be an object" unless data.is_a?(Hash)

      missing = required - data.keys
      unknown = data.keys - required - optional
      raise Error, "#{label} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise Error, "#{label} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
    end

    def strings!(value, label)
      unless value.is_a?(Array) && value.all? { |item| item.is_a?(String) && !item.strip.empty? }
        raise Error, "#{label} must be an array of non-empty strings"
      end
    end

    def positive_integer!(value, label)
      return if value.is_a?(Integer) && value.positive?

      raise Error, "#{label} must be a positive integer"
    end

    def positive_number!(value, label)
      return if value.is_a?(Numeric) && value.finite? && value.positive?

      raise Error, "#{label} must be a positive finite number"
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
