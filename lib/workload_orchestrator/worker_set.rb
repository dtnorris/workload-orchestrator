# frozen_string_literal: true

module WorkloadOrchestrator
  class WorkerSet
    attr_reader :workers

    def self.load(path)
      data = Config.load_yaml(path)
      rows = data["workers"]
      raise Error, "worker config must contain a non-empty workers mapping" unless rows.is_a?(Hash) && !rows.empty?

      new(rows)
    end

    def initialize(rows)
      @workers = rows.to_h do |name, attrs|
        raise Error, "worker #{name.inspect} must be an object" unless attrs.is_a?(Hash)

        worker = Worker.new(name, attrs)
        [worker.name, worker]
      end.freeze
    end

    def fetch(name)
      workers.fetch(name.to_s) { raise Error, "unknown worker #{name.inspect}" }
    end

    def validate_plan!(plan)
      if plan.logical? && !plan.execution_profile
        raise Error, "logical plan requires --execution-profile FILE"
      end
      validate_pools!(plan.pools.reject { |pool| rpof_pool?(plan, pool) })
      plan
    end

    # Readiness of mixed profiles validates only their fixed worker bindings;
    # RPOF capacity is validated through its signed handoff and fresh proof.
    def validate_pools!(pools)
      pools.each { |pool| validate_pool!(pool) }
    end

    def execution_sha256(plan)
      rows = plan.pools.flat_map(&:worker_names).uniq.sort.map do |name|
        worker = fetch(name)
        [name, worker.type, worker.base_url, worker.labels.sort, worker.hourly_rate_usd, worker.job_env.sort]
      end
      Digest::SHA256.hexdigest(JSON.generate(rows))
    end

    private

    def rpof_pool?(plan, pool)
      plan.execution_profile&.binding_for(pool.id)&.fetch("backend") == "rpof"
    end

    def validate_pool!(pool)
      pool.worker_names.each do |name|
        worker = fetch(name)
        validate_cost!(worker)
        validate_labels!(worker, pool)
        validate_ollama!(worker, pool)
      end
    end

    def validate_cost!(worker)
      return if worker.zero_cost?

      raise Error,
            "WLO v0.1 refuses paid worker #{worker.name.inspect} (#{format('$%.4f/hr', worker.hourly_rate_usd)})"
    end

    def validate_labels!(worker, pool)
      missing = pool.required_labels - worker.labels
      return if missing.empty?

      raise Error, "worker #{worker.name.inspect} is missing required labels: #{missing.join(', ')}"
    end

    def validate_ollama!(worker, pool)
      return unless pool.ollama_requirement
      extended = pool.ollama_requirement.keys & Plan::OLLAMA_CAPABILITY_KEYS
      unless extended.empty?
        raise Error, "pool #{pool.id.inspect}: #{extended.join(', ')} readiness requires the RPOF backend; " \
                     "fixed endpoints cannot verify these requirements"
      end
      return if worker.type == "ollama" && worker.base_url

      raise Error, "pool #{pool.id.inspect} requires Ollama but worker #{worker.name.inspect} is not an Ollama worker"
    end
  end
end
