# frozen_string_literal: true

module WorkloadOrchestrator
  # Provider-neutral compatibility checks between a validated plan pool and a
  # validated dynamic registry worker. Assignment belongs to the scheduler.
  class CapabilityMatcher
    REASON_ORDER = %w[
      worker_not_ready
      missing_required_label
      missing_capability
      model_mismatch
      digest_mismatch
      context_length_mismatch
      full_gpu_residency_required
      gpu_id_mismatch
    ].freeze

    class Result
      attr_reader :reasons

      def initialize(reasons)
        @reasons = reasons.uniq.sort_by { |reason| REASON_ORDER.index(reason) }.freeze
        freeze
      end

      def compatible?
        reasons.empty?
      end
    end

    attr_reader :plan

    def self.match(worker:, required_labels: [], ollama_requirement: nil)
      new.match(
        worker: worker,
        required_labels: required_labels,
        ollama_requirement: ollama_requirement
      )
    end

    def initialize(plan: nil)
      @plan = plan
    end

    def match(worker:, required_labels: [], ollama_requirement: nil)
      validate_worker!(worker)
      labels = Array(required_labels).map(&:to_s)
      reasons = []
      reasons << "worker_not_ready" unless worker.ready?
      reasons << "missing_required_label" unless (labels - worker.labels).empty?
      reasons.concat(capability_reasons(worker, normalize_requirement!(ollama_requirement))) if ollama_requirement
      Result.new(reasons)
    end

    def match_pool(worker:, pool:)
      unless pool.respond_to?(:required_labels) && pool.respond_to?(:ollama_requirement)
        raise Error, "capability requirement must be a plan pool"
      end

      match(
        worker: worker,
        required_labels: pool.required_labels,
        ollama_requirement: pool.ollama_requirement
      )
    end

    def requirement_for(job:)
      raise Error, "capability matcher requires a plan" unless plan
      raise Error, "job is not part of the capability matcher's plan" unless plan.jobs.include?(job)

      plan.pool(job.pool_id) || raise(Error, "job references an unknown pool #{job.pool_id.inspect}")
    end

    def match_job(job:, worker:)
      match_pool(worker: worker, pool: requirement_for(job: job))
    end

    def compatible_workers(job:, workers:)
      pool = requirement_for(job: job)
      Array(workers)
        .select { |worker| match_pool(worker: worker, pool: pool).compatible? }
        .sort_by(&:execution_identity)
        .freeze
    end

    private

    def validate_worker!(worker)
      return if worker.is_a?(RegistryWorker)

      raise Error, "capability matching requires a RegistryWorker"
    end

    def normalize_requirement!(requirement)
      raise Error, "ollama capability requirement must be an object" unless requirement.is_a?(Hash)

      normalized = requirement.transform_keys(&:to_s)
      required = Plan::OLLAMA_KEYS
      allowed = required + Plan::OLLAMA_CAPABILITY_KEYS
      missing = required - normalized.keys
      unknown = normalized.keys - allowed
      raise Error, "ollama capability requirement is missing field(s): #{missing.join(', ')}" unless missing.empty?
      raise Error, "ollama capability requirement has unknown field(s): #{unknown.join(', ')}" unless unknown.empty?

      normalized
    end

    def capability_reasons(worker, requirement)
      reasons = gpu_reasons(worker, requirement)
      models = worker.ollama_models
      unless models.is_a?(Array) && !models.empty?
        reasons << "missing_capability"
        return reasons
      end

      model = models.find do |candidate|
        candidate.is_a?(Hash) && candidate["model"] == requirement["model"]
      end
      unless model
        reasons << "model_mismatch"
        return reasons
      end

      compare_model_capability(model, requirement, reasons)
    end

    def gpu_reasons(worker, requirement)
      return [] unless requirement.key?("required_gpu_id")
      return ["missing_capability"] if worker.gpu_id.nil?
      return [] if worker.gpu_id == requirement["required_gpu_id"]

      ["gpu_id_mismatch"]
    end

    def compare_model_capability(model, requirement, reasons)
      compare_field(model, "digest", requirement.fetch("expected_digest"), "digest_mismatch", reasons)
      if requirement.key?("required_context_length")
        compare_field(
          model,
          "context_length",
          requirement.fetch("required_context_length"),
          "context_length_mismatch",
          reasons
        )
      end
      if requirement["require_fully_gpu_resident"]
        if !model.key?("fully_gpu_resident")
          reasons << "missing_capability"
        elsif model["fully_gpu_resident"] != true
          reasons << "full_gpu_residency_required"
        end
      end
      reasons
    end

    def compare_field(record, field, required_value, mismatch_reason, reasons)
      if !record.key?(field)
        reasons << "missing_capability"
      elsif record[field] != required_value
        reasons << mismatch_reason
      end
    end
  end
end
