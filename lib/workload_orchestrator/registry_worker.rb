# frozen_string_literal: true

module WorkloadOrchestrator
  # Immutable, provider-neutral worker capability record from a validated
  # dynamic-worker-registry snapshot. It is not yet wired into execution.
  class RegistryWorker
    attr_reader :registry_id, :registry_revision, :worker_id, :generation_id,
                :endpoint, :state, :labels, :capabilities, :gpu_id,
                :ollama_models, :capability_fingerprint, :published_at,
                :expires_at, :snapshot_sha256, :execution_identity

    def initialize(registry:, record:)
      @registry_id = registry.fetch(:registry_id)
      @registry_revision = registry.fetch(:revision)
      @published_at = registry.fetch(:published_at)
      @expires_at = registry.fetch(:expires_at)
      @snapshot_sha256 = registry.fetch(:sha256)
      @worker_id = record.fetch("worker_id")
      @generation_id = record.fetch("generation_id")
      @endpoint = record.fetch("endpoint")
      @state = record.fetch("state")
      @labels = record.fetch("labels")
      @capabilities = record.fetch("capabilities")
      @gpu_id = capabilities.fetch("gpu_id")
      @ollama_models = capabilities.dig("ollama", "models")
      @capability_fingerprint = record.fetch("capability_fingerprint")
      @execution_identity = [
        registry_id, worker_id, generation_id, endpoint, capability_fingerprint
      ].freeze
      freeze
    end

    def ready?
      state == "READY"
    end

    # Preserve the DW-09 predicate API while keeping matching semantics in the
    # single authoritative matcher used by future dynamic scheduling.
    def compatible?(required_labels: [], ollama_requirement: nil)
      CapabilityMatcher.match(
        worker: self,
        required_labels: required_labels,
        ollama_requirement: ollama_requirement
      ).compatible?
    end
  end
end
