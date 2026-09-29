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

    # Capability comparison is deterministic and mirrors the frozen contract's
    # exact-match semantics. Selection and assignment belong to later work.
    def compatible?(required_labels: [], ollama_requirement: nil)
      return false unless ready?
      return false unless (Array(required_labels).map(&:to_s) - labels).empty?
      return true unless ollama_requirement

      requirement = ollama_requirement.transform_keys(&:to_s)
      gpu_compatible?(requirement) && model_compatible?(requirement)
    end

    private

    def gpu_compatible?(requirement)
      !requirement["required_gpu_id"] || gpu_id == requirement["required_gpu_id"]
    end

    def model_compatible?(requirement)
      model = ollama_models.find do |candidate|
        candidate.fetch("model") == requirement.fetch("model") &&
          candidate.fetch("digest") == requirement.fetch("expected_digest")
      end
      return false unless model
      return false if requirement.key?("required_context_length") &&
                      model.fetch("context_length") != requirement.fetch("required_context_length")
      return false if requirement["require_fully_gpu_resident"] &&
                      model.fetch("fully_gpu_resident") != true

      true
    end
  end
end
