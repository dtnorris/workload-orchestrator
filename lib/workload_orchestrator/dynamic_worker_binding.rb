# frozen_string_literal: true

module WorkloadOrchestrator
  # Immutable execution identity and registry evidence captured before a
  # dynamic attempt is dispatched.
  class DynamicWorkerBinding
    IDENTITY_KEYS = %w[
      registry_id worker_id generation_id endpoint capability_fingerprint
    ].freeze
    REGISTRY_KEYS = %w[registry_revision registry_snapshot_sha256].freeze
    SHA256 = /\A[0-9a-f]{64}\z/

    attr_reader :execution_identity, :registry_binding

    def self.from_worker(worker)
      raise Error, "dynamic attempts require a validated registry worker" unless worker.is_a?(RegistryWorker)

      new(
        execution_identity: {
          "registry_id" => worker.registry_id,
          "worker_id" => worker.worker_id,
          "generation_id" => worker.generation_id,
          "endpoint" => worker.endpoint,
          "capability_fingerprint" => worker.capability_fingerprint
        },
        registry_binding: {
          "registry_revision" => worker.registry_revision,
          "registry_snapshot_sha256" => worker.snapshot_sha256
        }
      )
    end

    def self.from_metadata(document)
      new(
        execution_identity: document.fetch("worker_execution_identity"),
        registry_binding: document.fetch("worker_registry_binding")
      )
    rescue KeyError => e
      raise Error, "dynamic attempt binding is incomplete: #{e.message}"
    end

    def initialize(execution_identity:, registry_binding:)
      validate_identity!(execution_identity)
      validate_registry!(registry_binding)
      @execution_identity = immutable_copy(execution_identity, IDENTITY_KEYS)
      @registry_binding = immutable_copy(registry_binding, REGISTRY_KEYS)
      freeze
    end

    def tuple
      IDENTITY_KEYS.map { |key| execution_identity.fetch(key) }.freeze
    end

    def same_identity?(other)
      other.is_a?(self.class) && tuple == other.tuple
    end

    def metadata
      {
        "worker_execution_identity" => execution_identity,
        "worker_registry_binding" => registry_binding
      }
    end

    private

    def validate_identity!(value)
      exact_keys!(value, IDENTITY_KEYS, "dynamic worker execution identity")
      IDENTITY_KEYS.each do |key|
        field = value.fetch(key)
        raise Error, "dynamic worker execution identity #{key} is invalid" unless field.is_a?(String) && !field.empty?
      end
      fingerprint = value.fetch("capability_fingerprint")
      return if fingerprint.match?(SHA256)

      raise Error, "dynamic worker execution identity capability_fingerprint is invalid"
    end

    def validate_registry!(value)
      exact_keys!(value, REGISTRY_KEYS, "dynamic worker registry binding")
      revision = value.fetch("registry_revision")
      unless revision.is_a?(Integer) && !revision.negative?
        raise Error, "dynamic worker registry binding revision is invalid"
      end

      digest = value.fetch("registry_snapshot_sha256")
      return if digest.is_a?(String) && digest.match?(SHA256)

      raise Error, "dynamic worker registry binding snapshot digest is invalid"
    end

    def exact_keys!(value, expected, label)
      return if value.is_a?(Hash) && value.keys.sort == expected.sort

      raise Error, "#{label} fields are invalid"
    end

    def immutable_copy(value, keys)
      keys.to_h do |key|
        item = value.fetch(key)
        [key.freeze, item.is_a?(String) ? item.dup.freeze : item]
      end.freeze
    end
  end
end
