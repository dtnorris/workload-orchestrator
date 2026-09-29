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

    SNAPSHOT_KEYS = (IDENTITY_KEYS + REGISTRY_KEYS + %w[published_at expires_at state labels capabilities]).freeze

    attr_reader :execution_identity, :registry_binding, :worker_snapshot

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
        },
        worker_snapshot: {
          "published_at" => worker.published_at.iso8601,
          "expires_at" => worker.expires_at.iso8601,
          "state" => worker.state,
          "labels" => worker.labels,
          "capabilities" => worker.capabilities
        }
      )
    end

    def self.from_metadata(document)
      new(
        execution_identity: document.fetch("worker_execution_identity"),
        registry_binding: document.fetch("worker_registry_binding"),
        worker_snapshot: document["worker_snapshot"]
      )
    rescue KeyError => e
      raise Error, "dynamic attempt binding is incomplete: #{e.message}"
    end

    def initialize(execution_identity:, registry_binding:, worker_snapshot: nil)
      validate_identity!(execution_identity)
      validate_registry!(registry_binding)
      @execution_identity = immutable_copy(execution_identity, IDENTITY_KEYS)
      @registry_binding = immutable_copy(registry_binding, REGISTRY_KEYS)
      if worker_snapshot
        snapshot = execution_identity.merge(registry_binding).merge(worker_snapshot)
        validate_snapshot!(snapshot)
        @worker_snapshot = deep_immutable_copy(snapshot)
      end
      freeze
    end

    def tuple
      IDENTITY_KEYS.map { |key| execution_identity.fetch(key) }.freeze
    end

    def same_identity?(other)
      other.is_a?(self.class) && tuple == other.tuple
    end

    def metadata
      document = {
        "worker_execution_identity" => execution_identity,
        "worker_registry_binding" => registry_binding
      }
      document["worker_snapshot"] = worker_snapshot if worker_snapshot
      document
    end

    private

    def validate_snapshot!(snapshot)
      exact_keys!(snapshot, SNAPSHOT_KEYS, "dynamic worker snapshot")
      unless snapshot.slice(*IDENTITY_KEYS) == execution_identity &&
             snapshot.slice(*REGISTRY_KEYS) == registry_binding
        raise Error, "dynamic worker snapshot conflicts with attempt binding"
      end
      valid = DynamicWorkerRegistry::STATES.include?(snapshot["state"]) &&
              snapshot["labels"].is_a?(Array) && snapshot["labels"].all?(String) &&
              snapshot["capabilities"].is_a?(Hash)
      raise Error, "dynamic worker snapshot fields are invalid" unless valid

      %w[published_at expires_at].each { |key| Time.iso8601(snapshot.fetch(key)) }
    rescue ArgumentError, TypeError
      raise Error, "dynamic worker snapshot timestamps are invalid"
    end

    def deep_immutable_copy(value)
      case value
      when Hash
        value.to_h { |key, item| [key.dup.freeze, deep_immutable_copy(item)] }.freeze
      when Array
        value.map { |item| deep_immutable_copy(item) }.freeze
      when String
        value.dup.freeze
      else
        value
      end
    end

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
