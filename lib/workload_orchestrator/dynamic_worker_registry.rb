# frozen_string_literal: true

require "digest"
require "json"
require "time"
require "uri"

module WorkloadOrchestrator
  # Strict parser and immutable representation of dynamic-worker-registry/v0.1.
  class DynamicWorkerRegistry
    CONTRACT_VERSION = "dynamic-worker-registry/v0.1"
    ROOT_KEYS = %w[contract_version registry_id revision published_at expires_at workers].freeze
    WORKER_KEYS = %w[
      worker_id generation_id endpoint state labels capabilities capability_fingerprint
    ].freeze
    CAPABILITY_KEYS = %w[gpu_id ollama].freeze
    OLLAMA_KEYS = %w[models].freeze
    MODEL_KEYS = %w[model digest context_length fully_gpu_resident].freeze
    STATES = %w[READY NOT_READY UNAVAILABLE].freeze
    SHA256 = /\A[0-9a-f]{64}\z/

    class DuplicateKeyHash < Hash
      def []=(key, value)
        raise JSON::ParserError, "duplicate object key #{key.inspect}" if key?(key)

        super
      end
    end
    private_constant :DuplicateKeyHash

    attr_reader :bytes, :sha256, :contract_version, :registry_id, :revision,
                :published_at, :expires_at, :entries, :schedulable_workers,
                :document

    def self.from_source(source, now: Time.now.utc, previous: nil)
      raise Error, "worker source must implement #latest_snapshot" unless source.respond_to?(:latest_snapshot)

      new(source.latest_snapshot, now: now, previous: previous)
    rescue NotImplementedError => e
      raise Error, e.message
    end

    def initialize(snapshot_bytes, now: Time.now.utc, previous: nil)
      raise Error, "worker source snapshot must be JSON bytes" unless snapshot_bytes.is_a?(String)

      @bytes = snapshot_bytes.dup.freeze
      @sha256 = Digest::SHA256.hexdigest(bytes).freeze
      @document = JSON.parse(bytes, object_class: DuplicateKeyHash)
      validate_root!(now)
      validate_workers!
      validate_previous!(previous)
      deep_freeze(document)
      build_entries!
      freeze
    rescue JSON::ParserError => e
      raise Error, "invalid dynamic worker registry JSON: #{e.message}"
    end

    private

    def validate_root!(now)
      exact_keys!(document, ROOT_KEYS, "worker registry")
      validate_contract_identity!
      validate_snapshot_times!(now)

      workers = document.fetch("workers")
      raise Error, "worker registry workers must be an array" unless workers.is_a?(Array)
    end

    def validate_contract_identity!
      @contract_version = document.fetch("contract_version")
      raise Error, "worker registry contract must be #{CONTRACT_VERSION}" unless contract_version == CONTRACT_VERSION

      @registry_id = non_empty_string!(document.fetch("registry_id"), "registry_id")
      @revision = document.fetch("revision")
      return if revision.is_a?(Integer) && !revision.negative?

      raise Error, "worker registry revision must be a non-negative integer"
    end

    def validate_snapshot_times!(now)
      @published_at = timestamp!(document.fetch("published_at"), "published_at")
      @expires_at = timestamp!(document.fetch("expires_at"), "expires_at")
      comparison_time = coerce_time!(now, "current time")
      raise Error, "worker registry published_at is future-dated" if published_at > comparison_time
      raise Error, "worker registry expires_at must follow published_at" unless expires_at > published_at
      raise Error, "worker registry snapshot is expired" unless expires_at > comparison_time
    end

    def validate_workers!
      worker_ids = {}
      endpoints = {}
      document.fetch("workers").each_with_index do |record, index|
        label = "worker[#{index}]"
        exact_keys!(record, WORKER_KEYS, label)
        worker_id = non_empty_string!(record.fetch("worker_id"), "#{label}.worker_id")
        non_empty_string!(record.fetch("generation_id"), "#{label}.generation_id")
        endpoint_key = endpoint!(record.fetch("endpoint"), "#{label}.endpoint")
        state!(record.fetch("state"), label)
        labels!(record.fetch("labels"), label)
        capabilities!(record.fetch("capabilities"), label)
        fingerprint!(record, label)

        raise Error, "duplicate worker_id #{worker_id.inspect}" if worker_ids.key?(worker_id)
        raise Error, "duplicate worker endpoint #{record.fetch('endpoint').inspect}" if endpoints.key?(endpoint_key)

        worker_ids[worker_id] = true
        endpoints[endpoint_key] = true
      end
    end

    def validate_previous!(previous)
      return unless previous
      raise Error, "previous worker registry must be a validated #{self.class.name}" unless previous.is_a?(self.class)
      unless registry_id == previous.registry_id
        raise Error, "worker registry identity changed from #{previous.registry_id.inspect} to #{registry_id.inspect}"
      end
      if revision < previous.revision
        raise Error, "worker registry revision rolled back from #{previous.revision} to #{revision}"
      end

      if revision == previous.revision
        return if sha256 == previous.sha256

        raise Error, "worker registry revision #{revision} changed contents"
      end
      return if published_at > previous.published_at

      raise Error, "worker registry publication time did not advance with revision"
    end

    def build_entries!
      registry = {
        registry_id: registry_id,
        revision: revision,
        published_at: published_at,
        expires_at: expires_at,
        sha256: sha256
      }.freeze
      @entries = document.fetch("workers").map do |record|
        RegistryWorker.new(registry: registry, record: record)
      end.freeze
      @schedulable_workers = entries.select(&:ready?).freeze
    end

    def exact_keys!(value, required, label)
      raise Error, "#{label} must be an object" unless value.is_a?(Hash)

      missing = required - value.keys
      unknown = value.keys - required
      raise Error, "#{label} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise Error, "#{label} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
    end

    def non_empty_string!(value, label)
      unless value.is_a?(String) && !value.empty? && value == value.strip && !value.match?(/[[:cntrl:]]/)
        raise Error, "#{label} must be a non-empty trimmed string without control characters"
      end

      value
    end

    def timestamp!(value, label)
      non_empty_string!(value, label)
      Time.iso8601(value).freeze
    rescue ArgumentError
      raise Error, "#{label} must be an ISO 8601 timestamp"
    end

    def coerce_time!(value, label)
      return value.getutc.freeze if value.is_a?(Time)

      raise Error, "#{label} must be a Time"
    end

    def endpoint!(value, label)
      non_empty_string!(value, label)
      uri = URI.parse(value)
      valid_path = uri.path.nil? || uri.path.empty? || uri.path == "/"
      unless uri.is_a?(URI::HTTP) && uri.host && !uri.host.empty? &&
             uri.userinfo.nil? && uri.query.nil? && uri.fragment.nil? && valid_path
        raise Error, "#{label} must be an HTTP(S) origin without credentials, path, query, or fragment"
      end

      "#{uri.scheme.downcase}://#{uri.host.downcase}:#{uri.port}"
    rescue URI::InvalidURIError, ArgumentError
      raise Error, "#{label} must be a valid HTTP(S) origin"
    end

    def state!(value, label)
      return if STATES.include?(value)

      raise Error, "#{label}.state must be one of #{STATES.join(', ')}"
    end

    def labels!(value, label)
      strings!(value, "#{label}.labels")
      raise Error, "#{label}.labels must be unique" unless value.uniq == value
    end

    def capabilities!(value, label)
      exact_keys!(value, CAPABILITY_KEYS, "#{label}.capabilities")
      non_empty_string!(value.fetch("gpu_id"), "#{label}.capabilities.gpu_id")
      ollama = value.fetch("ollama")
      exact_keys!(ollama, OLLAMA_KEYS, "#{label}.capabilities.ollama")
      models = ollama.fetch("models")
      raise Error, "#{label}.capabilities.ollama.models must be an array" unless models.is_a?(Array)

      model_ids = models.each_with_index.map do |model, model_index|
        model_label = "#{label}.capabilities.ollama.models[#{model_index}]"
        exact_keys!(model, MODEL_KEYS, model_label)
        model_id = non_empty_string!(model.fetch("model"), "#{model_label}.model")
        digest = model.fetch("digest")
        unless digest.is_a?(String) && SHA256.match?(digest)
          raise Error,
                "#{model_label}.digest must be a lowercase SHA-256"
        end

        context = model.fetch("context_length")
        unless context.is_a?(Integer) && context.positive?
          raise Error, "#{model_label}.context_length must be a positive integer"
        end
        unless [true, false].include?(model.fetch("fully_gpu_resident"))
          raise Error, "#{model_label}.fully_gpu_resident must be boolean"
        end

        model_id
      end
      raise Error, "#{label}.capabilities.ollama model identifiers must be unique" unless model_ids.uniq == model_ids
    end

    def fingerprint!(record, label)
      supplied = record.fetch("capability_fingerprint")
      unless supplied.is_a?(String) && SHA256.match?(supplied)
        raise Error, "#{label}.capability_fingerprint must be a lowercase SHA-256"
      end

      capabilities = record.fetch("capabilities")
      models = capabilities.dig("ollama", "models").map do |model|
        {
          "context_length" => model.fetch("context_length"),
          "digest" => model.fetch("digest"),
          "fully_gpu_resident" => model.fetch("fully_gpu_resident"),
          "model" => model.fetch("model")
        }
      end
      expected = Digest::SHA256.hexdigest(JSON.generate(
                                            "gpu_id" => capabilities.fetch("gpu_id"),
                                            "labels" => record.fetch("labels"),
                                            "ollama_models" => models
                                          ))
      return if supplied == expected

      raise Error, "#{label}.capability_fingerprint does not match capabilities"
    end

    def strings!(value, label)
      unless value.is_a?(Array) && value.all? do |item|
        item.is_a?(String) && !item.empty? && item == item.strip && !item.match?(/[[:cntrl:]]/)
      end
        raise Error, "#{label} must be an array of non-empty trimmed strings"
      end
    end

    def deep_freeze(value)
      case value
      when Hash then value.each do |key, item|
        deep_freeze(key)
        deep_freeze(item)
      end
      when Array then value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end
  end
end
