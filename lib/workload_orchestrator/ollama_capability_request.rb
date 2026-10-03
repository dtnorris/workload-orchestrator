# frozen_string_literal: true

require "digest"
require "json"
require_relative "error"

module WorkloadOrchestrator
  # Strict parser and immutable representation of
  # ollama-capability-request/v0.1.
  class OllamaCapabilityRequest
    CONTRACT_VERSION = "ollama-capability-request/v0.1"
    ROOT_KEYS = %w[contract_version ollama].freeze
    OLLAMA_KEYS = %w[
      model expected_digest required_context_length require_fully_gpu_resident
    ].freeze
    OLLAMA_OPTIONAL_KEYS = %w[required_gpu_id].freeze
    SHA256 = /\A[0-9a-f]{64}\z/
    MAX_IDENTITY_LENGTH = 256

    class DuplicateKeyHash < Hash
      def []=(key, value)
        raise JSON::ParserError, "duplicate object key #{key.inspect}" if key?(key)

        super
      end
    end
    private_constant :DuplicateKeyHash

    attr_reader :document, :contract_version, :model, :expected_digest,
                :required_context_length, :require_fully_gpu_resident,
                :required_gpu_id, :normalized_request, :normalized_json,
                :fingerprint

    def self.load(path)
      new(File.binread(File.expand_path(path)))
    rescue Errno::ENOENT => e
      raise Error, e.message
    end

    def initialize(bytes)
      raise Error, "Ollama capability request must be JSON bytes" unless bytes.is_a?(String)

      @document = JSON.parse(bytes, object_class: DuplicateKeyHash)
      validate_and_assign!
      @normalized_request = build_normalized_request
      @normalized_json = JSON.generate(normalized_request).encode(Encoding::UTF_8).freeze
      @fingerprint = Digest::SHA256.hexdigest(normalized_json).freeze
      deep_freeze(document)
      freeze
    rescue JSON::ParserError, EncodingError => e
      raise Error, "invalid Ollama capability-request JSON: #{e.message}"
    end

    private

    def validate_and_assign!
      exact_keys!(document, ROOT_KEYS, "Ollama capability request")
      @contract_version = document.fetch("contract_version")
      unless contract_version == CONTRACT_VERSION
        raise Error, "Ollama capability request contract must be #{CONTRACT_VERSION}"
      end

      ollama = document.fetch("ollama")
      exact_keys!(ollama, OLLAMA_KEYS, "ollama", optional: OLLAMA_OPTIONAL_KEYS)
      @model = identity!(ollama.fetch("model"), "ollama.model")
      @expected_digest = digest!(ollama.fetch("expected_digest"))
      @required_context_length = context_length!(ollama.fetch("required_context_length"))
      @require_fully_gpu_resident = boolean!(
        ollama.fetch("require_fully_gpu_resident"), "ollama.require_fully_gpu_resident"
      )
      @required_gpu_id = if ollama.key?("required_gpu_id")
                           identity!(ollama.fetch("required_gpu_id"), "ollama.required_gpu_id")
                         end
    end

    def build_normalized_request
      ollama = {
        "model" => model,
        "expected_digest" => expected_digest,
        "required_context_length" => required_context_length,
        "require_fully_gpu_resident" => require_fully_gpu_resident
      }
      ollama["required_gpu_id"] = required_gpu_id unless required_gpu_id.nil?
      deep_freeze("ollama" => ollama)
    end

    def exact_keys!(value, required, label, optional: [])
      raise Error, "#{label} must be an object" unless value.is_a?(Hash)

      missing = required - value.keys
      unknown = value.keys - required - optional
      raise Error, "#{label} missing fields: #{missing.join(', ')}" unless missing.empty?
      raise Error, "#{label} unknown fields: #{unknown.join(', ')}" unless unknown.empty?
    end

    def identity!(value, label)
      valid = value.is_a?(String) && !value.empty? && value == value.strip &&
              !value.match?(/[[:cntrl:]]/) && value.length <= MAX_IDENTITY_LENGTH
      return value.freeze if valid

      raise Error,
            "#{label} must be a non-empty trimmed string of at most #{MAX_IDENTITY_LENGTH} characters"
    end

    def digest!(value)
      return value.freeze if value.is_a?(String) && SHA256.match?(value)

      raise Error, "ollama.expected_digest must be an exact lowercase 64-hex digest"
    end

    def context_length!(value)
      return value if value.is_a?(Integer) && value.positive?

      raise Error, "ollama.required_context_length must be a positive integer"
    end

    def boolean!(value, label)
      return value if [true, false].include?(value)

      raise Error, "#{label} must be boolean"
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
