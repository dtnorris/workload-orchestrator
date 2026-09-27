# frozen_string_literal: true

module WorkloadOrchestrator
  class Worker
    TYPES = %w[command ollama].freeze

    attr_reader :name, :type, :labels, :hourly_rate_usd, :job_env, :base_url

    def initialize(name, attrs)
      data = attrs.transform_keys(&:to_s)
      @name = name.to_s
      @type = data.fetch("type", "command").to_s
      @labels = Array(data.fetch("labels", [])).map(&:to_s).freeze
      @hourly_rate_usd = non_negative_rate(data.fetch("hourly_rate_usd", 0.0))
      @job_env = normalize_environment(data.fetch("job_env", {})).freeze
      @base_url = normalize_base_url(data["base_url"])
      validate!
    end

    def compatible?(required_labels)
      (Array(required_labels).map(&:to_s) - labels).empty?
    end

    def zero_cost?
      hourly_rate_usd.zero?
    end

    private

    def validate!
      raise Error, "worker #{name.inspect} has unsupported type #{type.inspect}" unless TYPES.include?(type)
      return unless type == "ollama" && base_url.nil?

      raise Error, "ollama worker #{name.inspect} requires base_url"
    end

    def non_negative_rate(value)
      rate = Float(value)
      raise ArgumentError unless rate.finite? && !rate.negative?

      rate
    rescue ArgumentError, TypeError
      raise Error, "worker #{name.inspect} hourly_rate_usd must be a non-negative finite number"
    end

    def normalize_environment(value)
      raise Error, "worker #{name.inspect} job_env must be an object" unless value.is_a?(Hash)

      value.each_with_object({}) do |(key, raw), output|
        unless raw.nil? || raw.is_a?(String)
          raise Error, "worker #{name.inspect} job_env value for #{key.inspect} must be a string or null"
        end

        output[key.to_s] = raw
      end
    end

    def normalize_base_url(value)
      text = value.to_s.strip
      text.empty? ? nil : text.sub(%r{/+\z}, "")
    end
  end
end
