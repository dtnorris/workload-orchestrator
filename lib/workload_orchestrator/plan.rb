# frozen_string_literal: true

require "digest"
require "json"

module WorkloadOrchestrator
  class Plan
    CONTRACT_VERSION = "wlo-execution-plan/v0.1"
    ID_PATTERN = /\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z/
    ENV_NAME_PATTERN = /\A[A-Za-z_][A-Za-z0-9_]*\z/
    DIGEST_PATTERN = /\A[0-9a-f]{64}\z/i
    TOP_KEYS = %w[contract_version plan_id failure_policy pools jobs].freeze
    POOL_KEYS = %w[pool_id worker_names max_concurrency].freeze
    POOL_OPTIONAL_KEYS = %w[required_labels requirements].freeze
    JOB_KEYS = %w[job_id pool_id argv env].freeze
    FAILURE_KEYS = %w[max_consecutive_failures max_total_failures].freeze
    REQUIREMENT_KEYS = %w[ollama].freeze
    OLLAMA_KEYS = %w[model expected_digest].freeze

    Pool = Struct.new(
      :id, :worker_names, :required_labels, :ollama_requirement, :max_concurrency,
      keyword_init: true
    )
    Job = Struct.new(:id, :pool_id, :argv, :env, keyword_init: true)

    attr_reader :path, :bytes, :sha256, :id, :failure_policy, :pools, :jobs

    def self.load(path)
      expanded = File.expand_path(path)
      new(File.binread(expanded), path: expanded)
    rescue Errno::ENOENT => e
      raise Error, e.message
    end

    def initialize(bytes, path: nil)
      @bytes = bytes
      @path = path
      @sha256 = Digest::SHA256.hexdigest(bytes)
      document = parse_document(bytes)
      validate_keys!(document, TOP_KEYS, "plan")
      validate_contract!(document)
      @id = identifier!(document.fetch("plan_id"), "plan_id")
      @failure_policy = parse_failure_policy(document.fetch("failure_policy"))
      @pools = parse_pools(document.fetch("pools"))
      @jobs = parse_jobs(document.fetch("jobs"), @pools)
    end

    def pool(id)
      pools.find { |candidate| candidate.id == id }
    end

    private

    def parse_document(bytes)
      document = JSON.parse(bytes)
      raise Error, "execution plan must be a JSON object" unless document.is_a?(Hash)

      document.transform_keys(&:to_s)
    rescue JSON::ParserError => e
      raise Error, "invalid execution-plan JSON: #{e.message}"
    end

    def validate_contract!(document)
      value = document.fetch("contract_version").to_s
      return if value == CONTRACT_VERSION

      raise Error, "execution plan contract must be #{CONTRACT_VERSION.inspect}"
    end

    def parse_failure_policy(value)
      data = mapping!(value, "failure_policy")
      validate_keys!(data, FAILURE_KEYS, "failure_policy")
      {
        "max_consecutive_failures" => positive_integer!(
          data.fetch("max_consecutive_failures"), "max_consecutive_failures"
        ),
        "max_total_failures" => positive_integer!(data.fetch("max_total_failures"), "max_total_failures")
      }
    end

    def parse_pools(value)
      rows = non_empty_array!(value, "pools")
      pools = rows.map.with_index { |row, index| parse_pool(row, index) }
      duplicate = duplicate_value(pools.map(&:id))
      raise Error, "duplicate pool_id #{duplicate.inspect}" if duplicate

      pools.freeze
    end

    def parse_pool(value, index)
      data = mapping!(value, "pools[#{index}]")
      validate_keys!(data, POOL_KEYS, "pools[#{index}]", optional: POOL_OPTIONAL_KEYS)
      worker_names = string_array!(data.fetch("worker_names"), "pools[#{index}].worker_names")
      concurrency = positive_integer!(data.fetch("max_concurrency"), "pools[#{index}].max_concurrency")
      if concurrency > worker_names.length
        raise Error, "pools[#{index}].max_concurrency cannot exceed worker_names count"
      end

      Pool.new(
        id: identifier!(data.fetch("pool_id"), "pools[#{index}].pool_id"),
        worker_names: worker_names.freeze,
        required_labels: optional_string_array!(
          data.fetch("required_labels", []), "pools[#{index}].required_labels"
        ).freeze,
        ollama_requirement: parse_requirements(data.fetch("requirements", {}), index),
        max_concurrency: concurrency
      ).freeze
    end

    def parse_requirements(value, index)
      data = mapping!(value, "pools[#{index}].requirements")
      validate_optional_keys!(data, REQUIREMENT_KEYS, "pools[#{index}].requirements")
      return nil unless data.key?("ollama")

      ollama = mapping!(data.fetch("ollama"), "pools[#{index}].requirements.ollama")
      validate_keys!(ollama, OLLAMA_KEYS, "pools[#{index}].requirements.ollama")
      digest = ollama.fetch("expected_digest").to_s.downcase
      raise Error, "expected_digest must be an exact 64-hex digest" unless digest.match?(DIGEST_PATTERN)

      { "model" => non_empty_string!(ollama.fetch("model"), "ollama model"), "expected_digest" => digest }.freeze
    end

    def parse_jobs(value, pools)
      rows = non_empty_array!(value, "jobs")
      pool_ids = pools.map(&:id)
      jobs = rows.map.with_index { |row, index| parse_job(row, index, pool_ids) }
      duplicate = duplicate_value(jobs.map(&:id))
      raise Error, "duplicate job_id #{duplicate.inspect}" if duplicate

      jobs.freeze
    end

    def parse_job(value, index, pool_ids)
      data = mapping!(value, "jobs[#{index}]")
      validate_keys!(data, JOB_KEYS, "jobs[#{index}]", optional: %w[env])
      pool_id = identifier!(data.fetch("pool_id"), "jobs[#{index}].pool_id")
      raise Error, "jobs[#{index}] references unknown pool #{pool_id.inspect}" unless pool_ids.include?(pool_id)

      Job.new(
        id: identifier!(data.fetch("job_id"), "jobs[#{index}].job_id"),
        pool_id: pool_id,
        argv: string_array!(data.fetch("argv"), "jobs[#{index}].argv").freeze,
        env: environment!(data.fetch("env", {}), "jobs[#{index}].env").freeze
      ).freeze
    end

    def environment!(value, label)
      data = mapping!(value, label)
      data.each_with_object({}) do |(key, raw), output|
        name = key.to_s
        raise Error, "#{label} has invalid variable name #{name.inspect}" unless name.match?(ENV_NAME_PATTERN)
        raise Error, "#{label}.#{name} must be a string or null" unless raw.nil? || raw.is_a?(String)

        output[name] = raw
      end
    end

    def validate_keys!(data, required, label, optional: [])
      missing = required - data.keys - optional
      unknown = data.keys - required - optional
      raise Error, "#{label} is missing field(s): #{missing.join(', ')}" unless missing.empty?
      raise Error, "#{label} has unknown field(s): #{unknown.join(', ')}" unless unknown.empty?
    end

    def validate_optional_keys!(data, allowed, label)
      unknown = data.keys - allowed
      raise Error, "#{label} has unknown field(s): #{unknown.join(', ')}" unless unknown.empty?
    end

    def mapping!(value, label)
      raise Error, "#{label} must be an object" unless value.is_a?(Hash)

      value.transform_keys(&:to_s)
    end

    def non_empty_array!(value, label)
      raise Error, "#{label} must be a non-empty array" unless value.is_a?(Array) && !value.empty?

      value
    end

    def string_array!(value, label)
      rows = non_empty_array!(value, label)
      rows.map.with_index { |item, index| non_empty_string!(item, "#{label}[#{index}]") }
    end

    def optional_string_array!(value, label)
      raise Error, "#{label} must be an array" unless value.is_a?(Array)

      value.map.with_index { |item, index| non_empty_string!(item, "#{label}[#{index}]") }
    end

    def non_empty_string!(value, label)
      text = value.to_s
      raise Error, "#{label} must be a non-empty string" if text.empty?

      text
    end

    def identifier!(value, label)
      text = non_empty_string!(value, label)
      raise Error, "#{label} has invalid identifier #{text.inspect}" unless text.match?(ID_PATTERN)

      text
    end

    def positive_integer!(value, label)
      number = Integer(value)
      raise ArgumentError unless number.positive?

      number
    rescue ArgumentError, TypeError
      raise Error, "#{label} must be a positive integer"
    end

    def duplicate_value(values)
      values.group_by(&:itself).find { |_value, rows| rows.length > 1 }&.first
    end
  end
end
