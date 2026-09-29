# frozen_string_literal: true

require "digest"
require "json"

module WorkloadOrchestrator
  class Plan
    CONTRACT_VERSION = "wlo-execution-plan/v0.1"
    LOGICAL_CONTRACT_VERSION = "wlo-execution-plan/v0.2"
    PRIORITY_CONTRACT_VERSION = "wlo-execution-plan/v0.3"
    LOGICAL_CONTRACT_VERSIONS = [LOGICAL_CONTRACT_VERSION, PRIORITY_CONTRACT_VERSION].freeze
    SUPPORTED_CONTRACT_VERSIONS = [CONTRACT_VERSION, *LOGICAL_CONTRACT_VERSIONS].freeze
    LEGACY_SCHEDULING = :legacy
    WORK_CONSERVING_PRIORITY_SCHEDULING = :work_conserving_priority
    NO_GROUP_SEMANTICS = :none
    HARD_GROUP_BARRIER = :hard_barrier
    PRIORITY_ONLY_GROUPS = :priority_only
    ID_PATTERN = /\A[A-Za-z0-9][A-Za-z0-9._-]{0,127}\z/
    ENV_NAME_PATTERN = /\A[A-Za-z_][A-Za-z0-9_]*\z/
    DIGEST_PATTERN = /\A[0-9a-f]{64}\z/i
    TOP_KEYS = %w[contract_version plan_id failure_policy pools jobs].freeze
    POOL_KEYS = %w[pool_id worker_names max_concurrency].freeze
    POOL_OPTIONAL_KEYS = %w[required_labels requirements].freeze
    JOB_KEYS = %w[job_id pool_id argv].freeze
    JOB_OPTIONAL_KEYS = %w[env group_id].freeze
    PRIORITY_JOB_OPTIONAL_KEYS = %w[depends_on_job_ids].freeze
    FAILURE_KEYS = %w[max_consecutive_failures max_total_failures].freeze
    FAILURE_OPTIONAL_KEYS = %w[non_operational_exit_statuses].freeze
    REQUIREMENT_KEYS = %w[ollama].freeze
    OLLAMA_KEYS = %w[model expected_digest].freeze
    OLLAMA_CAPABILITY_KEYS = %w[required_context_length require_fully_gpu_resident required_gpu_id].freeze

    Pool = Struct.new(
      :id, :worker_names, :required_labels, :ollama_requirement, :max_concurrency,
      keyword_init: true
    )
    Job = Struct.new(
      :id, :pool_id, :group_id, :argv, :env, :depends_on_job_ids, :priority_key,
      keyword_init: true
    )

    attr_reader :path, :bytes, :sha256, :id, :failure_policy, :pools, :jobs, :contract_version

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

    def logical?
      LOGICAL_CONTRACT_VERSIONS.include?(contract_version)
    end

    def priority_scheduling?
      contract_version == PRIORITY_CONTRACT_VERSION
    end

    def scheduling_semantics
      priority_scheduling? ? WORK_CONSERVING_PRIORITY_SCHEDULING : LEGACY_SCHEDULING
    end

    def group_semantics
      return NO_GROUP_SEMANTICS unless grouped_jobs?

      priority_scheduling? ? PRIORITY_ONLY_GROUPS : HARD_GROUP_BARRIER
    end

    def job_priority_key(job_or_id)
      unless priority_scheduling?
        raise Error, "deterministic job priority keys are defined only for wlo-execution-plan/v0.3"
      end

      id = job_or_id.respond_to?(:id) ? job_or_id.id : job_or_id.to_s
      job = jobs.find { |candidate| candidate.id == id }
      raise Error, "unknown job_id #{id.inspect}" unless job

      job.priority_key
    end

    def execution_profile
      nil
    end

    def pool(id)
      pools.find { |candidate| candidate.id == id }
    end

    def grouped_jobs?
      !jobs.empty? && !jobs.first.group_id.nil?
    end

    def job_groups
      return [] unless grouped_jobs?

      jobs.group_by(&:group_id).values
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
      @contract_version = document.fetch("contract_version").to_s
      return if SUPPORTED_CONTRACT_VERSIONS.include?(@contract_version)

      raise Error, "unsupported execution plan contract #{@contract_version.inspect}"
    end

    def parse_failure_policy(value)
      data = mapping!(value, "failure_policy")
      validate_keys!(data, FAILURE_KEYS, "failure_policy", optional: FAILURE_OPTIONAL_KEYS)
      excluded = data.fetch("non_operational_exit_statuses", [])
      unless excluded.is_a?(Array) && excluded.all? { |code| code.is_a?(Integer) && (1..255).cover?(code) } &&
             excluded.uniq == excluded
        raise Error, "non_operational_exit_statuses must be distinct nonzero process exit statuses (1..255)"
      end
      {
        "max_consecutive_failures" => positive_integer!(
          data.fetch("max_consecutive_failures"), "max_consecutive_failures"
        ),
        "max_total_failures" => positive_integer!(data.fetch("max_total_failures"), "max_total_failures"),
        "non_operational_exit_statuses" => excluded.freeze
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
      return parse_logical_pool(data, index) if logical?

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

    def parse_logical_pool(data, index)
      validate_keys!(data, %w[pool_id], "pools[#{index}]", optional: POOL_OPTIONAL_KEYS)
      Pool.new(
        id: identifier!(data.fetch("pool_id"), "pools[#{index}].pool_id"),
        required_labels: optional_string_array!(data.fetch("required_labels", []), "required_labels").freeze,
        ollama_requirement: parse_requirements(data.fetch("requirements", {}), index)
      ).freeze
    end

    def parse_requirements(value, index)
      data = mapping!(value, "pools[#{index}].requirements")
      validate_optional_keys!(data, REQUIREMENT_KEYS, "pools[#{index}].requirements")
      return nil unless data.key?("ollama")

      ollama = mapping!(data.fetch("ollama"), "pools[#{index}].requirements.ollama")
      validate_keys!(ollama, OLLAMA_KEYS, "pools[#{index}].requirements.ollama",
                     optional: logical? ? OLLAMA_CAPABILITY_KEYS : [])
      validate_ollama_capabilities!(ollama)
      digest = ollama.fetch("expected_digest").to_s.downcase
      raise Error, "expected_digest must be an exact 64-hex digest" unless digest.match?(DIGEST_PATTERN)

      { "model" => non_empty_string!(ollama.fetch("model"), "ollama model"), "expected_digest" => digest }
        .merge(ollama.slice(*OLLAMA_CAPABILITY_KEYS)).freeze
    end

    def validate_ollama_capabilities!(ollama)
      if ollama.key?("required_context_length")
        context = ollama["required_context_length"]
        unless context.is_a?(Integer) && context.positive?
          raise Error, "required_context_length must be a positive integer"
        end
      end
      if ollama.key?("require_fully_gpu_resident") && ollama["require_fully_gpu_resident"] != true
        raise Error, "require_fully_gpu_resident must be true when specified"
      end
      return unless ollama.key?("required_gpu_id")

      gpu = ollama["required_gpu_id"]
      unless gpu.is_a?(String) && !gpu.strip.empty? && !gpu.include?("\0") && gpu.length <= 256
        raise Error, "required_gpu_id must be a non-empty string of at most 256 characters"
      end
    end

    def parse_jobs(value, pools)
      rows = non_empty_array!(value, "jobs")
      pool_ids = pools.map(&:id)
      jobs = rows.map.with_index { |row, index| parse_job(row, index, pool_ids) }
      duplicate = duplicate_value(jobs.map(&:id))
      raise Error, "duplicate job_id #{duplicate.inspect}" if duplicate

      grouped_count = jobs.count { |job| !job.group_id.nil? }
      if grouped_count.positive? && grouped_count != jobs.length
        raise Error, "jobs must either all define group_id or all omit it"
      end

      validate_dependencies!(jobs) if priority_scheduling?
      assign_priority_keys!(jobs) if priority_scheduling?

      jobs.each(&:freeze).freeze
    end

    def parse_job(value, index, pool_ids)
      data = mapping!(value, "jobs[#{index}]")
      optional = JOB_OPTIONAL_KEYS + (priority_scheduling? ? PRIORITY_JOB_OPTIONAL_KEYS : [])
      validate_keys!(data, JOB_KEYS, "jobs[#{index}]", optional: optional)
      pool_id = identifier!(data.fetch("pool_id"), "jobs[#{index}].pool_id")
      raise Error, "jobs[#{index}] references unknown pool #{pool_id.inspect}" unless pool_ids.include?(pool_id)

      Job.new(
        id: identifier!(data.fetch("job_id"), "jobs[#{index}].job_id"),
        pool_id: pool_id,
        group_id: data.key?("group_id") ? identifier!(data.fetch("group_id"), "jobs[#{index}].group_id") : nil,
        argv: string_array!(data.fetch("argv"), "jobs[#{index}].argv").freeze,
        env: environment!(data.fetch("env", {}), "jobs[#{index}].env").freeze,
        depends_on_job_ids: dependency_ids!(
          data.fetch("depends_on_job_ids", []), "jobs[#{index}].depends_on_job_ids"
        ).freeze
      )
    end

    def dependency_ids!(value, label)
      raise Error, "#{label} must be an array" unless value.is_a?(Array)

      ids = value.map.with_index { |item, index| identifier!(item, "#{label}[#{index}]") }
      duplicate = duplicate_value(ids)
      raise Error, "#{label} contains duplicate job_id #{duplicate.inspect}" if duplicate

      ids
    end

    def validate_dependencies!(jobs)
      all_ids = jobs.map(&:id)
      earlier_ids = []
      jobs.each_with_index do |job, index|
        job.depends_on_job_ids.each do |dependency_id|
          if dependency_id == job.id
            raise Error, "jobs[#{index}] cannot depend on itself"
          end
          unless all_ids.include?(dependency_id)
            raise Error, "jobs[#{index}] references unknown dependency #{dependency_id.inspect}"
          end
          unless earlier_ids.include?(dependency_id)
            raise Error, "jobs[#{index}] dependency #{dependency_id.inspect} must appear earlier in jobs"
          end
        end
        earlier_ids << job.id
      end
    end

    def assign_priority_keys!(jobs)
      if grouped_jobs_for?(jobs)
        group_ranks = {}
        group_positions = Hash.new(0)
        jobs.each do |job|
          group_ranks[job.group_id] ||= group_ranks.length
          job.priority_key = [group_ranks.fetch(job.group_id), group_positions[job.group_id], job.id].freeze
          group_positions[job.group_id] += 1
        end
      else
        jobs.each_with_index { |job, index| job.priority_key = [0, index, job.id].freeze }
      end
    end

    def grouped_jobs_for?(rows)
      !rows.empty? && !rows.first.group_id.nil?
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
      if logical? && !value.is_a?(String)
        raise Error, "#{label} must be a non-empty string"
      end
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
      if logical? && !value.is_a?(Integer)
        raise Error, "#{label} must be a positive integer"
      end
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
