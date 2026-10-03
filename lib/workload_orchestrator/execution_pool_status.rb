# frozen_string_literal: true

require "json"
require "time"

module WorkloadOrchestrator
  # Provider-neutral, read-only diagnosis for each logical execution pool.
  # It uses only WLO execution evidence and the last accepted worker-registry
  # checkpoint; it never discovers or inspects provider resources.
  class ExecutionPoolStatus
    REASONS = %w[
      RUNNING
      READY_TO_DISPATCH
      NO_COMPATIBLE_READY_WORKERS
      ALL_COMPATIBLE_WORKERS_BUSY
      WORKERS_NOT_READY
      READY_WORKERS_INCOMPATIBLE
      PAUSED
      CIRCUIT_BREAKER
      DISPATCH_HALTED
      NO_ACCEPTED_REGISTRY_SNAPSHOT
      REGISTRY_INVALID_OR_STALE
      COMPLETE
      FAILED
      INTERRUPTED
      NO_PENDING_WORK
      NO_RUNNABLE_WORK
    ].freeze

    CAPACITY_WAIT_REASONS = %w[
      NO_COMPATIBLE_READY_WORKERS
      ALL_COMPATIBLE_WORKERS_BUSY
      WORKERS_NOT_READY
      READY_WORKERS_INCOMPATIBLE
      NO_ACCEPTED_REGISTRY_SNAPSHOT
      REGISTRY_INVALID_OR_STALE
    ].freeze

    def initialize(plan:, output:, clock: -> { Time.now.utc })
      @plan = plan
      @root = File.expand_path(output)
      @clock = clock
      @matcher = CapabilityMatcher.new(plan: plan)
    end

    def document(report)
      return [] unless @plan.priority_scheduling?

      checkpoints, registry_error = load_checkpoints
      registry_error ||= "an accepted registry checkpoint is expired" if checkpoints_expired?(checkpoints)
      registry_workers = build_registry_workers(checkpoints)
      ready_workers = registry_workers.select(&:ready?)
      busy_identities = running_identities(report.fetch("jobs"))

      shared = {
        checkpoint: checkpoints.length == 1 ? checkpoints.first : nil,
        checkpoints:, registry_error:, registry_workers:, ready_workers:, busy_identities:
      }
      @plan.pools.map { |pool| pool_document(pool, report, shared) }
    end

    private

    def pool_document(pool, report, shared)
      evidence = pool_evidence(pool, report, shared)
      reason = reason_for(report, evidence.fetch(:counts), capacity_evidence(evidence, shared))
      row = {
        "pool_id" => pool.id,
        "jobs" => evidence.fetch(:counts),
        "workers" => pool_worker_counts(evidence, shared),
        "state" => state_for(reason),
        "reason" => reason,
        "registry_revision" => shared.dig(:checkpoint, "revision"),
        "registry_revisions" => shared.fetch(:checkpoints).to_h do |checkpoint|
          [checkpoint.fetch("registry_id"), checkpoint.fetch("revision")]
        end,
        "relevant_worker_ids" => relevant_worker_ids(reason, evidence, shared)
      }
      registry_error = shared.fetch(:registry_error)
      row["detail"] = registry_error if reason == "REGISTRY_INVALID_OR_STALE" && registry_error
      row
    end

    def relevant_worker_ids(reason, evidence, shared)
      workers = case reason
                when "WORKERS_NOT_READY" then evidence.fetch(:not_ready_compatible)
                when "ALL_COMPATIBLE_WORKERS_BUSY" then evidence.fetch(:busy)
                when "READY_WORKERS_INCOMPATIBLE" then evidence.fetch(:ready_workers)
                when "READY_TO_DISPATCH", "RUNNING" then evidence.fetch(:compatible)
                else shared.fetch(:registry_workers)
                end
      workers.map(&:worker_id).compact.sort
    end

    def pool_evidence(pool, report, shared)
      jobs = report.fetch("jobs").select { |row| row.fetch("pool_id") == pool.id }
      counts = job_counts(jobs)
      ready_workers = shared.fetch(:ready_workers)
      compatible = ready_workers.select { |worker| compatible?(worker, pool) }
      busy = compatible.select do |worker|
        shared.fetch(:busy_identities).include?(worker.execution_identity)
      end
      idle = compatible - busy
      not_ready = shared.fetch(:registry_workers).select { |worker| worker.state == "NOT_READY" }
      not_ready_compatible = not_ready.select { |worker| compatible_except_readiness?(worker, pool) }
      {
        counts:, compatible:, busy:, idle:, ready_workers:, not_ready:, not_ready_compatible:,
        runnable: runnable_jobs(jobs, report.fetch("jobs"))
      }
    end

    def capacity_evidence(evidence, shared)
      {
        runnable: evidence.fetch(:runnable), checkpoint: shared.fetch(:checkpoints).empty? ? nil : true,
        registry_error: shared.fetch(:registry_error), compatible: evidence.fetch(:compatible),
        busy: evidence.fetch(:busy), idle: evidence.fetch(:idle),
        ready_workers: evidence.fetch(:ready_workers),
        not_ready_compatible: evidence.fetch(:not_ready_compatible)
      }
    end

    def pool_worker_counts(evidence, shared)
      {
        "compatible_ready" => evidence.fetch(:compatible).length,
        "busy" => evidence.fetch(:busy).length,
        "idle" => evidence.fetch(:idle).length,
        "ready_incompatible" => evidence.fetch(:ready_workers).length - evidence.fetch(:compatible).length,
        "not_ready" => evidence.fetch(:not_ready).length,
        "unavailable" => shared.fetch(:registry_workers).count { |worker| worker.state == "UNAVAILABLE" }
      }
    end

    def reason_for(report, counts, capacity)
      control_reason(report, counts) || capacity_reason(capacity)
    end

    def control_reason(report, counts)
      if terminal_pool?(counts)
        return "FAILED" if counts.fetch("failed").positive?
        return "INTERRUPTED" if counts.fetch("interrupted").positive?

        return "COMPLETE"
      end
      return "PAUSED" if report.fetch("paused")
      return "CIRCUIT_BREAKER" if report.dig("circuit_breaker", "tripped")

      if report["dispatch_halt"]
        return "REGISTRY_INVALID_OR_STALE" if report.dig("dispatch_halt", "kind") == "worker_registry"

        return "DISPATCH_HALTED"
      end
      return "RUNNING" if counts.fetch("running").positive?
      return "NO_PENDING_WORK" if counts.fetch("pending").zero?

      nil
    end

    def capacity_reason(capacity)
      return "REGISTRY_INVALID_OR_STALE" if capacity.fetch(:registry_error)
      return "NO_ACCEPTED_REGISTRY_SNAPSHOT" unless capacity.fetch(:checkpoint)
      return "NO_RUNNABLE_WORK" if capacity.fetch(:runnable).empty?
      return "READY_TO_DISPATCH" unless capacity.fetch(:idle).empty?
      return "ALL_COMPATIBLE_WORKERS_BUSY" unless capacity.fetch(:compatible).empty? || capacity.fetch(:busy).empty?
      return "WORKERS_NOT_READY" unless capacity.fetch(:not_ready_compatible).empty?
      return "READY_WORKERS_INCOMPATIBLE" unless capacity.fetch(:ready_workers).empty?

      "NO_COMPATIBLE_READY_WORKERS"
    end

    def state_for(reason)
      case reason
      when "RUNNING" then "ACTIVE"
      when "READY_TO_DISPATCH" then "RUNNABLE"
      when "PAUSED", "CIRCUIT_BREAKER", "DISPATCH_HALTED", "REGISTRY_INVALID_OR_STALE" then "BLOCKED"
      when "COMPLETE", "FAILED", "INTERRUPTED", "NO_PENDING_WORK" then "TERMINAL"
      else "WAITING"
      end
    end

    def terminal_pool?(counts)
      counts.fetch("pending").zero? && counts.fetch("running").zero? &&
        (counts.fetch("complete") + counts.fetch("failed") + counts.fetch("interrupted")).positive?
    end

    def job_counts(jobs)
      observed = jobs.map { |job| job.fetch("status") }.tally
      %w[complete running failed interrupted pending].to_h { |status| [status, observed.fetch(status, 0)] }
    end

    def runnable_jobs(pool_jobs, all_jobs)
      by_id = all_jobs.to_h { |row| [row.fetch("job_id"), row] }
      pool_jobs.select { |row| row.fetch("status") == "pending" }.select do |row|
        job = @plan.jobs.find { |candidate| candidate.id == row.fetch("job_id") }
        job.depends_on_job_ids.all? do |job_id|
          ExecutionStore::TERMINAL_JOB_STATUSES.include?(by_id.fetch(job_id).fetch("status"))
        end
      end
    end

    def compatible?(worker, pool)
      @matcher.match_pool(worker:, pool:).compatible?
    end

    def compatible_except_readiness?(worker, pool)
      @matcher.match_pool(worker:, pool:).reasons == ["worker_not_ready"]
    end

    def running_identities(jobs)
      jobs.filter_map do |row|
        next unless row.fetch("status") == "running" && row["worker_execution_identity"]

        DynamicWorkerBinding::IDENTITY_KEYS.map { |key| row.fetch("worker_execution_identity").fetch(key) }
      end
    rescue KeyError => e
      raise Error, "invalid running worker identity: #{e.message}"
    end

    def load_checkpoints
      source_paths = Dir.glob(File.join(@root, "dynamic-workers", "sources", "*", "checkpoint.json"))
      legacy_path = File.join(@root, "dynamic-workers", "checkpoint.json")
      paths = source_paths.empty? && File.file?(legacy_path) ? [legacy_path] : source_paths
      documents = paths.map do |path|
        JSON.parse(File.read(path)).tap { |document| validate_checkpoint!(document) }
      end
      ids = documents.map { |document| document.fetch("registry_id") }
      raise Error, "accepted registry checkpoints contain duplicate registry identities" unless ids.uniq == ids

      [documents, nil]
    rescue Errno::ENOENT
      [[], nil]
    rescue JSON::ParserError, KeyError, ArgumentError, TypeError, Error => e
      [[], "accepted registry checkpoint is invalid: #{e.message}"]
    end

    def validate_checkpoint!(document)
      raise Error, "checkpoint must be an object" unless document.is_a?(Hash)

      validate_checkpoint_header!(document)
      Time.iso8601(document.fetch("published_at"))
      Time.iso8601(document.fetch("expires_at"))
      workers = document.fetch("workers")
      raise Error, "checkpoint workers must be an array" unless workers.is_a?(Array)

      workers.each { |worker| validate_worker!(document, worker) }
    end

    def validate_checkpoint_header!(document)
      unless document.keys.sort == WorkerRegistryPoller::CHECKPOINT_KEYS.sort
        raise Error, "checkpoint fields are invalid"
      end
      unless document["contract_version"] == WorkerRegistryPoller::CHECKPOINT_VERSION
        raise Error, "checkpoint contract is invalid"
      end
      unless document.fetch("registry_id").is_a?(String) && !document.fetch("registry_id").empty?
        raise Error, "checkpoint registry identity is invalid"
      end
      unless document.fetch("revision").is_a?(Integer) && !document.fetch("revision").negative?
        raise Error, "checkpoint revision is invalid"
      end
      return if document.fetch("snapshot_sha256").to_s.match?(DynamicWorkerRegistry::SHA256)

      raise Error, "checkpoint digest is invalid"
    end

    def validate_worker!(checkpoint, worker)
      required = WorkerRegistryPoller::WORKER_KEYS
      optional = WorkerRegistryPoller::WORKER_OPTIONAL_KEYS
      unless worker.is_a?(Hash) && (required - worker.keys).empty? && (worker.keys - required - optional).empty?
        raise Error, "checkpoint worker fields are invalid"
      end

      unless DynamicWorkerRegistry::STATES.include?(worker.fetch("state"))
        raise Error, "checkpoint worker state is invalid"
      end

      expected = [checkpoint.fetch("registry_id"), worker.fetch("worker_id"),
                  worker.fetch("generation_id"), worker.fetch("endpoint"),
                  worker.fetch("capability_fingerprint")]
      unless worker.fetch("execution_identity") == expected
        raise Error, "checkpoint worker execution identity is invalid"
      end

      return unless worker["worker_snapshot"]

      validate_worker_snapshot!(checkpoint, worker, expected)
    end

    def validate_worker_snapshot!(checkpoint, worker, identity)
      binding = DynamicWorkerBinding.from_metadata(
        "worker_execution_identity" => DynamicWorkerBinding::IDENTITY_KEYS.zip(identity).to_h,
        "worker_registry_binding" => {
          "registry_revision" => checkpoint.fetch("revision"),
          "registry_snapshot_sha256" => checkpoint.fetch("snapshot_sha256")
        },
        "worker_snapshot" => worker.fetch("worker_snapshot")
      )
      return if binding.worker_snapshot.fetch("state") == worker.fetch("state")

      raise Error, "checkpoint worker snapshot state conflicts"
    end

    def checkpoints_expired?(checkpoints)
      checkpoints.any? { |checkpoint| Time.iso8601(checkpoint.fetch("expires_at")) <= current_time }
    end

    def current_time
      value = @clock.call
      raise Error, "status clock must return a Time" unless value.is_a?(Time)

      value.getutc
    end

    def build_registry_workers(checkpoints)
      checkpoints.flat_map do |checkpoint|
        checkpoint.fetch("workers").filter_map do |worker|
          next unless worker["worker_snapshot"]

          snapshot = worker.fetch("worker_snapshot")
          RegistryWorker.new(
            registry: {
              registry_id: checkpoint.fetch("registry_id"),
              revision: checkpoint.fetch("revision"),
              published_at: Time.iso8601(checkpoint.fetch("published_at")),
              expires_at: Time.iso8601(checkpoint.fetch("expires_at")),
              sha256: checkpoint.fetch("snapshot_sha256")
            }.freeze,
            record: {
              "worker_id" => worker.fetch("worker_id"),
              "generation_id" => worker.fetch("generation_id"),
              "endpoint" => worker.fetch("endpoint"),
              "state" => worker.fetch("state"),
              "labels" => snapshot.fetch("labels"),
              "capabilities" => snapshot.fetch("capabilities"),
              "capability_fingerprint" => worker.fetch("capability_fingerprint")
            }.freeze
          )
        end
      end
    rescue KeyError, ArgumentError, TypeError, Error => e
      raise Error, "invalid accepted registry worker evidence: #{e.message}"
    end
  end
end
