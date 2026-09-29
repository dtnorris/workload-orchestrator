# frozen_string_literal: true

require "json"
require "time"

module WorkloadOrchestrator
  # Read-only, refreshable view of one persisted execution and its last
  # accepted dynamic-worker checkpoint.
  class ExecutionWatch
    DEFAULT_INTERVAL_SECONDS = 1.0
    CONSISTENCY_RETRIES = 3
    TERMINAL_STATUSES = %w[completed workload_failed].freeze

    class ReadOnlyStore
      def initialize(metadata)
        @metadata = metadata
      end

      def metadata_for(job)
        @metadata.fetch(job.id)
      end
    end
    private_constant :ReadOnlyStore

    def initialize(plan:, output:, out: $stdout, interval_seconds: DEFAULT_INTERVAL_SECONDS,
                   sleeper: ->(seconds) { sleep(seconds) }, clock: -> { Time.now.utc })
      @plan = plan
      @root = File.expand_path(output)
      @out = out
      @interval_seconds = Float(interval_seconds)
      raise Error, "watch interval must be positive" unless @interval_seconds.positive?

      @sleeper = sleeper
      @clock = clock
      @tty = out.respond_to?(:tty?) && out.tty?
      @report = ExecutionReport.new(plan: plan, output: output)
    rescue ArgumentError, TypeError
      raise Error, "watch interval must be a positive number"
    end

    def run
      refresh = 0
      loop do
        begin
          document = snapshot
        rescue TransientRead
          @sleeper.call(@interval_seconds)
          next
        end
        render(document, clear: @tty && refresh.positive?)
        return 0 if TERMINAL_STATUSES.include?(document.fetch("status"))

        refresh += 1
        @sleeper.call(@interval_seconds)
      end
    rescue Interrupt
      0
    end

    def snapshot
      CONSISTENCY_RETRIES.times do
        before = @report.document
        checkpoint = read_checkpoint
        attempts, metadata = running_attempts(before)
        after = @report.document
        next unless stable_report?(before, after)

        return build_snapshot(after, checkpoint, attempts, metadata)
      rescue TransientRead
        next
      end
      raise TransientRead, "execution changed while watch was reading it"
    end

    def render(document, clear: false)
      @out.print("\e[2J\e[H") if clear
      @out.puts "Batch: #{document.fetch('plan_id')}"
      @out.puts "State: #{document.fetch('display_state')}"
      @out.puts progress_line(document.fetch("counts"))
      @out.puts "Terminal: #{document.fetch('terminal')} / #{document.fetch('total')} " \
                "(#{document.fetch('progress_percent')}%)"
      print_registry(document.fetch("worker_registry"))
      print_attempts(document.fetch("running_attempts"))
      print_workers(document.fetch("worker_registry"))
      @out.puts "Evidence: #{@root}"
      @out.puts "---" unless @tty || TERMINAL_STATUSES.include?(document.fetch("status"))
    end

    private

    class TransientRead < StandardError; end
    private_constant :TransientRead

    def build_snapshot(report, checkpoint, attempts, metadata)
      registry = worker_registry(checkpoint, attempts)
      waiting = waiting_state(report, registry, metadata, checkpoint)
      report.merge(
        "display_state" => display_state(report, waiting),
        "waiting_for_capacity" => waiting == :waiting,
        "capacity_evaluation" => waiting.to_s,
        "running_attempts" => attempts,
        "worker_registry" => registry
      )
    end

    def running_attempts(report)
      metadata = report.fetch("jobs").to_h do |row|
        [row.fetch("job_id"), row.fetch("status") == "pending" ? nil : { "status" => row.fetch("status") }]
      end
      attempts = report.fetch("jobs").select { |row| row.fetch("status") == "running" }.map do |row|
        document = read_running_metadata(row)
        metadata[row.fetch("job_id")] = document
        attempt_row(row, document)
      end
      [attempts, metadata]
    end

    def read_running_metadata(row)
      path = File.join(@root, "runs", row.fetch("job_id"), "metadata.json")
      document = JSON.parse(File.read(path))
      consistent = document["status"] == "running" && document["attempt"] == row["attempt"]
      raise TransientRead unless consistent

      document
    rescue Errno::ENOENT, JSON::ParserError, KeyError
      raise TransientRead
    end

    def attempt_row(row, metadata)
      job = @plan.jobs.find { |candidate| candidate.id == row.fetch("job_id") }
      identity = metadata["worker_execution_identity"] || {}
      snapshot = metadata["worker_snapshot"] || {}
      {
        "job_id" => row.fetch("job_id"),
        "group_id" => job&.group_id,
        "pool_id" => row.fetch("pool_id"),
        "attempt" => row.fetch("attempt"),
        "worker_id" => identity["worker_id"] || row["worker"],
        "generation_id" => identity["generation_id"],
        "endpoint" => identity["endpoint"],
        "models" => models_from(snapshot),
        "worker_execution_identity" => identity,
        "elapsed_seconds" => elapsed_seconds(metadata["started_at"])
      }
    end

    def elapsed_seconds(started_at)
      return unless started_at

      [(current_time - Time.iso8601(started_at)).round, 0].max
    rescue ArgumentError
      nil
    end

    def current_time
      value = @clock.call
      raise Error, "watch clock must return a Time" unless value.is_a?(Time)

      value.getutc
    end

    def read_checkpoint
      path = File.join(@root, "dynamic-workers", "checkpoint.json")
      return unless File.file?(path)

      JSON.parse(File.read(path)).tap { |document| validate_checkpoint!(document) }
    rescue Errno::ENOENT
      nil
    rescue JSON::ParserError, KeyError, ArgumentError, TypeError => e
      raise Error, "cannot read worker registry checkpoint: #{e.message}"
    end

    def validate_checkpoint!(document)
      raise Error, "worker registry checkpoint must be an object" unless document.is_a?(Hash)
      unless document["contract_version"] == WorkerRegistryPoller::CHECKPOINT_VERSION
        raise Error, "worker registry checkpoint contract is invalid"
      end

      raise Error, "worker registry checkpoint workers must be an array" unless document["workers"].is_a?(Array)

      document.fetch("workers").each { |worker| validate_checkpoint_worker!(document, worker) }
    end

    def validate_checkpoint_worker!(checkpoint, worker)
      required = WorkerRegistryPoller::WORKER_KEYS
      optional = WorkerRegistryPoller::WORKER_OPTIONAL_KEYS
      unless worker.is_a?(Hash) && (required - worker.keys).empty? && (worker.keys - required - optional).empty?
        raise Error, "worker registry checkpoint worker fields are invalid"
      end

      expected = [checkpoint.fetch("registry_id"), worker.fetch("worker_id"),
                  worker.fetch("generation_id"), worker.fetch("endpoint"),
                  worker.fetch("capability_fingerprint")]
      raise Error, "worker registry checkpoint execution identity is invalid" unless
        worker.fetch("execution_identity") == expected
      unless DynamicWorkerRegistry::STATES.include?(worker.fetch("state"))
        raise Error, "worker registry checkpoint worker state is invalid"
      end
      return unless worker["worker_snapshot"]

      binding = DynamicWorkerBinding.from_metadata(binding_document(checkpoint, worker))
      raise Error, "worker registry checkpoint snapshot state conflicts" unless
        binding.worker_snapshot.fetch("state") == worker.fetch("state")
    end

    def binding_document(checkpoint, worker)
      {
        "worker_execution_identity" => identity_hash(worker.fetch("execution_identity")),
        "worker_registry_binding" => {
          "registry_revision" => checkpoint.fetch("revision"),
          "registry_snapshot_sha256" => checkpoint.fetch("snapshot_sha256")
        },
        "worker_snapshot" => worker.fetch("worker_snapshot")
      }
    end

    def worker_registry(checkpoint, attempts)
      return { "available" => false, "workers" => [], "counts" => empty_worker_counts } unless checkpoint

      busy = attempts.map do |attempt|
        DynamicWorkerBinding::IDENTITY_KEYS.map do |key|
          attempt.fetch("worker_execution_identity")[key]
        end
      end
      workers = checkpoint.fetch("workers").map { |worker| watched_worker(worker, busy) }
      mark_current_attempts!(attempts, workers)
      {
        "available" => true,
        "registry_id" => checkpoint.fetch("registry_id"),
        "revision" => checkpoint.fetch("revision"),
        "accepted_at" => checkpoint.fetch("accepted_at"),
        "capabilities_available" => checkpoint.fetch("workers").all? { |row| row.key?("worker_snapshot") },
        "counts" => worker_counts(workers),
        "workers" => workers
      }
    end

    def watched_worker(worker, busy)
      is_busy = busy.include?(worker.fetch("execution_identity"))
      availability = if is_busy
                       "busy"
                     elsif worker.fetch("state") == "READY"
                       "idle"
                     else
                       "unavailable"
                     end
      {
        "worker_id" => worker.fetch("worker_id"),
        "generation_id" => worker.fetch("generation_id"),
        "endpoint" => worker.fetch("endpoint"),
        "state" => worker.fetch("state"),
        "availability" => availability,
        "labels" => worker.dig("worker_snapshot", "labels") || [],
        "models" => models_from(worker["worker_snapshot"] || {}),
        "execution_identity" => worker.fetch("execution_identity")
      }
    end

    def mark_current_attempts!(attempts, workers)
      identities = workers.map { |worker| worker.fetch("execution_identity") }
      attempts.each do |attempt|
        identity = DynamicWorkerBinding::IDENTITY_KEYS.map do |key|
          attempt.fetch("worker_execution_identity")[key]
        end
        attempt["current_worker"] = identities.include?(identity)
      end
    end

    def registry_workers(checkpoint)
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

    def worker_counts(workers)
      states = DynamicWorkerRegistry::STATES.to_h do |state|
        [state, workers.count { |worker| worker.fetch("state") == state }]
      end
      states.merge(
        "busy" => workers.count { |worker| worker.fetch("availability") == "busy" },
        "idle_ready" => workers.count do |worker|
          worker.fetch("state") == "READY" && worker.fetch("availability") == "idle"
        end
      )
    end

    def empty_worker_counts
      DynamicWorkerRegistry::STATES.to_h { |state| [state, 0] }.merge("busy" => 0, "idle_ready" => 0)
    end

    def waiting_state(report, registry, metadata, checkpoint)
      return :not_applicable unless report.dig("counts", "pending").positive?
      return :checkpoint_absent unless registry.fetch("available")
      return :capabilities_unavailable unless registry.fetch("capabilities_available")
      return :not_applicable unless @plan.priority_scheduling?

      scheduler = DynamicScheduler.new(plan: @plan, store: ReadOnlyStore.new(metadata))
      workers = registry_workers(checkpoint)
      scheduler.assignments(workers: workers, current_workers: workers).empty? ? :waiting : :capacity_available
    rescue Error => e
      return :reconciliation_pending if e.message.include?("absent or replaced")

      raise
    end

    def display_state(report, waiting)
      return "Paused" if report.fetch("paused")

      if report["dispatch_halt"]
        halt = report.fetch("dispatch_halt")
        return "Dispatch halted (#{halt.fetch('kind')}): #{halt.fetch('error')}"
      end
      return "Circuit breaker tripped: #{report.dig('circuit_breaker', 'reason')}" if
        report.dig("circuit_breaker", "tripped")

      labels = {
        "interrupted" => "Interrupted", "infrastructure_failed" => "Infrastructure failed",
        "completed" => "Completed", "workload_failed" => "Workload failed",
        "cleanup_pending" => "Cleanup pending", "cleanup_failed" => "Cleanup failed",
        "owner_crashed" => "Owner crashed"
      }
      return labels.fetch(report.fetch("status")) if labels.key?(report.fetch("status"))
      return "Waiting for compatible capacity" if waiting == :waiting
      return "Waiting for first accepted worker checkpoint" if waiting == :checkpoint_absent
      return "Worker reconciliation pending" if waiting == :reconciliation_pending
      return "Capacity eligibility unavailable (legacy checkpoint)" if waiting == :capabilities_unavailable

      "Running"
    end

    def stable_report?(before, after)
      keys = %w[status updated_at paused jobs circuit_breaker dispatch_halt]
      before.slice(*keys) == after.slice(*keys)
    end

    def identity_hash(tuple)
      DynamicWorkerBinding::IDENTITY_KEYS.zip(tuple).to_h
    end

    def models_from(snapshot)
      snapshot.dig("capabilities", "ollama", "models")&.map { |model| model.fetch("model") } || []
    end

    def progress_line(counts)
      values = %w[complete running pending failed].map { |status| "#{counts.fetch(status)} #{status}" }
      "Progress: #{values.join(' / ')}"
    end

    def print_registry(registry)
      unless registry.fetch("available")
        @out.puts "Workers: no accepted registry checkpoint"
        return
      end

      counts = registry.fetch("counts")
      @out.puts "Workers: READY=#{counts.fetch('READY')} NOT_READY=#{counts.fetch('NOT_READY')} " \
                "UNAVAILABLE=#{counts.fetch('UNAVAILABLE')} busy=#{counts.fetch('busy')} " \
                "idle-ready=#{counts.fetch('idle_ready')}"
      @out.puts "Registry: #{registry.fetch('registry_id')} revision=#{registry.fetch('revision')}"
    end

    def print_attempts(attempts)
      @out.puts "Running attempts:"
      if attempts.empty?
        @out.puts "  none"
        return
      end

      attempts.each do |attempt|
        current = attempt.fetch("current_worker") ? "current" : "not-current"
        @out.puts "  job=#{attempt.fetch('job_id')} group=#{attempt['group_id'] || '-'} " \
                  "pool=#{attempt.fetch('pool_id')} worker=#{attempt['worker_id'] || '-'} " \
                  "generation=#{attempt['generation_id'] || '-'} models=#{model_text(attempt)} " \
                  "elapsed=#{attempt['elapsed_seconds'] || '-'}s #{current}"
      end
    end

    def print_workers(registry)
      return unless registry.fetch("available")

      @out.puts "Worker availability:"
      registry.fetch("workers").each do |worker|
        @out.puts "  worker=#{worker.fetch('worker_id')} generation=#{worker.fetch('generation_id')} " \
                  "state=#{worker.fetch('state')} availability=#{worker.fetch('availability')} " \
                  "models=#{model_text(worker)} labels=#{worker.fetch('labels').join(',')}"
      end
    end

    def model_text(row)
      models = row.fetch("models")
      models.empty? ? "unavailable" : models.join(",")
    end
  end
end
