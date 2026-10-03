# frozen_string_literal: true

require "json"
require "time"

module WorkloadOrchestrator
  # Read-only, refreshable view of one persisted execution and its last
  # accepted dynamic-worker checkpoint.
  class ExecutionWatch
    DEFAULT_INTERVAL_SECONDS = 1.0
    CONSISTENCY_RETRIES = 3
    TERMINAL_STATUSES = %w[completed workload_failed interrupted].freeze

    def initialize(plan:, output:, out: $stdout, interval_seconds: DEFAULT_INTERVAL_SECONDS,
                   sleeper: ->(seconds) { sleep(seconds) }, **view_options)
      @plan = plan
      @root = File.expand_path(output)
      @out = out
      @interval_seconds = Float(interval_seconds)
      raise Error, "watch interval must be positive" unless @interval_seconds.positive?

      @sleeper = sleeper
      @clock = view_options.fetch(:clock, -> { Time.now.utc })
      unknown = view_options.keys - %i[clock width verbose]
      raise Error, "unknown watch view options: #{unknown.join(', ')}" unless unknown.empty?

      @dashboard = ExecutionDashboard.new(
        width: view_options.fetch(:width, ExecutionDashboard::DEFAULT_WIDTH)
      )
      @verbose = view_options.fetch(:verbose, false)
      @tty = out.respond_to?(:tty?) && out.tty?
      @report = ExecutionReport.new(plan: plan, output: output, clock: @clock)
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
        checkpoints = read_checkpoints
        attempts = running_attempts(before)
        after = @report.document
        next unless stable_report?(before, after)

        return build_snapshot(after, checkpoints, attempts)
      rescue TransientRead
        next
      end
      raise TransientRead, "execution changed while watch was reading it"
    end

    def render(document, clear: false)
      @out.print("\e[2J\e[H") if clear
      return render_dashboard(document) unless @verbose

      @out.puts "Batch: #{document.fetch('plan_id')}"
      @out.puts "State: #{document.fetch('display_state')}"
      @out.puts progress_line(document.fetch("counts"))
      @out.puts "Terminal: #{document.fetch('terminal')} / #{document.fetch('total')} " \
                "(#{document.fetch('progress_percent')}%)"
      print_pools(document.fetch("pool_status"))
      print_registry(document.fetch("worker_registry"))
      print_source_health(document.fetch("worker_sources"))
      print_attempts(document.fetch("running_attempts"))
      print_workers(document.fetch("worker_registry"))
      @out.puts "Evidence: #{@root}"
      @out.puts "---" unless @tty || TERMINAL_STATUSES.include?(document.fetch("status"))
    end

    private

    class TransientRead < StandardError; end
    private_constant :TransientRead

    def render_dashboard(document)
      @out.write(@dashboard.render(document))
      @out.puts "---" unless @tty || TERMINAL_STATUSES.include?(document.fetch("status"))
    end

    def build_snapshot(report, checkpoints, attempts)
      registry = worker_registry(checkpoints, attempts)
      waiting = waiting_state(report)
      report.merge(
        "display_state" => display_state(report, waiting),
        "waiting_for_capacity" => waiting == :waiting,
        "capacity_evaluation" => waiting.to_s,
        "running_attempts" => attempts,
        "worker_registry" => registry
      )
    end

    def running_attempts(report)
      report.fetch("jobs").select { |row| row.fetch("status") == "running" }.map do |row|
        document = read_running_metadata(row)
        attempt_row(row, document)
      end
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

    def read_checkpoints
      source_paths = Dir.glob(File.join(@root, "dynamic-workers", "sources", "*", "checkpoint.json"))
      legacy_path = File.join(@root, "dynamic-workers", "checkpoint.json")
      paths = source_paths.empty? && File.file?(legacy_path) ? [legacy_path] : source_paths
      documents = paths.map do |path|
        JSON.parse(File.read(path)).tap { |document| validate_checkpoint!(document) }
      end
      ids = documents.map { |document| document.fetch("registry_id") }
      raise Error, "worker registry checkpoints contain duplicate registry identities" unless ids.uniq == ids

      documents
    rescue Errno::ENOENT
      []
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

    def worker_registry(checkpoints, attempts)
      checkpoints = checkpoints.select do |checkpoint|
        Time.iso8601(checkpoint.fetch("expires_at")) > current_time
      end
      return { "available" => false, "workers" => [], "counts" => empty_worker_counts } if checkpoints.empty?

      busy = attempts.map do |attempt|
        DynamicWorkerBinding::IDENTITY_KEYS.map do |key|
          attempt.fetch("worker_execution_identity")[key]
        end
      end
      workers = checkpoints.flat_map do |checkpoint|
        checkpoint.fetch("workers").map do |worker|
          watched_worker(worker, busy, checkpoint.fetch("registry_id"))
        end
      end
      mark_current_attempts!(attempts, workers)
      registries = checkpoints.map do |checkpoint|
        checkpoint.slice("registry_id", "revision", "accepted_at")
      end
      {
        "available" => true,
        "registry_id" => registries.one? ? registries.first.fetch("registry_id") : nil,
        "revision" => registries.one? ? registries.first.fetch("revision") : nil,
        "accepted_at" => registries.map { |row| row.fetch("accepted_at") }.max,
        "registries" => registries,
        "capabilities_available" => checkpoints.all? do |checkpoint|
          checkpoint.fetch("workers").all? { |row| row.key?("worker_snapshot") }
        end,
        "counts" => worker_counts(workers),
        "workers" => workers
      }
    end

    def watched_worker(worker, busy, registry_id)
      is_busy = busy.include?(worker.fetch("execution_identity"))
      availability = if is_busy
                       "busy"
                     elsif worker.fetch("state") == "READY"
                       "idle"
                     else
                       "unavailable"
                     end
      {
        "registry_id" => registry_id,
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

    def waiting_state(report)
      return :not_applicable unless report.dig("counts", "pending").positive?

      reasons = report.fetch("pool_status").map { |pool| pool.fetch("reason") }
      return :waiting if reasons.intersect?(ExecutionPoolStatus::CAPACITY_WAIT_REASONS)
      return :capacity_available if reasons.include?("READY_TO_DISPATCH")
      return :no_runnable_work if reasons.include?("NO_RUNNABLE_WORK")

      :not_applicable
    end

    def display_state(report, waiting)
      terminal_labels = { "completed" => "Completed", "workload_failed" => "Workload failed" }
      return terminal_labels.fetch(report.fetch("status")) if terminal_labels.key?(report.fetch("status"))
      return "Paused" if report.fetch("paused")
      return "Circuit breaker tripped: #{report.dig('circuit_breaker', 'reason')}" if
        report.dig("circuit_breaker", "tripped")

      if report["dispatch_halt"]
        halt = report.fetch("dispatch_halt")
        return "Dispatch halted (#{halt.fetch('kind')}): #{halt.fetch('error')}"
      end

      labels = {
        "interrupted" => "Interrupted", "infrastructure_failed" => "Infrastructure failed",
        "cleanup_pending" => "Cleanup pending", "cleanup_failed" => "Cleanup failed",
        "owner_crashed" => "Owner crashed"
      }
      return labels.fetch(report.fetch("status")) if labels.key?(report.fetch("status"))

      pool_reasons = report.fetch("pool_status").map { |pool| pool.fetch("reason") }
      if waiting == :waiting
        reason = pool_reasons.find { |candidate| ExecutionPoolStatus::CAPACITY_WAIT_REASONS.include?(candidate) }
        return "Waiting: #{reason.downcase.tr('_', ' ')}"
      end
      return "Ready to dispatch" if waiting == :capacity_available
      return "Waiting: no runnable work" if waiting == :no_runnable_work

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
      statuses = %w[complete running pending failed]
      statuses << "interrupted" if counts.fetch("interrupted", 0).positive?
      values = statuses.map { |status| "#{counts.fetch(status)} #{status}" }
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
      registry.fetch("registries").each do |row|
        @out.puts "Registry: #{row.fetch('registry_id')} revision=#{row.fetch('revision')}"
      end
    end

    def print_source_health(sources)
      return if sources.empty?

      sources.each do |source|
        detail = source.fetch("failure_reason")
        detail = " reason=#{detail}" if detail
        @out.puts "Source: #{source.fetch('source_name')} policy=#{source.fetch('policy')} " \
                  "state=#{source.fetch('state')} poll=#{source.fetch('last_poll_result')} " \
                  "blocking=#{source.fetch('blocking')}#{detail}"
      end
    end

    def print_pools(pools)
      return if pools.empty?

      @out.puts "Pool             Jobs C/R/F/I/P  Ready  Busy  Idle  State"
      pools.each do |pool|
        jobs = pool.fetch("jobs")
        workers = pool.fetch("workers")
        counts = %w[complete running failed interrupted pending].map { |key| jobs.fetch(key) }.join("/")
        @out.puts format(
          "%<pool>-16s %<counts>-13s %<ready>5d %<busy>5d %<idle>5d  %<state>s",
          pool: pool.fetch("pool_id"), counts:, ready: workers.fetch("compatible_ready"),
          busy: workers.fetch("busy"), idle: workers.fetch("idle"), state: pool.fetch("reason")
        )
      end
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
