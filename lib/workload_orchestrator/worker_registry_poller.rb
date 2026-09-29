# frozen_string_literal: true

require "fileutils"
require "json"
require "time"

module WorkloadOrchestrator
  # Polls one provider-neutral WorkerSource, durably remembers the greatest
  # accepted snapshot, and exposes an immutable view for later scheduling work.
  class WorkerRegistryPoller
    CHECKPOINT_VERSION = "wlo-worker-registry-checkpoint/v0.1"
    DEFAULT_INTERVAL_SECONDS = 5.0
    CHECKPOINT_KEYS = %w[
      contract_version registry_id revision snapshot_sha256 published_at expires_at
      accepted_at workers last_reconciliation reconciliation_history
    ].freeze
    WORKER_KEYS = %w[
      worker_id generation_id endpoint state capability_fingerprint execution_identity
    ].freeze

    attr_reader :current_workers, :ready_workers, :last_reconciliation,
                :reconciliation_history, :registry

    def initialize(source:, checkpoint_path:, clock: -> { Time.now.utc },
                   interval_seconds: DEFAULT_INTERVAL_SECONDS, sleeper: nil)
      raise Error, "worker source must implement #latest_snapshot" unless source.respond_to?(:latest_snapshot)

      @source = source
      @checkpoint_path = File.expand_path(checkpoint_path)
      @clock = clock
      @interval_seconds = Float(interval_seconds)
      raise Error, "worker registry poll interval must be positive" unless @interval_seconds.positive?

      @sleeper = sleeper || method(:interruptible_sleep)
      @checkpoint = load_checkpoint
      @registry = nil
      @current_workers = [].freeze
      @ready_workers = [].freeze
      @last_reconciliation = empty_reconciliation
      @reconciliation_history = @checkpoint ? @checkpoint.fetch("reconciliation_history") : [].freeze
    rescue ArgumentError, TypeError
      raise Error, "worker registry poll interval must be a positive number"
    end

    def poll_once
      now = current_time
      candidate = DynamicWorkerRegistry.from_source(@source, now: now, previous: registry)
      validate_checkpoint_transition!(candidate) unless registry
      reconciliation = reconcile(previous_workers, candidate.entries)
      checkpoint = checkpoint_for(candidate, reconciliation, now)
      write_checkpoint(checkpoint)

      @registry = candidate
      @current_workers = candidate.entries
      @ready_workers = candidate.schedulable_workers
      @last_reconciliation = deep_freeze(reconciliation)
      @reconciliation_history = @checkpoint.fetch("reconciliation_history")
      self
    end

    def run(stop:)
      raise Error, "worker registry stop condition must be callable" unless stop.respond_to?(:call)

      until stop.call
        poll_once
        yield self if block_given?
        break if stop.call

        @sleeper.call(@interval_seconds, stop)
      end
      self
    end

    private

    def current_time
      value = @clock.call
      return value.getutc if value.is_a?(Time)

      raise Error, "worker registry clock must return a Time"
    end

    def validate_checkpoint_transition!(candidate)
      return unless @checkpoint

      prior_id = @checkpoint.fetch("registry_id")
      prior_revision = @checkpoint.fetch("revision")
      unless candidate.registry_id == prior_id
        raise Error,
              "worker registry identity changed from #{prior_id.inspect} to #{candidate.registry_id.inspect}"
      end
      if candidate.revision < prior_revision
        raise Error,
              "worker registry revision rolled back from #{prior_revision} to #{candidate.revision}"
      end
      if candidate.revision == prior_revision
        return if candidate.sha256 == @checkpoint.fetch("snapshot_sha256")

        raise Error, "worker registry revision #{candidate.revision} changed contents"
      end

      prior_publication = Time.iso8601(@checkpoint.fetch("published_at"))
      return if candidate.published_at > prior_publication

      raise Error, "worker registry publication time did not advance with revision"
    end

    def previous_workers
      return registry.entries.map { |worker| worker_row(worker) } if registry
      return [] unless @checkpoint

      @checkpoint.fetch("workers")
    end

    def reconcile(previous, current)
      before = workers_by_id(previous)
      after = workers_by_id(current.map { |worker| worker_row(worker) })

      {
        "arrived" => (after.keys - before.keys).sort.map { |id| after.fetch(id) },
        "disappeared" => (before.keys - after.keys).sort.map { |id| before.fetch(id) },
        "changed" => changed_workers(before, after)
      }
    end

    def workers_by_id(workers)
      workers.to_h { |worker| [worker.fetch("worker_id"), worker] }
    end

    def changed_workers(before, after)
      (before.keys & after.keys).sort.filter_map do |id|
        prior = before.fetch(id)
        replacement = after.fetch(id)
        kinds = change_kinds(prior, replacement)
        next if kinds.empty?

        { "worker_id" => id, "kinds" => kinds, "previous" => prior, "current" => replacement }
      end
    end

    def change_kinds(previous, current)
      {
        "generation" => "generation_id",
        "endpoint" => "endpoint",
        "capability" => "capability_fingerprint",
        "state" => "state"
      }.filter_map { |kind, field| kind unless previous.fetch(field) == current.fetch(field) }
    end

    def worker_row(worker)
      {
        "worker_id" => worker.worker_id,
        "generation_id" => worker.generation_id,
        "endpoint" => worker.endpoint,
        "state" => worker.state,
        "capability_fingerprint" => worker.capability_fingerprint,
        "execution_identity" => worker.execution_identity
      }
    end

    def checkpoint_for(candidate, reconciliation, accepted_at)
      history = @checkpoint ? @checkpoint.fetch("reconciliation_history").dup : []
      unless reconciliation.values.all?(&:empty?)
        history << {
          "revision" => candidate.revision,
          "snapshot_sha256" => candidate.sha256,
          "observed_at" => accepted_at.iso8601,
          "changes" => reconciliation
        }
      end
      {
        "contract_version" => CHECKPOINT_VERSION,
        "registry_id" => candidate.registry_id,
        "revision" => candidate.revision,
        "snapshot_sha256" => candidate.sha256,
        "published_at" => candidate.published_at.iso8601,
        "expires_at" => candidate.expires_at.iso8601,
        "accepted_at" => accepted_at.iso8601,
        "workers" => candidate.entries.map { |worker| worker_row(worker) },
        "last_reconciliation" => reconciliation,
        "reconciliation_history" => history
      }
    end

    def load_checkpoint
      return unless File.file?(@checkpoint_path)

      document = JSON.parse(File.read(@checkpoint_path))
      validate_checkpoint_header!(document)
      validate_checkpoint_workers!(document.fetch("workers"), document.fetch("registry_id"))
      validate_reconciliation!(document.fetch("last_reconciliation"))
      validate_reconciliation_history!(document.fetch("reconciliation_history"))

      deep_freeze(document)
    rescue Errno::ENOENT, JSON::ParserError, KeyError, ArgumentError, TypeError => e
      raise Error, "invalid worker registry checkpoint #{@checkpoint_path}: #{e.message}"
    end

    def validate_checkpoint_header!(document)
      exact_keys!(document, CHECKPOINT_KEYS, "worker registry checkpoint")
      raise Error, "worker registry checkpoint contract is invalid" unless
        document.fetch("contract_version") == CHECKPOINT_VERSION
      raise Error, "worker registry checkpoint registry_id is invalid" unless
        document.fetch("registry_id").is_a?(String) && !document.fetch("registry_id").empty?
      raise Error, "worker registry checkpoint revision is invalid" unless
        document.fetch("revision").is_a?(Integer) && !document.fetch("revision").negative?

      digest = document.fetch("snapshot_sha256")
      raise Error, "worker registry checkpoint digest is invalid" unless
        digest.is_a?(String) && digest.match?(DynamicWorkerRegistry::SHA256)

      %w[published_at expires_at accepted_at].each { |field| Time.iso8601(document.fetch(field)) }
    end

    def validate_checkpoint_workers!(workers, registry_id)
      raise Error, "worker registry checkpoint workers must be an array" unless workers.is_a?(Array)

      ids = workers.map do |worker|
        exact_keys!(worker, WORKER_KEYS, "worker registry checkpoint worker")
        identity = worker.fetch("execution_identity")
        expected = [registry_id, worker.fetch("worker_id"), worker.fetch("generation_id"),
                    worker.fetch("endpoint"), worker.fetch("capability_fingerprint")]
        raise Error, "worker registry checkpoint execution identity is invalid" unless identity == expected
        raise Error, "worker registry checkpoint worker state is invalid" unless
          DynamicWorkerRegistry::STATES.include?(worker.fetch("state"))

        worker.fetch("worker_id")
      end
      raise Error, "worker registry checkpoint has duplicate workers" unless ids.uniq == ids
    end

    def validate_reconciliation!(value)
      exact_keys!(value, %w[arrived disappeared changed], "worker registry reconciliation")
      raise Error, "worker registry reconciliation is invalid" unless value.values.all?(Array)
    end

    def validate_reconciliation_history!(history)
      raise Error, "worker registry reconciliation history is invalid" unless history.is_a?(Array)

      history.each do |entry|
        exact_keys!(entry, %w[revision snapshot_sha256 observed_at changes],
                    "worker registry reconciliation history entry")
        raise Error, "worker registry reconciliation history revision is invalid" unless
          entry.fetch("revision").is_a?(Integer) && !entry.fetch("revision").negative?

        Time.iso8601(entry.fetch("observed_at"))
        validate_reconciliation!(entry.fetch("changes"))
      end
    end

    def exact_keys!(value, expected, label)
      raise Error, "#{label} must be an object" unless value.is_a?(Hash)
      raise Error, "#{label} fields are invalid" unless value.keys.sort == expected.sort
    end

    def write_checkpoint(value)
      FileUtils.mkdir_p(File.dirname(@checkpoint_path))
      temporary = "#{@checkpoint_path}.tmp.#{Process.pid}.#{Thread.current.object_id}"
      File.write(temporary, "#{JSON.pretty_generate(value)}\n")
      File.rename(temporary, @checkpoint_path)
      @checkpoint = deep_freeze(JSON.parse(JSON.generate(value)))
    ensure
      File.delete(temporary) if defined?(temporary) && temporary && File.exist?(temporary)
    end

    def empty_reconciliation
      deep_freeze("arrived" => [], "disappeared" => [], "changed" => [])
    end

    def deep_freeze(value)
      case value
      when Hash
        value.each do |key, item|
          deep_freeze(key)
          deep_freeze(item)
        end
      when Array then value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end

    def interruptible_sleep(seconds, stop)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      until stop.call
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        break unless remaining.positive?

        sleep([remaining, 0.1].min)
      end
    end
  end
end
