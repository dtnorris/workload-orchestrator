# frozen_string_literal: true

module WorkloadOrchestrator
  # Coordinates independently checkpointed registry publishers and exposes
  # their accepted workers as one scheduler view without republishing them.
  class WorkerRegistrySet
    attr_reader :pollers, :current_workers, :ready_workers, :source_registry_ids,
                :accepted_checkpoints, :usable_checkpoints, :source_health

    def initialize(sources:, checkpoint_root:, clock: -> { Time.now.utc },
                   interval_seconds: WorkerRegistryPoller::DEFAULT_INTERVAL_SECONDS, sleeper: nil)
      @sources = WorkerSourceSet.coerce(sources)
      @checkpoint_root = File.expand_path(checkpoint_root)
      @clock = clock
      @interval_seconds = Float(interval_seconds)
      raise Error, "worker registry poll interval must be positive" unless @interval_seconds.positive?

      @sleeper = sleeper || method(:interruptible_sleep)
      @pollers = build_pollers.freeze
      @current_workers = [].freeze
      @ready_workers = [].freeze
      refresh_views!
    rescue ArgumentError, TypeError
      raise Error, "worker registry poll interval must be a positive number"
    end

    def poll_once
      pollers.each_value do |poller|
        poller.poll_once
      rescue Error
        # The poller durably records its source-local failure. Other sources
        # must still be polled and may continue contributing fresh capacity.
      end
      refresh_views!
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

    def source_checkpoints
      pollers.transform_values(&:accepted_checkpoint).freeze
    end

    # Compatibility for existing single-source callers and retained state.
    def accepted_checkpoint
      checkpoints = accepted_checkpoints.values
      return checkpoints.first if checkpoints.length <= 1

      raise Error, "multiple registry checkpoints are active; select one by registry_id"
    end

    def registry
      registries = pollers.values.filter_map(&:registry)
      return registries.first if registries.length <= 1

      raise Error, "multiple registries are active; select one by configured source"
    end

    def dispatch_blocked?
      source_health.values.any? { |row| row.fetch("blocking") }
    end

    def blocking_sources
      source_health.values.select { |row| row.fetch("blocking") }.freeze
    end

    def source_health_by_registry_id
      source_health.values.filter_map do |row|
        registry_id = row.fetch("registry_id")
        registry_id && [registry_id, row]
      end.to_h.freeze
    end

    private

    def build_pollers
      @sources.entries.to_h do |entry|
        path = if @sources.legacy_checkpoint?
                 File.join(@checkpoint_root, "checkpoint.json")
               else
                 File.join(@checkpoint_root, "sources", entry.name, "checkpoint.json")
               end
        poller = WorkerRegistryPoller.new(
          source: entry.source, checkpoint_path: path, clock: @clock,
          interval_seconds: @interval_seconds, source_name: entry.name,
          policy: entry.policy
        )
        [entry.name, poller]
      end
    end

    def refresh_views!
      workers = pollers.values.flat_map(&:current_workers).sort_by(&:execution_identity)
      identities = workers.map(&:execution_identity)
      unless identities.uniq == identities
        raise Error, "configured worker sources published duplicate registry-qualified identities"
      end

      @current_workers = workers.freeze
      @ready_workers = workers.select(&:ready?).freeze
      refresh_checkpoint_index!
      @source_health = pollers.transform_values(&:health).freeze
      usable = @source_health.values.filter_map do |row|
        registry_id = row.fetch("registry_id")
        @accepted_checkpoints[registry_id] if registry_id && row.fetch("usable")
      end
      @usable_checkpoints = usable.to_h do |checkpoint|
        [checkpoint.fetch("registry_id"), checkpoint]
      end.freeze
    end

    def refresh_checkpoint_index!
      checkpoints = pollers.transform_values(&:accepted_checkpoint).compact
      by_registry = {}
      source_ids = {}
      checkpoints.each do |name, checkpoint|
        registry_id = checkpoint.fetch("registry_id")
        if by_registry.key?(registry_id)
          raise Error, "worker sources #{source_ids.key(registry_id).inspect} and #{name.inspect} " \
                       "published duplicate registry_id #{registry_id.inspect}"
        end

        by_registry[registry_id] = checkpoint
        source_ids[name] = registry_id
      end
      @accepted_checkpoints = by_registry.freeze
      @source_registry_ids = source_ids.freeze
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
