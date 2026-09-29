# frozen_string_literal: true

module WorkloadOrchestrator
  # Provider-neutral input seam for dynamic worker-registry snapshots.
  # Implementations return the latest complete snapshot as JSON bytes; parsing,
  # validation, and reconciliation remain WLO responsibilities.
  class WorkerSource
    def latest_snapshot
      raise NotImplementedError, "worker sources must implement #latest_snapshot"
    end
  end

  # In-memory source for fixtures and callers that already possess a snapshot.
  class StaticWorkerSource < WorkerSource
    def self.from_file(path)
      new(File.binread(File.expand_path(path)))
    rescue Errno::ENOENT => e
      raise Error, e.message
    end

    def initialize(snapshot_bytes)
      super()
      raise Error, "worker source snapshot must be JSON bytes" unless snapshot_bytes.is_a?(String)

      @snapshot_bytes = snapshot_bytes.dup.freeze
    end

    def latest_snapshot
      @snapshot_bytes
    end
  end
end
