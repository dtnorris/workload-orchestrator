# frozen_string_literal: true

require "open3"

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

  # Executes one argv-safe command for every requested registry snapshot.
  # The command is discovery-only: stdout is passed through unchanged for the
  # registry parser, and no provider or workload semantics are interpreted.
  class CommandWorkerSource < WorkerSource
    attr_reader :argv

    def initialize(command, args = [])
      super()
      values = [command, *args]
      unless values.all? { |value| value.is_a?(String) && !value.empty? }
        raise Error, "worker source command and arguments must be non-empty strings"
      end

      @argv = values.map { |value| value.dup.freeze }.freeze
    end

    def latest_snapshot
      stdout, stderr, status = Open3.capture3(*argv)
      return stdout if status.success?

      detail = stderr.strip
      message = "worker source command failed (#{status_detail(status)})"
      message = "#{message}: #{detail}" unless detail.empty?
      raise Error, message
    rescue SystemCallError => e
      raise Error, "cannot execute worker source command #{argv.first.inspect}: #{e.message}"
    end

    private

    def status_detail(status)
      return "exit #{status.exitstatus}" if status.exitstatus

      "signal #{status.termsig}"
    end
  end

  module DynamicWorkerCLI
    module_function

    def add_options(parser, options)
      options[:worker_source_args] = []
      parser.on("--worker-source-command FILE") { |value| options[:worker_source_command] = value }
      parser.on("--worker-source-arg ARG") { |value| options[:worker_source_args] << value }
    end

    def source_for(plan, options)
      command = options[:worker_source_command].to_s
      args = options.fetch(:worker_source_args)
      if unbound_plan?(plan)
        raise Error, missing_source_message if command.empty?

        return CommandWorkerSource.new(command, args)
      end

      return if command.empty? && args.empty?

      raise Error, "dynamic worker-source options require an unbound wlo-execution-plan/v0.3 plan"
    end

    def unbound_plan?(plan)
      plan.priority_scheduling? && !plan.execution_profile
    end

    def missing_source_message
      "wlo-execution-plan/v0.3 work-conserving execution requires --worker-source-command FILE"
    end
  end
end
