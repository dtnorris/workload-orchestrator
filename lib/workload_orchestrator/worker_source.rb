# frozen_string_literal: true

require "open3"
require "pathname"
require "yaml"

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
    attr_reader :argv, :environment, :workdir

    def initialize(command, args = [], environment: {}, workdir: Dir.pwd,
                   command_executor: OwnedCommandRunner.new)
      super()
      values = [command, *args]
      unless values.all? { |value| value.is_a?(String) && !value.empty? }
        raise Error, "worker source command and arguments must be non-empty strings"
      end
      unless environment.is_a?(Hash) && environment.all? do |name, value|
        name.is_a?(String) && name.match?(/\A[A-Za-z_][A-Za-z0-9_]*\z/) && value.is_a?(String)
      end
        raise Error, "worker source environment must map variable names to strings"
      end
      unless workdir.is_a?(String) && !workdir.empty? && File.directory?(File.expand_path(workdir))
        raise Error, "worker source workdir must be an existing directory"
      end

      @argv = values.map { |value| value.dup.freeze }.freeze
      @environment = environment.to_h { |name, value| [name.dup.freeze, value.dup.freeze] }.freeze
      @workdir = File.expand_path(workdir).freeze
      @command_executor = command_executor
    end

    def latest_snapshot
      stdout, stderr, status = @command_executor.call(environment, *argv, chdir: workdir)
      return stdout if status.success?

      detail = stderr.strip
      message = "worker source command failed (#{status_detail(status)})"
      message = "#{message}: #{detail}" unless detail.empty?
      raise Error, message
    rescue SystemCallError => e
      raise Error, "cannot execute worker source command #{argv.first.inspect}: #{e.message}"
    end

    def cancel(signal:, force: false)
      @command_executor.cancel(signal: signal, force: force)
    end

    def wait_for_cancellation
      @command_executor.wait_for_cancellation
    end

    private

    def status_detail(status)
      return "exit #{status.exitstatus}" if status.exitstatus

      "signal #{status.termsig}"
    end
  end

  # A named collection of independent registry publishers. Names identify
  # operator configuration and checkpoint paths only; a publisher's own
  # registry_id remains its scheduling namespace.
  class WorkerSourceSet
    NAME = /\A[a-z0-9][a-z0-9_-]{0,63}\z/
    Entry = Struct.new(:name, :source, keyword_init: true)

    attr_reader :entries

    def self.single(source, name: "default", legacy_checkpoint: true)
      new([Entry.new(name:, source:)], legacy_checkpoint:)
    end

    def self.coerce(value)
      return value if value.is_a?(self)
      return single(value) if value.respond_to?(:latest_snapshot)

      raise Error, "dynamic execution requires one or more worker sources"
    end

    def initialize(entries, legacy_checkpoint: false)
      @entries = Array(entries).map do |entry|
        name = entry.name
        source = entry.source
        raise Error, "worker source name is invalid: #{name.inspect}" unless name.is_a?(String) && name.match?(NAME)
        unless source.respond_to?(:latest_snapshot)
          raise Error, "worker source #{name.inspect} must implement #latest_snapshot"
        end

        Entry.new(name: name.dup.freeze, source:).freeze
      end.freeze
      names = @entries.map(&:name)
      raise Error, "worker source set must not be empty" if names.empty?
      raise Error, "worker source names must be unique" unless names.uniq == names
      if legacy_checkpoint && names.length != 1
        raise Error, "legacy worker checkpoint compatibility requires exactly one source"
      end

      @legacy_checkpoint = legacy_checkpoint
      freeze
    end

    def legacy_checkpoint?
      @legacy_checkpoint
    end

    def cancel(signal:, force: false)
      entries.sum do |entry|
        source = entry.source
        source.respond_to?(:cancel) ? source.cancel(signal:, force:) : 0
      end
    end

    def wait_for_cancellation
      entries.each do |entry|
        source = entry.source
        source.wait_for_cancellation if source.respond_to?(:wait_for_cancellation)
      end
    end
  end

  # Strict WLO-owned production configuration for argv-safe registry sources.
  class WorkerSourceConfiguration
    CONTRACT_VERSION = "wlo-worker-sources/v0.1"
    ROOT_KEYS = %w[contract_version sources].freeze
    SOURCE_KEYS = %w[name command args environment workdir].freeze

    def self.load(path)
      expanded = File.expand_path(path)
      document = YAML.safe_load_file(expanded, permitted_classes: [], aliases: false)
      new(document, path: expanded).source_set
    rescue Errno::ENOENT, Psych::Exception => e
      raise Error, "invalid worker source configuration #{expanded || path}: #{e.message}"
    end

    attr_reader :source_set

    def initialize(document, path: nil)
      exact_keys!(document, ROOT_KEYS, "worker source configuration")
      unless document.fetch("contract_version") == CONTRACT_VERSION
        raise Error, "worker source configuration contract must be #{CONTRACT_VERSION}"
      end

      rows = document.fetch("sources")
      raise Error, "worker source configuration sources must be a non-empty array" unless
        rows.is_a?(Array) && !rows.empty?

      entries = rows.map.with_index { |row, index| build_entry(row, index, path) }
      @source_set = WorkerSourceSet.new(entries)
      freeze
    rescue KeyError, TypeError => e
      raise Error, "invalid worker source configuration: #{e.message}"
    end

    private

    def build_entry(row, index, path)
      label = "worker source configuration sources[#{index}]"
      exact_keys!(row, SOURCE_KEYS, label)
      name = row.fetch("name")
      command = non_empty_string!(row.fetch("command"), "#{label}.command")
      args = row.fetch("args")
      unless args.is_a?(Array) && args.all? { |value| value.is_a?(String) && !value.empty? }
        raise Error, "#{label}.args must be an array of non-empty strings"
      end

      environment = row.fetch("environment")
      workdir = resolve_workdir(row.fetch("workdir"), path)
      source = CommandWorkerSource.new(command, args, environment:, workdir:)
      WorkerSourceSet::Entry.new(name:, source:)
    end

    def resolve_workdir(value, path)
      workdir = non_empty_string!(value, "worker source configuration workdir")
      return workdir if Pathname.new(workdir).absolute? || path.nil?

      File.expand_path(workdir, File.dirname(path))
    end

    def exact_keys!(value, expected, label)
      raise Error, "#{label} must be a mapping" unless value.is_a?(Hash)
      raise Error, "#{label} fields are invalid" unless value.keys.sort == expected.sort
    end

    def non_empty_string!(value, label)
      return value if value.is_a?(String) && !value.empty? && value == value.strip

      raise Error, "#{label} must be a non-empty trimmed string"
    end
  end

  module DynamicWorkerCLI
    module_function

    def add_options(parser, options)
      options[:worker_source_args] = []
      parser.on("--worker-sources-config FILE") { |value| options[:worker_sources_config] = value }
      parser.on("--worker-source-command FILE") { |value| options[:worker_source_command] = value }
      parser.on("--worker-source-arg ARG") { |value| options[:worker_source_args] << value }
    end

    def source_for(plan, options)
      config = options[:worker_sources_config].to_s
      config = ENV.fetch("WLO_WORKER_SOURCES_CONFIG", "").to_s if config.empty?
      command = options[:worker_source_command].to_s
      args = options.fetch(:worker_source_args)
      if unbound_plan?(plan)
        unless config.empty?
          unless command.empty? && args.empty?
            raise Error, "--worker-sources-config cannot be combined with legacy worker-source command options"
          end

          return WorkerSourceConfiguration.load(config)
        end
        raise Error, missing_source_message if command.empty?

        return WorkerSourceSet.single(CommandWorkerSource.new(command, args))
      end

      return if config.empty? && command.empty? && args.empty?

      raise Error, "dynamic worker-source options require an unbound wlo-execution-plan/v0.3 plan"
    end

    def unbound_plan?(plan)
      plan.priority_scheduling? && !plan.execution_profile
    end

    def missing_source_message
      "wlo-execution-plan/v0.3 work-conserving execution requires --worker-sources-config FILE " \
        "or --worker-source-command FILE"
    end
  end
end
