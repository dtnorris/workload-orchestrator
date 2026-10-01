# frozen_string_literal: true

require "fileutils"
require "json"
require "optparse"

module WorkloadOrchestrator
  PROCESS_OWNERSHIP_HELP = <<~HELP.freeze
    Process ownership:
      run, resume    FOREGROUND WORK OWNER. Ctrl-C interrupts the foreground runner; it is not
                     the documented graceful-pause path. WLO never tears down paid provider capacity.
                     Use `wlo pause --output DIR` for an intentional graceful workload pause.
      start          DETACHED WORK LAUNCHER. The manager continues after this CLI and its
                     terminal exit. Later Ctrl-C in the launching shell does not stop it.
      watch          READ-ONLY OBSERVER. Ctrl-C closes only the view; execution and paid
                     provider resources are unchanged.
      status,
      summary        ONE-SHOT INSPECTION. Interrupting the request changes no lifecycle state.
      pause          CONTROL REQUEST. Stops new WLO dispatch and lets already-running work
                     finish under the existing pause contract; it is not provider teardown.
      retry-failed,
      import-terminal CONTROL REQUEST. Changes retained WLO execution evidence/state only;
                      it neither starts execution nor changes provider capacity.
      validate, plan,
      worker-check   ONE-SHOT INSPECTION. No workload or provider lifecycle is owned.

    Paid teardown is a separate RPOF action. For campaign-owned capacity, use
    `bin/rpof campaign stop ...` and verify provider absence; shell or terminal loss is
    never a substitute for an explicit lifecycle command.
  HELP

  class CLI
    DEFAULT_ROOT = File.expand_path("../..", __dir__).freeze

    def initialize(argv, out: $stdout, err: $stderr, root: DEFAULT_ROOT,
                   worker_poll_interval: WorkerRegistryPoller::DEFAULT_INTERVAL_SECONDS)
      @argv = argv.dup
      @out = out
      @err = err
      @root = File.expand_path(root)
      @worker_poll_interval = worker_poll_interval
    end

    def run
      command = @argv.shift
      return help if command.nil? || %w[--help -h].include?(command)
      return version if %w[--version -v].include?(command)

      dispatch(command)
    rescue Error, OptionParser::ParseError => e
      @err.puts "ERROR: #{e.message}"
      1
    end

    private

    def dispatch(command)
      case command
      when "validate" then validate_command
      when "plan" then plan_command
      when "worker-check" then worker_check_command
      when "start" then run_command(resume: false, detached: true)
      when "run" then run_command(resume: false)
      when "resume" then run_command(resume: true)
      when "retry-failed" then retry_failed_command
      when "import-terminal" then import_terminal_command
      when "status", "summary", "watch" then reporting_command(command)
      when "pause" then pause_command
      else raise Error, "unknown command #{command.inspect}; run bin/wlo --help"
      end
    end

    def reporting_command(command)
      return status_command if command == "status"
      return status_command(human: true) if command == "summary"

      watch_command
    end

    def validate_command
      plan_path = required_argument!("PLAN.json")
      options = parse_profile_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      @out.puts "VALID #{plan.id} #{plan.sha256}"
      0
    end

    def plan_command
      plan_path = required_argument!("PLAN.json")
      options = parse_runtime_options(require_output: false)
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      if plan.execution_profile&.rpof?
        @out.puts "Plan: #{plan.id} (#{plan.sha256})"
        @out.puts "Execution profile: #{plan.execution_profile.sha256}"
        @out.puts JSON.pretty_generate(plan.execution_profile.document)
        @out.puts "Execution: historical RPOF profile (read-only; execution retired)"
        require_workdir(options)
        return 0
      end
      if DynamicWorkerCLI.unbound_plan?(plan)
        PlanPrinter.new(@out).print(plan, WorkerSet.new({}), require_workdir(options), dynamic: true)
        return 0
      end
      workers = load_workers(options)
      workers.validate_plan!(plan)
      workdir = require_workdir(options)
      PlanPrinter.new(@out).print(plan, workers, workdir)
      0
    end

    def worker_check_command
      plan_path = required_argument!("PLAN.json")
      options = parse_worker_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      if DynamicWorkerCLI.unbound_plan?(plan)
        raise Error,
              "dynamic v0.3 worker-check is not supported; inspect the registry source directly " \
              "(for RPOF: bin/rpof workers --json)"
      end
      plan.execution_profile&.ensure_runnable!
      workers = load_workers(options)
      results = WorkerCheck.new.check_plan!(plan, workers)
      results.each { |row| print_worker_result(row) }
      0
    end

    def run_command(resume:, detached: false)
      plan_path = required_argument!("PLAN.json")
      options = parse_runtime_options(require_output: true, allow_acknowledge: resume || detached, detached: detached)
      resume ||= options.fetch(:resume, false)
      raise Error, "breaker acknowledgement requires --resume" if options[:acknowledge] && !resume

      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      plan.execution_profile&.ensure_runnable!
      workers = load_workers_for_plan(options, plan)
      worker_source = DynamicWorkerCLI.source_for(plan, options)
      runner_options = {
        plan: plan,
        workers: workers,
        workdir: require_workdir(options),
        output_dir: options.fetch(:output),
        out: detached ? $stdout : @out,
        worker_source: worker_source,
        worker_poll_interval: @worker_poll_interval
      }
      runner = Runner.new(**runner_options)
      return start_manager(runner, resume, options) if detached

      status = runner.run(resume: resume, acknowledge_circuit_breaker: options.fetch(:acknowledge, false))
      %w[completed paused].include?(status) ? 0 : 2
    end

    def start_manager(runner, resume, options)
      record = DetachedManager.new(runner).start(
        resume: resume, acknowledge_circuit_breaker: options.fetch(:acknowledge, false)
      )
      @out.puts "Detached manager started: PID #{record.fetch('pid')}"
      @out.puts "Manager log: #{record.fetch('log_path')}"
      @out.puts "The manager continues after this CLI or terminal exits; Ctrl-C here is not a workload pause."
      @out.puts "WLO never tears down paid provider capacity; use the applicable RPOF teardown command."
      @out.puts "Use summary to check readiness, progress and the final result."
      0
    end

    def status_command(human: false)
      plan_path = required_argument!("PLAN.json")
      output = nil
      OptionParser.new do |opts|
        opts.on("--output DIR") { |value| output = value }
        opts.on("--human") { human = true }
        opts.on("--json") { human = false }
      end.parse!(@argv)
      reject_extra_arguments!
      raise OptionParser::MissingArgument, "--output DIR" if output.to_s.empty?

      report = ExecutionReport.new(plan: load_plan(plan_path), output: output)
      human ? report.print(@out) : @out.puts(JSON.pretty_generate(report.document))
      0
    end

    def watch_command
      plan_path = required_argument!("PLAN.json")
      output = nil
      interval = ExecutionWatch::DEFAULT_INTERVAL_SECONDS
      OptionParser.new do |opts|
        opts.on("--output DIR") { |value| output = value }
        opts.on("--interval SECONDS", Float) { |value| interval = value }
      end.parse!(@argv)
      reject_extra_arguments!
      raise OptionParser::MissingArgument, "--output DIR" if output.to_s.empty?

      ExecutionWatch.new(
        plan: load_plan(plan_path), output: output, out: @out, interval_seconds: interval
      ).run
    end

    def retry_failed_command
      plan_path = required_argument!("PLAN.json")
      options = parse_retry_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      workers_sha256 = nil
      if plan.logical?
        workers = load_workers_for_plan(options, plan)
        workers.validate_plan!(plan)
        workers_sha256 = workers.execution_sha256(plan)
      end
      store = ExecutionStore.new(
        plan: plan, workdir: require_workdir(options), output_dir: options.fetch(:output),
        workers_sha256: workers_sha256
      )
      selected = store.retry_failed!(
        all: options.fetch(:all), job_ids: options.fetch(:jobs), reason: options.fetch(:reason),
        acknowledge_circuit_breaker: options.fetch(:acknowledge)
      )
      @out.puts "Retry queued: #{selected.join(', ')}"
      @out.puts "Execution remains paused. Resume the same plan, workdir and output to run pending jobs."
      0
    end

    def import_terminal_command
      plan_path = required_argument!("PLAN.json")
      handoff_path = required_argument!("HANDOFF.json")
      options = parse_import_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      workers_sha256 = terminal_import_workers_sha256(options, plan)
      store = ExecutionStore.new(
        plan: plan, workdir: require_workdir(options), output_dir: options.fetch(:output),
        workers_sha256: workers_sha256
      )
      count = store.import_terminal!(bytes: File.binread(File.expand_path(handoff_path)))
      @out.puts "Imported #{count} terminal jobs into #{store.output_dir}; no commands executed."
      0
    rescue SystemCallError => e
      raise Error, "cannot read terminal handoff: #{e.message}"
    end

    def parse_import_options
      options = {}
      OptionParser.new do |opts|
        opts.on("--workers-config FILE") { |value| options[:workers_config] = value }
        opts.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
        opts.on("--workdir DIR") { |value| options[:workdir] = value }
        opts.on("--output DIR") { |value| options[:output] = value }
      end.parse!(@argv)
      %i[workdir output].each do |key|
        raise OptionParser::MissingArgument, "--#{key}" if options[key].to_s.empty?
      end
      options
    end

    def parse_retry_options
      options = { all: false, jobs: [], reason: nil, acknowledge: false }
      OptionParser.new do |opts|
        opts.on("--workers-config FILE") { |value| options[:workers_config] = value }
        opts.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
        opts.on("--workdir DIR") { |value| options[:workdir] = value }
        opts.on("--output DIR") { |value| options[:output] = value }
        opts.on("--all") { options[:all] = true }
        opts.on("--job ID") { |value| options[:jobs] << value }
        opts.on("--reason TEXT") { |value| options[:reason] = value }
        opts.on("--acknowledge-circuit-breaker") { options[:acknowledge] = true }
      end.parse!(@argv)
      %i[workdir output reason].each do |key|
        raise OptionParser::MissingArgument, "--#{key}" if options[key].to_s.strip.empty?
      end
      options
    end

    def pause_command
      output = nil
      OptionParser.new { |opts| opts.on("--output DIR") { |value| output = value } }.parse!(@argv)
      reject_extra_arguments!
      raise OptionParser::MissingArgument, "--output DIR" if output.to_s.empty?

      pause_existing_output(output)
      @out.puts "Pause requested: #{File.expand_path(output)}"
      0
    end

    def parse_runtime_options(require_output:, allow_acknowledge: false, detached: false)
      options = {
        workdir: nil, output: nil, workers_config: nil, acknowledge: false
      }
      parser = OptionParser.new do |opts|
        add_runtime_options(opts, options, detached: detached, allow_acknowledge: allow_acknowledge)
      end
      parser.parse!(@argv)
      raise OptionParser::MissingArgument, "--workdir DIR" if options[:workdir].to_s.empty?
      raise OptionParser::MissingArgument, "--output DIR" if require_output && options[:output].to_s.empty?

      options
    end

    def add_runtime_options(parser, options, detached:, allow_acknowledge:)
      parser.on("--resume") { options[:resume] = true } if detached
      parser.on("--workdir DIR") { |value| options[:workdir] = value }
      parser.on("--output DIR") { |value| options[:output] = value }
      parser.on("--workers-config FILE") { |value| options[:workers_config] = value }
      parser.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
      DynamicWorkerCLI.add_options(parser, options)
      return unless allow_acknowledge

      parser.on("--acknowledge-circuit-breaker") { options[:acknowledge] = true }
    end

    def parse_worker_options
      options = { workers_config: nil }
      OptionParser.new do |opts|
        opts.on("--workers-config FILE") { |value| options[:workers_config] = value }
        opts.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
      end.parse!(@argv)
      options
    end

    def parse_profile_options
      options = {}
      OptionParser.new do |opts|
        opts.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
      end.parse!(@argv)
      options
    end

    def load_bound_plan(path, options)
      plan = load_plan(path)
      profile = options[:execution_profile]
      profile ? ExecutionProfile.load(profile).bind(plan) : plan
    end

    def load_workers(options)
      path = options[:workers_config].to_s
      path = ENV.fetch("WLO_WORKERS_CONFIG", "").to_s if path.empty?
      path = File.join(@root, "config", "workers.yml") if path.empty?
      WorkerSet.load(path)
    end

    def load_workers_for_plan(options, plan)
      return WorkerSet.new({}) if DynamicWorkerCLI.unbound_plan?(plan)

      fixed = plan.pools.any? do |pool|
        plan.execution_profile&.binding_for(pool.id)&.fetch("backend") != "rpof"
      end
      fixed ? load_workers(options) : WorkerSet.new({})
    end

    def terminal_import_workers_sha256(options, plan)
      # An unbound v0.3 plan intentionally has no placement or static worker
      # identity yet. Importing terminal evidence does not dispatch work, so its
      # execution identity binds null profile/worker digests until the remaining
      # pending jobs later use the dynamic registry.
      return nil if plan.priority_scheduling? && !plan.execution_profile

      workers = load_workers_for_plan(options, plan)
      workers.validate_plan!(plan)
      plan.logical? ? workers.execution_sha256(plan) : nil
    end

    def print_worker_result(row)
      version = row.version ? " ollama=#{row.version}" : ""
      digest = row.model_digest ? " digest=#{row.model_digest}" : ""
      @out.puts "#{row.pool_id}/#{row.worker_name}: PASS#{version}#{digest}"
    end

    def pause_existing_output(output)
      root = File.expand_path(output)
      state = File.join(root, "execution.json")
      raise Error, "execution state not found in #{root}" unless File.file?(state)

      control = File.join(root, "control")
      FileUtils.mkdir_p(control)
      File.write(File.join(control, "pause"), "requested\n")
    end

    def load_plan(path)
      Plan.load(path)
    end

    def require_workdir(options)
      path = File.expand_path(options.fetch(:workdir))
      raise Error, "workdir is not a directory: #{path}" unless File.directory?(path)

      path
    end

    def required_argument!(label)
      @argv.shift || raise(OptionParser::MissingArgument, label)
    end

    def reject_extra_arguments!
      return if @argv.empty?

      raise OptionParser::InvalidArgument, "unexpected arguments: #{@argv.join(' ')}"
    end

    def version
      @out.puts VERSION
      0
    end

    def help
      @out.puts <<~HELP
        workload-orchestrator #{VERSION}

        Usage:
          bin/wlo validate PLAN.json [--execution-profile FILE]
          bin/wlo plan PLAN.json --workdir DIR [--workers-config FILE]
          bin/wlo worker-check PLAN.json [--workers-config FILE] [--execution-profile FILE]
          bin/wlo run PLAN.json --workdir DIR --output DIR [--workers-config FILE]
                      [--execution-profile FILE] [--worker-source-command FILE] [--worker-source-arg ARG ...]
          bin/wlo start PLAN.json --workdir DIR --output DIR [--workers-config FILE] [--resume]
                        [--acknowledge-circuit-breaker] [--execution-profile FILE]
                        [--worker-source-command FILE [--worker-source-arg ARG ...]]
          bin/wlo status PLAN.json --output DIR [--human | --json]
          bin/wlo summary PLAN.json --output DIR [--json]
          bin/wlo watch PLAN.json --output DIR [--interval SECONDS]
          bin/wlo pause --output DIR
          bin/wlo resume PLAN.json --workdir DIR --output DIR [--workers-config FILE] [--acknowledge-circuit-breaker]
                         [--execution-profile FILE]
                         [--worker-source-command FILE [--worker-source-arg ARG ...]]
          bin/wlo retry-failed PLAN.json --workdir DIR --output DIR (--all | --job ID ...) --reason TEXT
                               [--acknowledge-circuit-breaker]
          bin/wlo import-terminal PLAN.json HANDOFF.json --workdir DIR --output DIR
                                  [--workers-config FILE] [--execution-profile FILE]
          bin/wlo --version

        Logical v0.2 plans require --execution-profile FILE for plan, worker-check,
        run, start, resume and retry-failed. Retry also accepts --workers-config FILE.
        Provider capacity lifecycle is external to the v0.3 dynamic runtime.
        Dynamic v0.3 jobs run locally against the selected worker endpoint.
        Historical RPOF profiles can be inspected but cannot be executed.

        #{PROCESS_OWNERSHIP_HELP}
      HELP
      0
    end
  end
end
