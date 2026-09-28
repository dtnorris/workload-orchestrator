# frozen_string_literal: true

require "fileutils"
require "json"
require "optparse"

module WorkloadOrchestrator
  class CLI
    DEFAULT_ROOT = File.expand_path("../..", __dir__).freeze

    def initialize(argv, out: $stdout, err: $stderr, root: DEFAULT_ROOT)
      @argv = argv.dup
      @out = out
      @err = err
      @root = File.expand_path(root)
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
      when "run" then run_command(resume: false)
      when "resume" then run_command(resume: true)
      when "retry-failed" then retry_failed_command
      when "status" then status_command
      when "pause" then pause_command
      else raise Error, "unknown command #{command.inspect}; run bin/wlo --help"
      end
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
        @out.puts "Execution: BLOCKED — RPOF safety/fulfillment/dispatch are not implemented"
        require_workdir(options)
        return 0
      end
      workers = load_workers(options)
      workers.validate_plan!(plan)
      workdir = require_workdir(options)
      print_plan(plan, workers, workdir)
      0
    end

    def worker_check_command
      plan_path = required_argument!("PLAN.json")
      options = parse_worker_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      plan.execution_profile&.ensure_runnable!
      workers = load_workers(options)
      results = WorkerCheck.new.check_plan!(plan, workers)
      results.each { |row| print_worker_result(row) }
      0
    end

    def run_command(resume:)
      plan_path = required_argument!("PLAN.json")
      options = parse_runtime_options(require_output: true, allow_acknowledge: resume)
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      plan.execution_profile&.ensure_runnable!
      workers = load_workers(options)
      runner = Runner.new(
        plan: plan,
        workers: workers,
        workdir: require_workdir(options),
        output_dir: options.fetch(:output),
        out: @out
      )
      status = runner.run(resume: resume, acknowledge_circuit_breaker: options.fetch(:acknowledge, false))
      %w[completed paused].include?(status) ? 0 : 2
    end

    def status_command
      plan_path = required_argument!("PLAN.json")
      output = nil
      OptionParser.new { |opts| opts.on("--output DIR") { |value| output = value } }.parse!(@argv)
      reject_extra_arguments!
      raise OptionParser::MissingArgument, "--output DIR" if output.to_s.empty?

      plan = load_plan(plan_path)
      state = load_execution_for_status(plan, output)
      @out.puts JSON.pretty_generate(state)
      0
    end

    def retry_failed_command
      plan_path = required_argument!("PLAN.json")
      options = parse_retry_options
      reject_extra_arguments!
      plan = load_bound_plan(plan_path, options)
      workers_sha256 = nil
      if plan.logical?
        workers = load_workers(options)
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

    def parse_runtime_options(require_output:, allow_acknowledge: false)
      options = { workdir: nil, output: nil, workers_config: nil, acknowledge: false }
      parser = OptionParser.new do |opts|
        opts.on("--workdir DIR") { |value| options[:workdir] = value }
        opts.on("--output DIR") { |value| options[:output] = value }
        opts.on("--workers-config FILE") { |value| options[:workers_config] = value }
        opts.on("--execution-profile FILE") { |value| options[:execution_profile] = value }
        opts.on("--acknowledge-circuit-breaker") { options[:acknowledge] = true } if allow_acknowledge
      end
      parser.parse!(@argv)
      raise OptionParser::MissingArgument, "--workdir DIR" if options[:workdir].to_s.empty?
      raise OptionParser::MissingArgument, "--output DIR" if require_output && options[:output].to_s.empty?

      options
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

    def print_plan(plan, workers, workdir)
      @out.puts "Plan: #{plan.id}"
      @out.puts "SHA-256: #{plan.sha256}"
      @out.puts "Execution profile: #{plan.execution_profile.sha256}" if plan.execution_profile
      @out.puts "Workdir: #{workdir}"
      @out.puts "Pools: #{plan.pools.length}"
      @out.puts "Jobs: #{plan.jobs.length}"
      if plan.grouped_jobs?
        @out.puts "Scheduling: group-major (#{plan.job_groups.length} groups)"
      else
        @out.puts "Scheduling: pool-major (legacy)"
      end
      plan.pools.each { |pool| print_pool(pool, workers) }
      @out.puts "Zero-cost gate: PASS"
    end

    def print_pool(pool, workers)
      requirement = pool.ollama_requirement
      detail = requirement ? " ollama=#{requirement.fetch('model')}" : ""
      names = pool.worker_names.map { |name| workers.fetch(name).name }.join(",")
      @out.puts "  #{pool.id}: workers=#{names} concurrency=#{pool.max_concurrency}#{detail}"
    end

    def print_worker_result(row)
      version = row.version ? " ollama=#{row.version}" : ""
      digest = row.model_digest ? " digest=#{row.model_digest}" : ""
      @out.puts "#{row.pool_id}/#{row.worker_name}: PASS#{version}#{digest}"
    end

    def load_execution_for_status(plan, output)
      path = File.join(File.expand_path(output), "execution.json")
      state = JSON.parse(File.read(path))
      unless state["plan_id"] == plan.id && state["plan_sha256"] == plan.sha256
        raise Error, "output execution identity does not match plan"
      end

      jobs = JSON.parse(File.read(File.join(File.expand_path(output), "jobs.json")))
      state.merge(
        "paused" => File.file?(File.join(File.expand_path(output), "control", "pause")),
        "jobs" => jobs.fetch("jobs")
      )
    rescue Errno::ENOENT, JSON::ParserError, KeyError => e
      raise Error, "cannot read execution status: #{e.message}"
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
          bin/wlo worker-check PLAN.json [--workers-config FILE]
          bin/wlo run PLAN.json --workdir DIR --output DIR [--workers-config FILE]
          bin/wlo status PLAN.json --output DIR
          bin/wlo pause --output DIR
          bin/wlo resume PLAN.json --workdir DIR --output DIR [--workers-config FILE] [--acknowledge-circuit-breaker]
          bin/wlo retry-failed PLAN.json --workdir DIR --output DIR (--all | --job ID ...) --reason TEXT
                               [--acknowledge-circuit-breaker]
          bin/wlo --version

        Logical v0.2 plans require --execution-profile FILE for plan, worker-check,
        run, resume and retry-failed. Retry also accepts --workers-config FILE.
      HELP
      0
    end
  end
end
