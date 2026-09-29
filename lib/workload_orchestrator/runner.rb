# frozen_string_literal: true

require "open3"
require "time"
require_relative "pool_fulfillment"
require_relative "rpof_contract"
require_relative "worker_admission_policy"
require_relative "worker_registry_poller"
require_relative "dynamic_scheduler"

module WorkloadOrchestrator
  class Runner
    attr_reader :plan, :workers, :workdir, :store, :worker_registry_poller,
                :worker_loss_reconciler, :dynamic_scheduler

    RemoteWorker = Struct.new(:name, :index, keyword_init: true)

    def initialize(plan:, workers:, workdir:, output_dir:, worker_check: WorkerCheck.new, out: $stdout,
                   rpof_client: nil, capacity_session: nil, admission_policy: WorkerAdmissionPolicy.new,
                   command_executor: Open3.method(:capture3), worker_source: nil,
                   worker_poll_interval: WorkerRegistryPoller::DEFAULT_INTERVAL_SECONDS,
                   worker_registry_clock: -> { Time.now.utc }, worker_registry_sleeper: nil)
      @plan = plan
      @workers = workers
      @workdir = File.expand_path(workdir)
      @worker_source = worker_source
      @store = ExecutionStore.new(
        output_dir: output_dir, plan: plan, workdir: @workdir,
        workers_sha256: plan.execution_profile && !dynamic_workers? ? workers.execution_sha256(plan) : nil
      )
      @worker_check = worker_check
      @out = out
      @rpof_client = rpof_client
      @capacity_session = capacity_session
      @admission_policy = admission_policy
      @command_executor = command_executor
      @worker_poll_interval = worker_poll_interval
      @worker_registry_clock = worker_registry_clock
      @worker_registry_sleeper = worker_registry_sleeper
      @dynamic_schedule_mutex = Mutex.new
      @dynamic_wait_mutex = Mutex.new
      @dynamic_wait_condition = ConditionVariable.new
      @dynamic_activity_pending = false
      @dynamic_threads = []
      @remote_halt_mutex = Mutex.new
      @remote_halted = false
      @job_positions = plan.jobs.each_with_index.to_h { |job, index| [job.id, index + 1] }.freeze
    end

    def run(resume: false, acknowledge_circuit_breaker: false)
      if plan.priority_scheduling? && !dynamic_workers?
        raise Error, "wlo-execution-plan/v0.3 work-conserving execution requires a dynamic worker source"
      end

      validate_workdir!
      workers.validate_plan!(plan) unless dynamic_workers?
      store.with_execution_lock do
        store.prepare!
        store.restore_dynamic_worker_loss_halt! if dynamic_workers?
        install_interrupt_handlers
        store.reconcile_remote_running! if rpof? && !dynamic_workers?
        yield if block_given?
        run_locked(resume, acknowledge_circuit_breaker)
      ensure
        restore_interrupt_handlers
      end
    end

    private

    def run_locked(resume, acknowledge_circuit_breaker)
      prepare_resume!(resume, acknowledge_circuit_breaker)
      return finalize_and_report if store.paused?

      raise Error, "circuit breaker is tripped; resume with explicit acknowledgement" if store.circuit_tripped?

      if store.dispatch_halted?
        if store.dispatch_halt.fetch("kind") == "worker_registry"
          raise Error, "worker registry polling is halted after a fatal validation error"
        end

        raise Error, "remote dispatch is halted; explicitly retry the failed/in-doubt attempt after reviewing evidence"
      end

      return run_with_dynamic_workers if dynamic_workers?

      check_workers!
      validate_remote_jobs!
      counts = store.counts
      return finalize_and_report(resource_cleanup_pending: false) if (counts["pending"] + counts["running"]).zero?
      return run_with_capacity(resume) if rpof?

      store.start!
      schedule_jobs
      finalize_and_report
    end

    def run_with_dynamic_workers
      counts = store.counts
      return finalize_and_report(resource_cleanup_pending: false) if (counts["pending"] + counts["running"]).zero?

      store.start!
      @worker_registry_poller ||= WorkerRegistryPoller.new(
        source: @worker_source,
        checkpoint_path: File.join(store.output_dir, "dynamic-workers", "checkpoint.json"),
        clock: @worker_registry_clock,
        interval_seconds: @worker_poll_interval,
        sleeper: @worker_registry_sleeper || method(:dynamic_poll_sleep)
      )
      @worker_loss_reconciler ||= DynamicWorkerLossReconciler.new(store: store)
      worker_loss_reconciler.reconcile!(worker_registry_poller)
      @dynamic_scheduler ||= DynamicScheduler.new(plan: plan, store: store) if plan.priority_scheduling?
      worker_registry_poller.run(stop: method(:stop_dynamic_polling?)) do |poller|
        worker_loss_reconciler.reconcile!(poller)
        if dynamic_scheduler
          schedule_dynamic_assignments(
            poller.ready_workers, current_workers: poller.current_workers
          )
        end
      end
      join_dynamic_threads
      store.record_interruption!(@interrupt_signal) if @interrupt_signal
      finalize_and_report(resource_cleanup_pending: false)
    rescue Error => e
      join_dynamic_threads
      if @interrupt_signal
        store.record_interruption!(@interrupt_signal)
      else
        store.record_dispatch_halt!(kind: "worker_registry", error: e.message) unless store.dispatch_halted?
      end
      raise
    ensure
      @dynamic_threads.clear
    end

    def schedule_dynamic_assignments(workers, current_workers: workers)
      return if stop_dispatch?

      @dynamic_schedule_mutex.synchronize do
        return if stop_dispatch?

        dynamic_scheduler.assignments(workers: workers, current_workers: current_workers).each do |assignment|
          break if stop_dispatch?

          attempt = store.record_dynamic_running!(
            job: assignment.job, worker: assignment.worker, environment_keys: assignment.job.env.keys
          )
          launch_dynamic_assignment(assignment, attempt)
        end
      end
    end

    def launch_dynamic_assignment(assignment, attempt)
      job = assignment.job
      worker = assignment.worker
      @out.puts "[#{@job_positions.fetch(job.id)}/#{plan.jobs.length}] [#{worker.worker_id}] #{job.id}"
      @dynamic_threads << Thread.new do
        JobClaim.new(output_dir: store.output_dir).synchronize(job.id) do
          execute_dynamic_command(job, attempt)
        end
      end
    end

    def execute_dynamic_command(job, attempt)
      stdout, stderr, status = @command_executor.call(job.env, *job.argv, chdir: workdir)
      record_dynamic_result(job, attempt, stdout, stderr, status)
    rescue StandardError => e
      @dynamic_schedule_mutex.synchronize do
        store.write_logs(job, "", "#{e.class}: #{e.message}\n")
        store.record_dynamic_terminal!(
          attempt: attempt, status: "failed", exit_status: nil, error: e.message
        )
      end
    ensure
      signal_dynamic_activity
    end

    def record_dynamic_result(job, attempt, stdout, stderr, status)
      @dynamic_schedule_mutex.synchronize do
        store.write_logs(job, stdout, stderr)
        terminal = status.success? ? "complete" : "failed"
        store.record_dynamic_terminal!(
          attempt: attempt, status: terminal, exit_status: status.exitstatus
        )
      end
    end

    def join_dynamic_threads
      @dynamic_threads.each(&:value)
    end

    def signal_dynamic_activity
      @dynamic_wait_mutex.synchronize do
        @dynamic_activity_pending = true
        @dynamic_wait_condition.broadcast
      end
    end

    def dynamic_poll_sleep(seconds, stop)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
      @dynamic_wait_mutex.synchronize do
        if @dynamic_activity_pending
          @dynamic_activity_pending = false
          return
        end

        until stop.call
          remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          break unless remaining.positive?

          @dynamic_wait_condition.wait(@dynamic_wait_mutex, [remaining, 0.1].min)
          next unless @dynamic_activity_pending

          @dynamic_activity_pending = false
          break
        end
      end
    end

    def run_with_capacity(resume)
      unless @rpof_client && @capacity_session
        raise Error, "RPOF execution requires the explicit capacity session and RPOF client"
      end

      status = @capacity_session.with_capacity(authorize_paid: true, resume: resume) do |handoffs, lifecycle|
        @remote_handoffs = handoffs
        @budget_lifecycle = lifecycle
        store.start!
        schedule_jobs
        store.record_interruption!(@interrupt_signal) if @interrupt_signal
        value = finalize_and_report
        PoolFulfillment::Outcome.new(value: value, retain_capacity: value == "paused")
      end
      store.record_resource_disposition!(capacity_disposition) if File.file?(capacity_session_path)
      status
    rescue Error => e
      if @interrupt_signal
        store.record_interruption!(@interrupt_signal)
      else
        store.record_dispatch_halt!(kind: "capacity", error: e.message) unless store.dispatch_halted?
      end
      store.record_resource_disposition!(capacity_disposition) if File.file?(capacity_session_path)
      if store.workload_status
        @out.puts "Execution: #{store.status}"
        store.status
      else
        finalize_and_report
      end
    ensure
      @remote_handoffs = @budget_lifecycle = nil
    end

    def schedule_jobs
      if plan.grouped_jobs?
        run_job_groups
      else
        run_pools
      end
    end

    def prepare_resume!(resume, acknowledge)
      return unless resume

      store.clear_pause!
      return unless acknowledge

      store.acknowledge_circuit_breaker!
    end

    def check_workers!
      if rpof?
        @worker_check.check_fixed_pools!(plan, workers)
      else
        @worker_check.check_plan!(plan, workers)
      end
    end

    def run_pools
      plan.pools.each do |pool|
        run_pool(pool, jobs: plan.jobs)
        break if stop_dispatch?
      end
    end

    def run_job_groups
      plan.job_groups.each do |jobs|
        @out.puts "Group: #{jobs.first.group_id}"
        plan.pools.each do |pool|
          run_pool(pool, jobs: jobs)
          break if stop_dispatch?
        end
        break if stop_dispatch?
      end
    end

    def run_pool(pool, jobs:)
      pending = jobs.select { |job| job.pool_id == pool.id && !store.terminal?(job) }
      return if pending.empty?

      queue = Queue.new
      pending.each { |job| queue << job }
      expansion_disabled = rpof_pool?(pool) && expansion_disabled?(pool.id)
      threads = selected_workers(pool).map { |worker| worker_thread(worker, queue) }
      if rpof_pool?(pool)
        coordinate_admissions(pool, jobs, queue, threads, expansion_disabled: expansion_disabled)
      else
        threads.each(&:value)
      end
    end

    def coordinate_admissions(pool, jobs, queue, threads, expansion_disabled:)
      handoff = @remote_handoffs.fetch(pool.id)
      samples = handoff.fetch("bootstrap_samples_seconds", [])
      observed = 0
      until threads.all? { |thread| !thread.alive? }
        break if stop_dispatch?

        completions = jobs.filter_map do |job|
          next unless job.pool_id == pool.id
          metadata = store.metadata_for(job)
          metadata["elapsed_seconds"] if metadata && metadata["status"] == "complete" &&
                                         metadata["elapsed_seconds"].to_f.positive?
        end
        if !expansion_disabled && completions.length > observed && queue.size.positive?
          observed = completions.length
          deadline = Time.iso8601(handoff.fetch("deadline_at_utc")) - Time.now.utc
          decision = @admission_policy.evaluate(
            unclaimed: queue.size, workers: threads.length,
            ceiling: [pool.max_concurrency, plan.execution_profile.binding_for(pool.id).fetch("desired_workers")].min,
            job_seconds: completions.sum / completions.length.to_f,
            bootstrap_seconds: samples.empty? ? nil : samples.sum / samples.length,
            deadline_seconds: deadline
          )
          record_admission_decision(pool.id, decision)
          if decision.fetch("expand") && !stop_dispatch?
            # A failed guardian/budget check still halts execution; optional
            # admission errors only disable acceleration for this pool.
            check_admission_budget!
            begin
              admitted = @capacity_session.admit_worker(
                pool_id: pool.id, handoff: handoff, lifecycle: @budget_lifecycle
              )
            rescue StandardError => e
              record_admission_decision(pool.id, { "expand" => false, "reason" => "admission_failed",
                                                    "error" => e.message })
              expansion_disabled = true
              check_admission_budget!
            else
              handoff = admitted
              @remote_handoffs[pool.id] = admitted
              samples = admitted.fetch("bootstrap_samples_seconds")
              worker_index = admitted.dig("target", "worker_indices").last
              record_admission_decision(pool.id, { "expand" => true, "reason" => "admitted",
                                                    "worker_index" => worker_index })
              unless stop_dispatch?
                threads << worker_thread(RemoteWorker.new(name: "rpof:#{pool.id}:burst_#{worker_index}",
                                                          index: worker_index).freeze, queue)
              end
            end
          end
        end
        sleep(0.1) if threads.any?(&:alive?)
      end
      record_admission_decision(pool.id, { "expand" => false, "reason" => "no_unclaimed_work" }) if
        !stop_dispatch? && queue.empty?
    ensure
      threads.each(&:value)
    end

    def expansion_disabled?(pool_id)
      path = File.join(store.output_dir, "worker-admissions.jsonl")
      return false unless File.file?(path)

      disabled = false
      File.foreach(path) do |line|
        row = JSON.parse(line)
        unless row["plan_sha256"] == plan.sha256 &&
               row["profile_sha256"] == plan.execution_profile.sha256
          raise Error, "worker admission evidence differs from the original execution"
        end
        disabled = true if row["pool_id"] == pool_id && row.dig("decision", "reason") == "admission_failed"
      end
      disabled
    rescue JSON::ParserError, SystemCallError => e
      raise Error, "invalid worker admission evidence: #{e.message}"
    end

    def check_admission_budget!
      @budget_lifecycle.check!
    rescue StandardError => e
      halt_remote_dispatch!(nil, "budget", e.message)
      raise
    end

    def record_admission_decision(pool_id, decision)
      path = File.join(store.output_dir, "worker-admissions.jsonl")
      row = { "at_utc" => Time.now.utc.iso8601, "pool_id" => pool_id,
              "plan_sha256" => plan.sha256, "profile_sha256" => plan.execution_profile.sha256,
              "decision" => decision }
      File.open(path, "a", 0o600) do |file|
        file.write(JSON.generate(row) + "\n")
        file.flush
        file.fsync
      end
    end

    def selected_workers(pool)
      return pool.worker_names.first(pool.max_concurrency).map { |name| workers.fetch(name) } unless rpof_pool?(pool)

      handoff = @remote_handoffs.fetch(pool.id)
      handoff.fetch("target").fetch("worker_indices").first(pool.max_concurrency).map do |index|
        RemoteWorker.new(name: "rpof:#{pool.id}:burst_#{index}", index: index).freeze
      end
    end

    def worker_thread(worker, queue)
      Thread.new do
        loop do
          break if stop_dispatch?

          job = pop_job(queue)
          break unless job

          JobClaim.new(output_dir: store.output_dir).synchronize(job.id) do
            execute_job(job, worker) unless store.terminal?(job) || stop_dispatch?
          end
        end
      end
    end

    def pop_job(queue)
      queue.pop(true)
    rescue ThreadError
      nil
    end

    def execute_job(job, worker)
      return execute_remote_job(job, worker) if remote_worker?(worker)

      environment = merged_environment(worker, job)
      started_at = store.record_running!(job: job, worker: worker, environment_keys: environment.keys)
      @out.puts "[#{@job_positions.fetch(job.id)}/#{plan.jobs.length}] [#{worker.name}] #{job.id}"
      execute_command(job, environment, started_at)
    end

    def execute_remote_job(job, worker)
      started_at = store.record_running!(job: job, worker: worker, environment_keys: job.env.keys)
      attempt = store.metadata_for(job).fetch("attempt")
      @out.puts "[#{@job_positions.fetch(job.id)}/#{plan.jobs.length}] [#{worker.name}] #{job.id}"
      output = File.join(store.output_dir, "runs", job.id, "provider-attempt-#{attempt}")
      handoff = @remote_handoffs.fetch(job.pool_id)
      request = remote_request(job, handoff, worker.index)
      begin
        @budget_lifecycle.check!
        remaining = Time.iso8601(handoff.fetch("deadline_at_utc")) - Time.now.utc
        raise Error, "original paid runtime deadline has expired" unless remaining.positive?

        result = @rpof_client.dispatch(
          request: request, workdir: workdir, output_dir: output, timeout_seconds: remaining
        )
        record_remote_result(job, started_at, result, output)
      rescue StandardError => e
        record_remote_transport_failure(job, started_at, e, output)
      end
    end

    def record_remote_result(job, started_at, result, output)
      row = result.document.fetch("jobs").first
      stdout, stderr = remote_logs(result, row, output)
      store.write_logs(job, stdout, stderr)
      if result.document.fetch("status") == "completed"
        store.record_terminal!(job: job, status: "complete", started_at: started_at,
                               exit_status: row.fetch("exit_status"), evidence: remote_evidence(result, output))
      elsif result.document.fetch("status") == "workload_failed"
        store.record_terminal!(job: job, status: "failed", started_at: started_at,
                               exit_status: row["exit_status"], error: row["error"],
                               evidence: remote_evidence(result, output).merge("kind" => "workload"))
      else
        detail = "RPOF dispatch #{result.document.fetch('status')}"
        store.record_terminal!(job: job, status: "failed", started_at: started_at,
                               exit_status: row && row["exit_status"], error: detail,
                               evidence: remote_evidence(result, output).merge("kind" => "infrastructure"),
                               failure_class: "operational")
        halt_remote_dispatch!(job, "remote_infrastructure", detail)
      end
    end

    def record_remote_transport_failure(job, started_at, error, output)
      stderr = "#{error.class}: #{error.message}\n"
      store.write_logs(job, "", stderr)
      store.record_terminal!(job: job, status: "failed", started_at: started_at, exit_status: nil,
                             error: "remote attempt outcome is in doubt: #{error.message}",
                             evidence: { "kind" => "remote_in_doubt", "provider_output" => output })
      halt_remote_dispatch!(job, "remote_in_doubt", error.message)
    end

    def halt_remote_dispatch!(job, kind, error)
      @remote_halt_mutex.synchronize { @remote_halted = true }
      store.record_dispatch_halt!(kind: kind, error: error, job: job)
    end

    def remote_request(job, handoff, worker_index)
      {
        "contract_version" => RpofContract::DISPATCH_REQUEST,
        "target" => handoff.fetch("target").merge("worker_indices" => [worker_index]),
        "group_by_affinity" => false,
        "jobs" => [{ "job_id" => job.id, "argv" => job.argv, "env" => job.env }]
      }
    end

    def remote_logs(result, row, output)
      stdout = result.stdout.to_s
      stderr = result.stderr.to_s
      return [stdout, stderr] unless row

      stdout += read_provider_log(output, row["stdout_path"])
      stderr += read_provider_log(output, row["stderr_path"])
      [stdout, stderr]
    end

    def read_provider_log(root, relative)
      return "" unless relative.is_a?(String) && !relative.empty?

      path = File.expand_path(relative, root)
      prefix = root.end_with?(File::SEPARATOR) ? root : "#{root}#{File::SEPARATOR}"
      raise Error, "provider log path escapes attempt evidence" unless path.start_with?(prefix)

      File.file?(path) ? File.read(path) : ""
    end

    def remote_evidence(result, output)
      {
        "kind" => "remote",
        "provider_output" => output,
        "provider_status" => result.document.fetch("status"),
        "fleet_id" => result.document.fetch("fleet_id"),
        "worker_indices" => result.document["worker_indices"]
      }
    end

    def execute_command(job, environment, started_at)
      stdout, stderr, status = @command_executor.call(environment, *job.argv, chdir: workdir)
      store.write_logs(job, stdout, stderr)
      terminal = status.success? ? "complete" : "failed"
      store.record_terminal!(job: job, status: terminal, started_at: started_at, exit_status: status.exitstatus)
    rescue StandardError => e
      store.write_logs(job, "", "#{e.class}: #{e.message}\n")
      store.record_terminal!(job: job, status: "failed", started_at: started_at, exit_status: nil, error: e.message)
    end

    def merged_environment(worker, job)
      worker.job_env.merge(job.env)
    end

    def stop_dispatch?
      @interrupt_signal || store.paused? || store.circuit_tripped? || store.dispatch_halted? ||
        @remote_halt_mutex.synchronize { @remote_halted }
    end

    def stop_dynamic_polling?
      return true if stop_dispatch?

      counts = store.counts
      (counts["pending"] + counts["running"]).zero?
    end

    def capacity_session_path
      File.join(store.output_dir, "capacity", "session.json")
    end

    def capacity_disposition
      root = File.dirname(capacity_session_path)
      sessions = Dir.glob(File.join(root, "sessions", "session-*.json"))
      latest = (sessions + [capacity_session_path]).select { |path| File.file?(path) }
                     .max_by { |path| path[%r{session-(\d+)\.json\z}, 1]&.to_i || 1 }
      JSON.parse(File.read(latest)).fetch("disposition")
    end

    def install_interrupt_handlers
      return unless (rpof? || dynamic_workers?) && Thread.current == Thread.main

      @previous_handlers = %w[INT TERM].to_h do |signal|
        [signal, Signal.trap(signal) { @interrupt_signal ||= signal }]
      end
    end

    def restore_interrupt_handlers
      @previous_handlers&.each { |signal, previous| Signal.trap(signal, previous) }
    end

    def rpof?
      plan.execution_profile&.rpof? == true
    end

    def dynamic_workers?
      !@worker_source.nil?
    end

    def rpof_pool?(pool)
      plan.execution_profile&.binding_for(pool.id)&.fetch("backend") == "rpof"
    end

    def remote_worker?(worker)
      worker.is_a?(RemoteWorker)
    end

    def validate_remote_jobs!
      return unless rpof?

      reserved = %w[LME_JOB_ID LME_WORKER_INDEX LME_OLLAMA_URL]
      plan.jobs.each do |job|
        next unless rpof_pool?(plan.pool(job.pool_id))

        raise Error, "remote job #{job.id} cannot unset environment values" if job.env.value?(nil)
        conflict = job.env.keys & reserved
        unless conflict.empty?
          raise Error, "remote job #{job.id} cannot override RPOF environment: #{conflict.join(', ')}"
        end
        RpofContract.job!({ "job_id" => job.id, "argv" => job.argv, "env" => job.env })
      end
    end

    def finalize_and_report(resource_cleanup_pending: rpof?)
      status = store.finish!(resource_cleanup_pending: resource_cleanup_pending)
      counts = store.counts
      @out.puts "Execution: #{status}"
      @out.puts "Jobs: #{format_counts(counts)}"
      status
    end

    def format_counts(counts)
      %w[complete failed running pending].map { |key| "#{key}=#{counts[key]}" }.join(" ")
    end

    def validate_workdir!
      raise Error, "workdir is not a directory: #{workdir}" unless File.directory?(workdir)
    end
  end
end
