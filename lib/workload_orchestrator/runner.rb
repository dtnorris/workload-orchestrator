# frozen_string_literal: true

require "open3"
require "time"
require_relative "worker_registry_poller"
require_relative "worker_registry_set"
require_relative "dynamic_scheduler"

module WorkloadOrchestrator
  class Runner
    PLACEMENT_INTERFACE_VERSION = "wlo-attempt-placement/v0.1"
    WORKER_ENDPOINT_ENV = "WLO_WORKER_ENDPOINT"

    attr_reader :plan, :workers, :workdir, :store, :worker_registry_poller,
                :worker_loss_reconciler, :dynamic_scheduler, :interrupt_signal

    def initialize(plan:, workers:, workdir:, output_dir:, worker_check: WorkerCheck.new, out: $stdout,
                   command_executor: nil, worker_source: nil, foreground: true,
                   worker_poll_interval: WorkerRegistryPoller::DEFAULT_INTERVAL_SECONDS,
                   worker_registry_clock: -> { Time.now.utc }, worker_registry_sleeper: nil)
      plan.execution_profile&.ensure_runnable!
      @plan = plan
      @workers = workers
      @workdir = File.expand_path(workdir)
      @worker_sources = worker_source && WorkerSourceSet.coerce(worker_source)
      @store = ExecutionStore.new(
        output_dir: output_dir, plan: plan, workdir: @workdir,
        workers_sha256: plan.execution_profile && !dynamic_workers? ? workers.execution_sha256(plan) : nil
      )
      @worker_check = worker_check
      @out = out
      @command_executor = command_executor || OwnedCommandRunner.new
      @foreground = foreground
      @worker_poll_interval = worker_poll_interval
      @worker_registry_clock = worker_registry_clock
      @worker_registry_sleeper = worker_registry_sleeper
      @dynamic_schedule_mutex = Mutex.new
      @dynamic_wait_mutex = Mutex.new
      @dynamic_wait_condition = ConditionVariable.new
      @dynamic_activity_pending = false
      @dynamic_threads = []
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
        reconcile_running_attempts!
        yield if block_given?
        run_locked(resume, acknowledge_circuit_breaker)
      ensure
        wait_for_owned_cancellation
        restore_interrupt_handlers
      end
    end

    private

    def run_locked(resume, acknowledge_circuit_breaker)
      prepare_resume!(resume, acknowledge_circuit_breaker)
      if !@interrupt_signal && store.counts["interrupted"].positive?
        raise Error, "interrupted attempts require explicit retry authorization before resume"
      end
      if dynamic_workers?
        @live_display = LiveExecutionDisplay.new(plan: plan, output: store.output_dir, out: @out)
        @live_display.refresh(resume: resume)
      end
      return finalize_and_report if store.paused?

      raise Error, "circuit breaker is tripped; resume with explicit acknowledgement" if store.circuit_tripped?

      if store.dispatch_halted?
        if store.dispatch_halt.fetch("kind") == "worker_registry"
          raise Error, "worker registry polling is halted after a fatal validation error"
        end

        raise Error, "dispatch is halted; explicitly retry the failed/in-doubt attempt after reviewing evidence"
      end

      return run_with_dynamic_workers if dynamic_workers?

      check_workers!
      counts = store.counts
      return finalize_and_report if (counts["pending"] + counts["running"]).zero?

      store.start!
      schedule_jobs
      finalize_and_report
    end

    def run_with_dynamic_workers
      counts = store.counts
      return finalize_and_report if (counts["pending"] + counts["running"]).zero?

      store.start!
      @live_display.refresh
      @worker_registry_poller ||= WorkerRegistrySet.new(
        sources: @worker_sources,
        checkpoint_root: File.join(store.output_dir, "dynamic-workers"),
        clock: @worker_registry_clock,
        interval_seconds: @worker_poll_interval,
        sleeper: @worker_registry_sleeper || method(:dynamic_poll_sleep)
      )
      @worker_loss_reconciler ||= DynamicWorkerLossReconciler.new(store: store)
      worker_loss_reconciler.reconcile!(worker_registry_poller)
      @dynamic_scheduler ||= DynamicScheduler.new(plan: plan, store: store) if plan.priority_scheduling?
      worker_registry_poller.run(stop: method(:stop_dynamic_polling?)) do |poller|
        store.consumer_heartbeat!
        worker_loss_reconciler.reconcile!(poller)
        @live_display.accept_workers(poller.current_workers)
        if dynamic_scheduler && !poller.dispatch_blocked?
          schedule_dynamic_assignments(
            poller.ready_workers, current_workers: poller.current_workers
          )
        end
        @dynamic_schedule_mutex.synchronize { @live_display.refresh }
      end
      join_dynamic_threads
      finalize_and_report
    rescue CommandCancelled
      join_dynamic_threads
      store.record_interruption!(@interrupt_signal || "INT")
      @live_display.refresh
      finalize_and_report
    rescue Error => e
      join_dynamic_threads
      if @interrupt_signal
        store.record_interruption!(@interrupt_signal)
      else
        store.record_dispatch_halt!(kind: "worker_registry", error: e.message) unless store.dispatch_halted?
      end
      @live_display.refresh
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
            job: assignment.job, worker: assignment.worker,
            environment_keys: assignment.job.env.keys | [WORKER_ENDPOINT_ENV],
            placement: {
              "contract_version" => PLACEMENT_INTERFACE_VERSION,
              "endpoint_environment_variable" => WORKER_ENDPOINT_ENV
            }
          )
          launch_dynamic_assignment(assignment, attempt)
        end
      end
    end

    def launch_dynamic_assignment(assignment, attempt)
      job = assignment.job
      @live_display.refresh
      @dynamic_threads << Thread.new do
        JobClaim.new(output_dir: store.output_dir).synchronize(job.id) do
          execute_dynamic_command(job, attempt)
        end
      end
    end

    def execute_dynamic_command(job, attempt)
      # Consume the identity persisted before launch, never a fresh registry view.
      environment = job.env.merge(
        WORKER_ENDPOINT_ENV => attempt.worker_binding.execution_identity.fetch("endpoint")
      )
      stdout, stderr, status = @command_executor.call(environment, *job.argv, chdir: workdir)
      record_dynamic_result(job, attempt, stdout, stderr, status)
    rescue CommandCancelled => e
      @dynamic_schedule_mutex.synchronize do
        store.write_logs(job, e.stdout, e.stderr)
        store.record_dynamic_terminal!(
          attempt: attempt, status: "interrupted", exit_status: e.status&.exitstatus,
          error: e.message, evidence: cancellation_evidence(e)
        )
        @live_display.refresh
      end
    rescue StandardError => e
      @dynamic_schedule_mutex.synchronize do
        store.write_logs(job, "", "#{e.class}: #{e.message}\n")
        store.record_dynamic_terminal!(
          attempt: attempt, status: "failed", exit_status: nil, error: e.message
        )
        @live_display.refresh
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
        @live_display.refresh
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
      @worker_check.check_plan!(plan, workers)
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
      threads = selected_workers(pool).map { |worker| worker_thread(worker, queue) }
      threads.each(&:value)
    end

    def selected_workers(pool)
      pool.worker_names.first(pool.max_concurrency).map { |name| workers.fetch(name) }
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
      environment = merged_environment(worker, job)
      started_at = store.record_running!(job: job, worker: worker, environment_keys: environment.keys)
      @out.puts "[#{@job_positions.fetch(job.id)}/#{plan.jobs.length}] [#{worker.name}] #{job.id}"
      execute_command(job, environment, started_at)
    end

    def execute_command(job, environment, started_at)
      stdout, stderr, status = @command_executor.call(environment, *job.argv, chdir: workdir)
      store.write_logs(job, stdout, stderr)
      terminal = status.success? ? "complete" : "failed"
      store.record_terminal!(job: job, status: terminal, started_at: started_at, exit_status: status.exitstatus)
    rescue CommandCancelled => e
      store.write_logs(job, e.stdout, e.stderr)
      store.record_interrupted!(
        job: job, started_at: started_at, exit_status: e.status&.exitstatus,
        term_signal: e.status&.termsig, evidence: cancellation_evidence(e)
      )
    rescue StandardError => e
      store.write_logs(job, "", "#{e.class}: #{e.message}\n")
      store.record_terminal!(job: job, status: "failed", started_at: started_at, exit_status: nil, error: e.message)
    end

    def merged_environment(worker, job)
      worker.job_env.merge(job.env)
    end

    def stop_dispatch?
      @interrupt_signal || store.paused? || store.circuit_tripped? || store.dispatch_halted?
    end

    def stop_dynamic_polling?
      return true if stop_dispatch?

      counts = store.counts
      (counts["pending"] + counts["running"]).zero?
    end

    def install_interrupt_handlers
      return unless @foreground && Thread.current == Thread.main

      @interrupt_reader, @interrupt_writer = IO.pipe
      @previous_handlers = {
        "INT" => Signal.trap("INT") { receive_interrupt("INT") },
        "TERM" => Signal.trap("TERM") { receive_interrupt("TERM") }
      }
      @interrupt_supervisor = Thread.new { supervise_interrupts }
    end

    def restore_interrupt_handlers
      @previous_handlers&.each { |signal, previous| Signal.trap(signal, previous) }
      @interrupt_writer&.close unless @interrupt_writer&.closed?
      @interrupt_supervisor&.join
      @interrupt_reader&.close unless @interrupt_reader&.closed?
    end

    def dynamic_workers?
      !@worker_sources.nil?
    end

    def reconcile_running_attempts!; end

    def finalize_and_report
      wait_for_owned_cancellation
      store.record_interruption!(@interrupt_signal) if @interrupt_signal
      status = store.finish!(resource_cleanup_pending: false)
      @live_display&.refresh
      counts = store.counts
      @out.puts "Execution: #{status}"
      @out.puts "Jobs: #{format_counts(counts)}"
      status
    end

    def format_counts(counts)
      keys = %w[complete failed running pending]
      keys << "interrupted" if counts["interrupted"].positive?
      keys.map { |key| "#{key}=#{counts[key]}" }.join(" ")
    end

    def receive_interrupt(signal)
      @interrupt_signal ||= signal
      @interrupt_writer.write_nonblock(".")
    rescue IO::WaitWritable, IOError, Errno::EPIPE
      nil
    end

    def supervise_interrupts
      handled = 0
      while @interrupt_reader.read(1)
        handled += 1
        force = handled > 1
        count = cancellation_targets.sum do |target|
          target.cancel(signal: @interrupt_signal || "INT", force: force)
        end
        if force
          @out.puts "Second Ctrl-C received; force-killing #{count} owned job process(es)."
        else
          @out.puts "Cancellation requested; stopping #{count} owned job process(es)."
        end
        signal_dynamic_activity
      end
    rescue IOError
      nil
    end

    def wait_for_owned_cancellation
      return unless @interrupt_signal

      cancellation_targets.each(&:wait_for_cancellation)
    end

    def cancellation_targets
      [@command_executor, @worker_sources].compact.select do |target|
        target.respond_to?(:cancel) && target.respond_to?(:wait_for_cancellation)
      end.uniq
    end

    def cancellation_evidence(error)
      {
        "kind" => "foreground_cancellation",
        "signal" => error.signal,
        "requested_at" => error.requested_at,
        "termination_mode" => error.termination_mode,
        "exit_status" => error.status&.exitstatus,
        "term_signal" => error.status&.termsig,
        "pid" => error.pid,
        "process_group_id" => error.process_group_id
      }
    end

    def validate_workdir!
      raise Error, "workdir is not a directory: #{workdir}" unless File.directory?(workdir)
    end
  end
end
