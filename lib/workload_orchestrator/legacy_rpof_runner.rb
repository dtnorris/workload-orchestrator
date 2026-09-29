# frozen_string_literal: true

require_relative "runner"
require_relative "pool_fulfillment"
require_relative "rpof_contract"
require_relative "worker_admission_policy"

module WorkloadOrchestrator
  # Compatibility-only v0.2 runtime retained until the final compatibility
  # purge. New v0.3 production execution uses Runner + WorkerSource instead.
  class LegacyRpofRunner < Runner
    def initialize(rpof_client:, capacity_session:, admission_policy: WorkerAdmissionPolicy.new, **options)
      if options.fetch(:plan).priority_scheduling? || options[:worker_source]
        raise Error, "legacy RPOF dispatch cannot execute dynamic attempts"
      end

      super(**options)
      @rpof_client = rpof_client
      @capacity_session = capacity_session
      @admission_policy = admission_policy
      @remote_halt_mutex = Mutex.new
      @remote_halted = false
    end

    private

    def reconcile_running_attempts!
      store.reconcile_remote_running!
    end

    def run_locked(resume, acknowledge_circuit_breaker)
      prepare_resume!(resume, acknowledge_circuit_breaker)
      return finalize_and_report if store.paused?

      raise Error, "circuit breaker is tripped; resume with explicit acknowledgement" if store.circuit_tripped?
      if store.dispatch_halted?
        raise Error, "remote dispatch is halted; explicitly retry the failed/in-doubt attempt after reviewing evidence"
      end

      check_workers!
      validate_remote_jobs!
      counts = store.counts
      return finalize_and_report(resource_cleanup_pending: false) if (counts["pending"] + counts["running"]).zero?

      run_with_capacity(resume)
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

    def check_workers!
      @worker_check.check_fixed_pools!(plan, workers)
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
            check_admission_budget!
            begin
              admitted = @capacity_session.admit_worker(
                pool_id: pool.id, handoff: handoff, lifecycle: @budget_lifecycle
              )
            rescue StandardError => e
              record_admission_decision(
                pool.id, { "expand" => false, "reason" => "admission_failed", "error" => e.message }
              )
              expansion_disabled = true
              check_admission_budget!
            else
              handoff = admitted
              @remote_handoffs[pool.id] = admitted
              samples = admitted.fetch("bootstrap_samples_seconds")
              worker_index = admitted.dig("target", "worker_indices").last
              record_admission_decision(
                pool.id, { "expand" => true, "reason" => "admitted", "worker_index" => worker_index }
              )
              unless stop_dispatch?
                threads << worker_thread(
                  RemoteWorker.new(name: "rpof:#{pool.id}:burst_#{worker_index}", index: worker_index).freeze,
                  queue
                )
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
        unless row["plan_sha256"] == plan.sha256 && row["profile_sha256"] == plan.execution_profile.sha256
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
      row = {
        "at_utc" => Time.now.utc.iso8601, "pool_id" => pool_id,
        "plan_sha256" => plan.sha256, "profile_sha256" => plan.execution_profile.sha256,
        "decision" => decision
      }
      File.open(path, "a", 0o600) do |file|
        file.write(JSON.generate(row) + "\n")
        file.flush
        file.fsync
      end
    end

    def selected_workers(pool)
      unless rpof_pool?(pool)
        return pool.worker_names.first(pool.max_concurrency).map { |name| workers.fetch(name) }
      end

      handoff = @remote_handoffs.fetch(pool.id)
      handoff.fetch("target").fetch("worker_indices").first(pool.max_concurrency).map do |index|
        RemoteWorker.new(name: "rpof:#{pool.id}:burst_#{index}", index: index).freeze
      end
    end

    def execute_job(job, worker)
      return execute_remote_job(job, worker) if worker.is_a?(RemoteWorker)

      super
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
      case result.document.fetch("status")
      when "completed"
        store.record_terminal!(job: job, status: "complete", started_at: started_at,
                               exit_status: row.fetch("exit_status"), evidence: remote_evidence(result, output))
      when "workload_failed"
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
      store.write_logs(job, "", "#{error.class}: #{error.message}\n")
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
        "kind" => "remote", "provider_output" => output,
        "provider_status" => result.document.fetch("status"),
        "fleet_id" => result.document.fetch("fleet_id"),
        "worker_indices" => result.document["worker_indices"]
      }
    end

    def stop_dispatch?
      super || @remote_halt_mutex.synchronize { @remote_halted }
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
      return unless Thread.current == Thread.main

      @previous_handlers = %w[INT TERM].to_h do |signal|
        [signal, Signal.trap(signal) { @interrupt_signal ||= signal }]
      end
    end

    def rpof_pool?(pool)
      plan.execution_profile.binding_for(pool.id).fetch("backend") == "rpof"
    end

    def validate_remote_jobs!
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

    def finalize_and_report(resource_cleanup_pending: true)
      status = store.finish!(resource_cleanup_pending: resource_cleanup_pending)
      counts = store.counts
      @out.puts "Execution: #{status}"
      @out.puts "Jobs: #{format_counts(counts)}"
      status
    end
  end
end
