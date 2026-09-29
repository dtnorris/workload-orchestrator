# frozen_string_literal: true

require "open3"
require "time"
require_relative "pool_fulfillment"
require_relative "rpof_contract"

module WorkloadOrchestrator
  class Runner
    attr_reader :plan, :workers, :workdir, :store

    RemoteWorker = Struct.new(:name, :index, keyword_init: true)

    def initialize(plan:, workers:, workdir:, output_dir:, worker_check: WorkerCheck.new, out: $stdout,
                   rpof_client: nil, capacity_session: nil)
      @plan = plan
      @workers = workers
      @workdir = File.expand_path(workdir)
      @store = ExecutionStore.new(
        output_dir: output_dir, plan: plan, workdir: @workdir,
        workers_sha256: plan.execution_profile && workers.execution_sha256(plan)
      )
      @worker_check = worker_check
      @out = out
      @rpof_client = rpof_client
      @capacity_session = capacity_session
      @remote_halt_mutex = Mutex.new
      @remote_halted = false
      @job_positions = plan.jobs.each_with_index.to_h { |job, index| [job.id, index + 1] }.freeze
    end

    def run(resume: false, acknowledge_circuit_breaker: false)
      validate_workdir!
      workers.validate_plan!(plan)
      store.with_execution_lock do
        store.prepare!
        store.reconcile_remote_running! if rpof?
        yield if block_given?
        run_locked(resume, acknowledge_circuit_breaker)
      end
    end

    private

    def run_locked(resume, acknowledge_circuit_breaker)
      prepare_resume!(resume, acknowledge_circuit_breaker)
      return finalize_and_report if store.paused?

      raise Error, "circuit breaker is tripped; resume with explicit acknowledgement" if store.circuit_tripped?
      if store.dispatch_halted?
        raise Error, "remote dispatch is halted; explicitly retry the failed/in-doubt attempt after reviewing evidence"
      end

      check_workers!
      validate_remote_jobs!
      return run_with_capacity(resume) if rpof?

      store.start!
      schedule_jobs
      finalize_and_report
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
        value = finalize_and_report
        PoolFulfillment::Outcome.new(value: value, retain_capacity: value != "completed")
      end
      status
    rescue Error => e
      store.record_dispatch_halt!(kind: "capacity", error: e.message) unless store.dispatch_halted?
      finalize_and_report
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
      selected_workers(pool).map { |worker| worker_thread(worker, queue) }.each(&:value)
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
                               evidence: remote_evidence(result, output).merge("kind" => "infrastructure"))
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
      stdout, stderr, status = Open3.capture3(environment, *job.argv, chdir: workdir)
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
      store.paused? || store.circuit_tripped? || @remote_halt_mutex.synchronize { @remote_halted }
    end

    def rpof?
      plan.execution_profile&.rpof? == true
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

    def finalize_and_report
      status = store.finish!
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
