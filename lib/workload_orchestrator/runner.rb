# frozen_string_literal: true

require "open3"

module WorkloadOrchestrator
  class Runner
    attr_reader :plan, :workers, :workdir, :store

    def initialize(plan:, workers:, workdir:, output_dir:, worker_check: WorkerCheck.new, out: $stdout)
      @plan = plan
      @workers = workers
      @workdir = File.expand_path(workdir)
      @store = ExecutionStore.new(
        output_dir: output_dir, plan: plan, workdir: @workdir,
        workers_sha256: plan.execution_profile && workers.execution_sha256(plan)
      )
      @worker_check = worker_check
      @out = out
      @job_positions = plan.jobs.each_with_index.to_h { |job, index| [job.id, index + 1] }.freeze
    end

    def run(resume: false, acknowledge_circuit_breaker: false)
      validate_workdir!
      workers.validate_plan!(plan)
      store.with_execution_lock do
        store.prepare!
        yield if block_given?
        run_locked(resume, acknowledge_circuit_breaker)
      end
    end

    private

    def run_locked(resume, acknowledge_circuit_breaker)
      prepare_resume!(resume, acknowledge_circuit_breaker)
      return finalize_and_report if store.paused?

      raise Error, "circuit breaker is tripped; resume with explicit acknowledgement" if store.circuit_tripped?

      check_workers!
      store.start!
      if plan.grouped_jobs?
        run_job_groups
      else
        run_pools
      end
      finalize_and_report
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
      selected = pool.worker_names.first(pool.max_concurrency)
      selected.map { |name| worker_thread(workers.fetch(name), queue) }.each(&:value)
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
      store.paused? || store.circuit_tripped?
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
