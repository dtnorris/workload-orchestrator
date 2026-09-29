# frozen_string_literal: true

module WorkloadOrchestrator
  # Read-only presentation of the same execution report used by status/summary.
  # Workers are supplied by the accepted registry, never discovered by the UI.
  class LiveExecutionReport
    def initialize(plan:, output:)
      @plan = plan
      @root = File.expand_path(output)
      @report = ExecutionReport.new(plan: plan, output: output)
      @matcher = CapabilityMatcher.new(plan: plan)
    end

    def document(workers: nil)
      state = @report.document
      state["jobs"] = state.fetch("jobs").map do |row|
        row.merge("in_doubt" => in_doubt?(row))
      end
      ready = Array(workers).select(&:ready?).sort_by(&:execution_identity)
      running = state.fetch("jobs").select { |row| row["status"] == "running" }
      identities = running.filter_map { |row| row["worker_execution_identity"] }
      busy = ready.select do |worker|
        identities.include?(DynamicWorkerBinding.from_worker(worker).execution_identity)
      end
      idle = ready - busy
      pending = state.fetch("jobs").select { |row| row["status"] == "pending" }
      eligible = eligible_idle_workers(idle, pending, state.fetch("jobs"))
      state.merge(
        "registry_known" => !workers.nil?,
        "ready_workers" => ready.length, "busy_workers" => busy.length,
        "idle_workers" => idle.length, "eligible_idle_workers" => eligible.length,
        "workers" => ready.map { |worker| worker_row(worker, busy.include?(worker)) },
        "condition" => condition(state, workers, pending, eligible)
      )
    end

    def job_label(row)
      job = @plan.jobs.find { |entry| entry.id == row.fetch("job_id") }
      identity = row["worker_execution_identity"] || {}
      fields = [row.fetch("job_id")]
      fields << "group=#{job.group_id}" if job&.group_id
      fields << "pool=#{row.fetch('pool_id')}"
      fields << "worker=#{identity['worker_id'] || row['worker']}"
      fields << "generation=#{identity['generation_id']}" if identity["generation_id"]
      fields << "attempt=#{row['attempt']}"
      fields.join(" ")
    end

    private

    def in_doubt?(row)
      return false unless row["status"] == "failed"

      metadata = JSON.parse(File.read(File.join(@root, "runs", row.fetch("job_id"), "metadata.json")))
      metadata["attempt"] == row["attempt"] && metadata.dig("evidence", "kind") == "dynamic_worker_loss_in_doubt"
    end

    def eligible_idle_workers(idle, pending, rows)
      by_id = rows.to_h { |row| [row.fetch("job_id"), row] }
      running = rows.select { |row| row["status"] == "running" }.group_by { |row| row["pool_id"] }
      runnable = pending.filter_map do |row|
        job = @plan.jobs.find { |entry| entry.id == row.fetch("job_id") }
        next unless job.depends_on_job_ids.all? do |id|
          ExecutionStore::TERMINAL_JOB_STATUSES.include?(by_id.fetch(id).fetch("status"))
        end

        pool = @plan.pool(job.pool_id)
        next if pool.max_concurrency && running.fetch(pool.id, []).length >= pool.max_concurrency

        job
      end
      idle.select { |worker| runnable.any? { |job| @matcher.match_job(job: job, worker: worker).compatible? } }
    end

    def worker_row(worker, busy)
      {
        "worker_id" => worker.worker_id, "generation_id" => worker.generation_id,
        "busy" => busy,
        "models" => worker.ollama_models.map { |model| model.fetch("model") },
        "pools" => @plan.pools.select { |pool| @matcher.match_pool(worker: worker, pool: pool).compatible? }.map(&:id)
      }
    end

    def condition(state, workers, pending, eligible)
      if state["dispatch_halt"]
        halt = state.fetch("dispatch_halt")
        return "HALT dispatch halted kind=#{halt['kind']} reason=#{halt['error']}; explicit review required"
      end
      return "INTERRUPTED signal=#{state.dig('interruption', 'signal')}" if state["interruption"]
      return "BREAKER #{state.dig('circuit_breaker', 'reason')}" if state.dig("circuit_breaker", "tripped")
      return "PAUSED execution retained; resume the same plan" if state["paused"] && state.dig("counts", "running").zero?
      return "PAUSE requested; draining active jobs" if state["paused"]
      return "Execution: #{state['status']}" unless state["status"] == "running"
      return nil if workers.nil? || pending.empty? || !eligible.empty?

      "WAIT Waiting for compatible capacity: #{pending.length} pending, 0 eligible idle READY workers"
    end
  end
end
