# frozen_string_literal: true

require "digest"

module WorkloadOrchestrator
  # Public aggregate evidence; attempt identities and storage paths stay here.
  module ConsumerDemand
    DEMAND_CONTRACT = "wlo-consumer-demand/v0.1"
    DEMAND_HEARTBEAT_SECONDS = 30

    def consumer_heartbeat!(now: Time.now.utc)
      update_execution { |state| state["consumer_heartbeat_at"] = now.utc.iso8601(6) }
    end

    def consumer_demand(pool_id:, now: Time.now.utc)
      pool = plan.pool(pool_id) || raise(Error, "unknown consumer pool")
      identity = Digest::SHA256.hexdigest(JSON.generate([plan.sha256, workdir, output_dir]))
      result = {
        "contract_version" => DEMAND_CONTRACT, "consumer_id" => identity,
        "plan_sha256" => plan.sha256, "pool_id" => pool_id,
        "capability_fingerprint" => demand_capability(pool),
        "observed_at" => now.utc.iso8601(6), "heartbeat_at" => nil,
        "fresh" => false, "state" => "unknown", "runnable_count" => 0,
        "bound_count" => nil, "uncertain_count" => nil, "quiescent" => false
      }
      return result.merge("state" => "missing") unless File.file?(execution_path)

      inspect_action_lock(".state.lock", result, "state_busy", "snapshot unavailable") do
        demand_snapshot(result, pool, now)
      end
    rescue SystemCallError, JSON::ParserError, KeyError, ArgumentError, TypeError => e
      raise Error, "invalid consumer demand evidence: #{e.message}"
    end

    private

    def demand_capability(pool)
      requirement = pool.ollama_requirement
      return nil unless requirement

      OllamaCapabilityRequest.new(JSON.generate(
        "contract_version" => OllamaCapabilityRequest::CONTRACT_VERSION,
        "ollama" => requirement
      )).fingerprint
    end

    def demand_snapshot(result, pool, now)
      validate_existing!
      state = read_execution
      validate_action_state!(state)
      validate_action_jobs!
      metadata = plan.jobs.to_h { |job| [job.id, metadata_for(job)] }
      attempts = demand_attempts
      uncertain = attempts.count { |row| demand_uncertain?(row) }
      bound = attempts.count { |row| row.fetch("status") == "running" || demand_uncertain?(row) }
      heartbeat = state["consumer_heartbeat_at"]
      age = heartbeat && now - Time.iso8601(heartbeat)
      fresh = age && age >= 0 && age <= DEMAND_HEARTBEAT_SECONDS
      active = fresh && state["status"] == "running" && !paused? &&
               !state["dispatch_halt"] && !state["interruption"] && !state.dig("circuit_breaker", "tripped")
      runnable = active ? demand_runnable(pool, metadata) : 0
      terminal = %w[paused completed workload_failed interrupted circuit_broken infrastructure_failed].include?(state["status"])
      result.merge(
        "heartbeat_at" => heartbeat, "fresh" => !!fresh,
        "state" => active ? "active" : (terminal ? state.fetch("status") : (heartbeat ? "stale" : "missing_heartbeat")),
        "runnable_count" => runnable, "bound_count" => bound, "uncertain_count" => uncertain,
        "quiescent" => bound.zero? && (!!fresh || terminal)
      )
    end

    def demand_runnable(pool, metadata)
      selected = plan.jobs.select { |job| job.pool_id == pool.id }
      pending = selected.count do |job|
        row = metadata.fetch(job.id)
        (row.nil? || row["status"] == "pending") && job.depends_on_job_ids.all? do |dependency|
          ExecutionStore::TERMINAL_JOB_STATUSES.include?(metadata[dependency]&.fetch("status"))
        end
      end
      running = selected.count { |job| metadata.fetch(job.id)&.fetch("status") == "running" }
      limit = pool.max_concurrency
      limit ? [pending, [limit - running, 0].max].min : pending
    end

    def demand_attempts
      plan.jobs.flat_map do |job|
        paths = Dir.glob(File.join(output_dir, "attempts", job.id, "attempt-*", "metadata.json"))
        paths << metadata_path(job) if File.file?(metadata_path(job))
        paths.map do |path|
          row = read_json(path, "consumer attempt evidence")
          unless row.is_a?(Hash) && row["job_id"] == job.id && row["pool_id"] == job.pool_id &&
                 (%w[running] + ExecutionStore::TERMINAL_JOB_STATUSES).include?(row["status"])
            raise Error, "invalid consumer attempt identity or status"
          end
          row
        end.uniq { |row| [row.fetch("job_id"), row.fetch("attempt", 1)] }
      end
    end

    def demand_uncertain?(row)
      evidence = row["evidence"] || {}
      # Explicit retry does not establish what happened to the remote command.
      row["status"] == "interrupted" || evidence["outcome_known"] == false ||
        %w[dynamic_worker_loss_in_doubt remote_in_doubt].include?(evidence["kind"])
    end
  end
end
