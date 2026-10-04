# frozen_string_literal: true

module WorkloadOrchestrator
  # Read-only admissibility on the execution store. Never calls prepare!, retry,
  # repair or runner setup: those operations can create or rewrite evidence.
  module ExecutionActionCheck
    ACTION_CHECK_CONTRACT_VERSION = "wlo-execution-action-check/v0.1"
    CHECKED_ACTIONS = %w[run resume retry-failed recovery repair].freeze
    RETAINED_STATUSES = %w[
      pending running paused completed workload_failed interrupted circuit_broken
      infrastructure_failed cleanup_pending cleanup_failed
    ].freeze

    def action_check(action)
      raise Error, "unknown execution action #{action.inspect}" unless CHECKED_ACTIONS.include?(action)

      result = {
        "contract_version" => ACTION_CHECK_CONTRACT_VERSION, "action" => action,
        "disposition" => "blocked", "reason" => "invalid_retained_execution", "execution_status" => nil
      }
      inspect_action_lock(".execution.lock", result, "executor_active", "already has an active executor") do
        inspect_action_lock(".state.lock", result, "state_busy", "execution state is being updated; inspect again") do
          inspect_execution_action(action, result)
        end
      end
    rescue Error, SystemCallError, IOError, JSON::ParserError, KeyError, TypeError, ArgumentError => e
      raise unless defined?(result) && result

      result.merge("disposition" => "blocked", "reason" => "invalid_retained_execution",
                   "execution_status" => nil, "message" => "cannot inspect retained execution: #{e.message}")
    end

    private

    # Only open existing locks, read-only, and retain shared locks through the
    # snapshot. Contention is a blocked result; no directory or lock is created.
    def inspect_action_lock(name, result, reason, message)
      path = File.join(output_dir, name)
      return yield unless File.exist?(path)

      File.open(path, File::RDONLY) do |file|
        return action_result(result, "blocked", reason, message) unless file.flock(File::LOCK_SH | File::LOCK_NB)

        yield
      end
    end

    def inspect_execution_action(action, result)
      unless File.file?(execution_path)
        ensure_unclaimed_output! if File.exist?(output_dir)
        if action == "run"
          return runnable_action_result(result, "new_execution")
        end

        return action_result(result, "blocked", "no_retained_execution", "has no retained execution")
      end

      # Shape checks precede the store's identity/import validation so malformed
      # evidence cannot escape as a Ruby method error or be treated as pending.
      state = read_execution
      validate_action_state!(state)
      validate_existing!
      current = validate_action_jobs!
      result["execution_status"] = state.fetch("status")
      return action_result(result, "execute", "retained_execution") if action == "recovery"
      if action == "repair"
        return action_result(result, "blocked", "repair_unsupported",
                             "has no supported deterministic retained-state repair") unless repair_supported?
      end
      return check_retry_action(result, current) if action == "retry-failed"

      check_execution_action(action, result, state, current)
    end

    def check_retry_action(result, current)
      if current.fetch("running", 0).positive?
        return action_result(result, "blocked", "running_attempts", "has running attempts; wait for a clean stop before retrying")
      end
      unless plan.jobs.any? { |job| raw_retryable?(job) }
        return action_result(result, "blocked", "no_retryable_jobs", "has no failed or interrupted jobs to select for retry")
      end

      # Selection, reason, acknowledgement, archive conflicts and idempotence
      # remain exclusively owned by authorize_retry!. This is not authorization.
      action_result(result, "execute", "retry_selection_required")
    end

    def check_execution_action(action, result, state, current)
      if circuit_tripped?
        return action_result(result, "blocked", "circuit_breaker", "circuit breaker is tripped; explicit reviewed recovery is required")
      end
      if dispatch_halted? || unrestored_dynamic_loss(state).first
        return action_result(result, "blocked", "dispatch_halt", "dispatch is halted; explicit reviewed recovery is required")
      end
      if state["interruption"] || current.fetch("interrupted", 0).positive?
        return action_result(result, "blocked", "interrupted_attempts", "has interrupted work; explicit retry authorization is required")
      end
      if state.fetch("status") == "running"
        return action_result(result, "blocked", "inactive_running_state",
                             "retains running state without an active executor; review before recovery")
      end
      if current.fetch("running", 0).positive?
        return action_result(result, "blocked", "running_attempts", "has running attempts without an active executor; review before recovery")
      end
      case state.fetch("status")
      when "completed"
        unless current.fetch("complete", 0) == plan.jobs.length
          raise Error, "completed execution has non-complete job evidence"
        end
        return action_result(result, "already_complete", "completed")
      when "workload_failed"
        return action_result(result, "blocked", "workload_failed", "has failed work; review evidence and use retry-failed explicitly")
      when "interrupted", "circuit_broken", "infrastructure_failed", "cleanup_pending", "cleanup_failed"
        return action_result(result, "blocked", "recovery_required", "requires explicit reviewed recovery (#{state.fetch('status')})")
      end
      if paused? || state.fetch("status") == "paused"
        return action_result(result, "blocked", "paused", "is paused; use explicit resume") if action == "run"

        return runnable_action_result(result, "resume_paused_execution")
      end

      runnable_action_result(result, "retained_pending_execution")
    end

    def runnable_action_result(result, reason)
      plan.execution_profile&.ensure_runnable!
      action_result(result, "execute", reason)
    rescue Error
      action_result(result, "blocked", "execution_profile_not_runnable", "execution profile is historical and cannot be executed")
    end

    def action_result(result, disposition, reason, message = nil)
      result.merge("disposition" => disposition, "reason" => reason).tap do |document|
        document["message"] = message if message
      end
    end

    def validate_action_state!(state)
      raise Error, "execution state must be an object" unless state.is_a?(Hash)
      raise Error, "invalid retained execution status" unless RETAINED_STATUSES.include?(state["status"])
      breaker = state.fetch("circuit_breaker")
      unless breaker.is_a?(Hash) && [true, false].include?(breaker["tripped"])
        raise Error, "execution circuit breaker must contain boolean tripped"
      end
      %w[terminal_import dispatch_halt interruption retry_pending].each do |key|
        raise Error, "invalid execution #{key}" if state.key?(key) && !state[key].is_a?(Hash)
      end
      if state.key?("retry_history") &&
         (!state["retry_history"].is_a?(Array) || !state["retry_history"].all? { |row| row.is_a?(Hash) })
        raise Error, "invalid execution retry_history"
      end
      pending = state.fetch("retry_pending", {})
      unless pending.all? { |id, attempt| plan.jobs.any? { |job| job.id == id } && attempt.is_a?(Integer) && attempt.positive? }
        raise Error, "invalid execution retry_pending"
      end
    end

    def validate_action_jobs!
      summary = read_json(File.join(output_dir, "jobs.json"), "job state")
      rows = summary.is_a?(Hash) && summary["jobs"]
      unless rows.is_a?(Array) && rows.all? { |row| row.is_a?(Hash) } &&
             rows.map { |row| row["job_id"] }.sort == plan.jobs.map(&:id).sort
        raise Error, "retained jobs must match the plan exactly"
      end
      by_id = rows.to_h { |row| [row.fetch("job_id"), row] }
      plan.jobs.each do |job|
        row = by_id.fetch(job.id)
        validate_action_metadata!(job)
        metadata = metadata_for(job)
        expected = [job.pool_id, metadata ? metadata.fetch("status") : "pending", metadata && metadata["attempt"]]
        actual = [row["pool_id"], row["status"], row["attempt"]]
        raise Error, "retained job summary differs from attempt evidence for #{job.id}" unless actual == expected
      end
      counts
    end

    def validate_action_metadata!(job)
      path = metadata_path(job)
      return unless File.exist?(path)

      raw = read_json(path, "job metadata")
      unless raw.is_a?(Hash) && raw["job_id"] == job.id && raw["pool_id"] == job.pool_id
        raise Error, "retained attempt identity differs for #{job.id}"
      end
      raise Error, "invalid attempt for #{job.id}" unless Integer(raw.fetch("attempt", 1)).positive?
      if raw.key?("evidence") && !raw["evidence"].is_a?(Hash)
        raise Error, "invalid attempt evidence for #{job.id}"
      end
      raw_metadata_for(job)
    end
  end
end
