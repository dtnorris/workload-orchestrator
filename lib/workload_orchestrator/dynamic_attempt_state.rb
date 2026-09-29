# frozen_string_literal: true

module WorkloadOrchestrator
  DynamicAttempt = Struct.new(:job, :attempt_id, :started_at, :worker_binding, keyword_init: true) do
    def initialize(**values)
      super
      freeze
    end
  end

  # Shared durable attempt creation used by legacy and dynamic execution.
  module AttemptPersistence
    private

    def persist_running!(job:, worker_name:, environment_keys:, worker_binding:)
      started_at = Time.now
      attempt_id = nil
      with_lock do
        prior = metadata_for(job)
        if worker_binding && prior && prior.fetch("status") != "pending"
          raise Error, "dynamic claim requires pending work; existing attempt evidence was retained"
        end
        attempt_id = prior ? Integer(prior.fetch("attempt", 0)) + 1 : 1
        document = {
          "job_id" => job.id,
          "pool_id" => job.pool_id,
          "worker" => worker_name,
          "status" => "running",
          "attempt" => attempt_id,
          "started_at" => started_at.iso8601,
          "argv" => job.argv,
          "environment_keys" => environment_keys.sort
        }
        document.merge!(worker_binding.metadata) if worker_binding
        FileUtils.mkdir_p(run_dir(job))
        write_json(metadata_path(job), document)
        rebuild_jobs_unlocked
      end
      [started_at, attempt_id]
    rescue ArgumentError, TypeError => e
      raise Error, "invalid attempt number for #{job.id}: #{e.message}"
    end

    def validate_terminal_status!(status)
      return if ExecutionStore::TERMINAL_JOB_STATUSES.include?(status)

      raise Error, "invalid terminal status #{status.inspect}"
    end
  end

  # Dynamic-attempt extensions for the authoritative ExecutionStore. Keeping
  # these operations in the store prevents a second attempt-state system.
  module DynamicAttemptState
    # The returned token is derived from metadata durably written before
    # dispatch. The exact token must accompany the attempt's terminal result.
    def record_dynamic_running!(job:, worker:, environment_keys:)
      unless worker.is_a?(RegistryWorker) && worker.ready?
        raise Error, "dynamic attempts require a READY registry worker"
      end

      binding = DynamicWorkerBinding.from_worker(worker)
      started_at, attempt_id = persist_running!(
        job: job,
        worker_name: worker.worker_id,
        environment_keys: environment_keys,
        worker_binding: binding
      )
      DynamicAttempt.new(job: job, attempt_id: attempt_id, started_at: started_at, worker_binding: binding)
    end

    def dynamic_running_attempts
      plan.jobs.filter_map do |job|
        path = metadata_path(job)
        next unless File.file?(path)

        document = read_json(path, "job metadata")
        next unless document.fetch("status") == "running"

        binding = DynamicWorkerBinding.from_metadata(document)
        DynamicAttempt.new(
          job: job,
          attempt_id: positive_attempt!(document, job),
          started_at: Time.iso8601(document.fetch("started_at")),
          worker_binding: binding
        )
      end.freeze
    rescue KeyError, ArgumentError => e
      raise Error, "invalid dynamic running attempt: #{e.message}"
    end

    private

    def positive_attempt!(document, job)
      attempt_id = Integer(document.fetch("attempt"))
      return attempt_id if attempt_id.positive?

      raise Error, "dynamic attempt number is invalid for #{job.id}"
    rescue ArgumentError, TypeError
      raise Error, "dynamic attempt number is invalid for #{job.id}"
    end

    def validate_dynamic_attempt!(attempt)
      valid = attempt.is_a?(DynamicAttempt) && plan.jobs.include?(attempt.job) &&
              attempt.attempt_id.is_a?(Integer) && attempt.attempt_id.positive? &&
              attempt.started_at.is_a?(Time) && attempt.worker_binding.is_a?(DynamicWorkerBinding)
      raise Error, "invalid dynamic attempt token" unless valid
    end
  end

  module DynamicAttemptTerminalState
    def record_dynamic_terminal!(attempt:, status:, exit_status:, error: nil, evidence: nil)
      validate_dynamic_attempt!(attempt)
      validate_terminal_status!(status)
      with_lock do
        document = read_json(metadata_path(attempt.job), "job metadata")
        if current_dynamic_attempt?(document, attempt)
          record_current_dynamic_terminal!(document, attempt, status, exit_status, error, evidence)
        else
          record_late_dynamic_terminal!(attempt, status, exit_status, error, evidence)
        end
      end
    end

    private

    def current_dynamic_attempt?(document, attempt)
      return false unless document.fetch("attempt") == attempt.attempt_id

      DynamicWorkerBinding.from_metadata(document).same_identity?(attempt.worker_binding)
    end

    def record_current_dynamic_terminal!(document, attempt, status, exit_status, error, evidence)
      unless document.fetch("status") == "running"
        return record_late_evidence!(metadata_path(attempt.job), document, status, exit_status, error, evidence)
      end

      classification = dynamic_failure_class(status, exit_status)
      terminalize_dynamic_document!(document, {
                                      status: status,
                                      started_at: attempt.started_at,
                                      exit_status: exit_status,
                                      error: error,
                                      evidence: evidence,
                                      classification: classification
                                    })
      write_json(metadata_path(attempt.job), document)
      record_dynamic_breaker_result!(status, classification)
      rebuild_jobs_unlocked
      :recorded
    end

    def record_late_dynamic_terminal!(attempt, status, exit_status, error, evidence)
      path = File.join(output_dir, "attempts", attempt.job.id, "attempt-#{attempt.attempt_id}", "metadata.json")
      return :stale_attempt unless File.file?(path)

      document = read_json(path, "archived job metadata")
      return :stale_attempt unless current_dynamic_attempt?(document, attempt)

      record_late_evidence!(path, document, status, exit_status, error, evidence)
    end

    def record_late_evidence!(path, document, status, exit_status, error, evidence)
      return :already_terminal unless document.dig("evidence", "kind") == "dynamic_worker_loss_in_doubt"

      row = {
        "kind" => "late_dynamic_attempt_terminal",
        "observed_at" => timestamp,
        "status" => status,
        "exit_status" => exit_status
      }
      row["error"] = error if error
      row["evidence"] = evidence if evidence
      (document["late_evidence"] ||= []) << row
      write_json(path, document)
      :late_evidence
    end

    def terminalize_dynamic_document!(document, attributes)
      completed_at = Time.now
      status = attributes.fetch(:status)
      document.merge!(
        "status" => status,
        "completed_at" => completed_at.iso8601,
        "elapsed_seconds" => (completed_at - attributes.fetch(:started_at)).round(3),
        "exit_status" => attributes.fetch(:exit_status),
        "failure_class" => status == "failed" ? attributes.fetch(:classification) : nil
      )
      document["error"] = attributes[:error] if attributes[:error]
      document["evidence"] = attributes[:evidence] if attributes[:evidence]
    end

    def dynamic_failure_class(status, exit_status)
      return nil unless status == "failed"
      return "non_operational" if plan.failure_policy.fetch("non_operational_exit_statuses").include?(exit_status)

      "operational"
    end

    def record_dynamic_breaker_result!(job_status, classification)
      state = read_execution
      update_dynamic_breaker!(state, job_status, classification)
      state["updated_at"] = timestamp
      write_json(execution_path, state)
    end

    def update_dynamic_breaker!(state, job_status, classification)
      breaker = state.fetch("circuit_breaker")
      return if breaker.fetch("tripped")

      update_breaker_counters!(breaker, job_status, classification)
      trip_breaker!(breaker) if breaker_reason(breaker)
    end
  end

  module DynamicWorkerLossState
    def record_dynamic_worker_loss!(attempt:, evidence:)
      validate_dynamic_attempt!(attempt)
      validate_worker_loss_evidence!(attempt, evidence)
      with_lock do
        document = read_json(metadata_path(attempt.job), "job metadata")
        return :already_terminal unless document.fetch("status") == "running"
        return :stale_attempt unless current_dynamic_attempt?(document, attempt)

        classification = "non_operational"
        terminalize_dynamic_document!(document, {
                                        status: "failed",
                                        started_at: attempt.started_at,
                                        exit_status: nil,
                                        error: "dynamic worker identity was lost; command outcome is in doubt " \
                                               "and explicit retry is required",
                                        evidence: evidence,
                                        classification: classification
                                      })
        write_json(metadata_path(attempt.job), document)
        record_dynamic_loss_execution!(attempt, evidence, classification)
        rebuild_jobs_unlocked
        :recorded_in_doubt
      end
    end

    # Repairs the only cross-file crash window: loss metadata is authoritative
    # and is written before the execution-level dispatch halt.
    def restore_dynamic_worker_loss_halt!
      with_lock do
        state = read_execution
        return false if state.key?("dispatch_halt")

        job, document = unrestored_dynamic_loss(state)
        return false unless job

        evidence = document.fetch("evidence")
        update_dynamic_breaker!(state, "failed", document.fetch("failure_class"))
        state["dispatch_halt"] = {
          "kind" => "dynamic_worker_loss",
          "error" => evidence.fetch("reason"),
          "job_id" => job.id,
          "attempt" => document.fetch("attempt"),
          "at" => timestamp
        }
        state["status"] = "infrastructure_failed"
        state["updated_at"] = timestamp
        write_json(execution_path, state)
        true
      end
    end

    private

    def unrestored_dynamic_loss(state)
      retry_pending = state.fetch("retry_pending", {})
      plan.jobs.each do |job|
        path = metadata_path(job)
        next unless File.file?(path)

        document = read_json(path, "job metadata")
        next unless document.fetch("status") == "failed"
        next unless document.dig("evidence", "kind") == "dynamic_worker_loss_in_doubt"
        next if retry_pending[job.id] == document.fetch("attempt")

        return [job, document]
      end
      [nil, nil]
    end

    def validate_worker_loss_evidence!(attempt, evidence)
      valid = evidence.is_a?(Hash) && evidence["kind"] == "dynamic_worker_loss_in_doubt" &&
              evidence["job_id"] == attempt.job.id && evidence["attempt_id"] == attempt.attempt_id &&
              evidence["outcome_known"] == false &&
              evidence["worker_execution_identity"] == attempt.worker_binding.execution_identity &&
              evidence["worker_registry_binding"] == attempt.worker_binding.registry_binding
      raise Error, "dynamic worker-loss evidence does not match the bound attempt" unless valid
    end

    def record_dynamic_loss_execution!(attempt, evidence, classification)
      state = read_execution
      update_dynamic_breaker!(state, "failed", classification)
      state["dispatch_halt"] ||= {
        "kind" => "dynamic_worker_loss",
        "error" => evidence.fetch("reason"),
        "job_id" => attempt.job.id,
        "attempt" => attempt.attempt_id,
        "at" => timestamp
      }
      state["status"] = "infrastructure_failed"
      state["updated_at"] = timestamp
      write_json(execution_path, state)
    end
  end
end
