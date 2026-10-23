# frozen_string_literal: true

module WorkloadOrchestrator
  # Validates the stopped authoritative store, separately from immutable review
  # parsing. None of these checks changes execution, attempt or worker state.
  module WorkerLossStateValidation
    private

    # Check exact state/job bytes, the full attempt set and the single loss halt.
    # Running, interrupted, retried, changed or unrelated work raises Error.
    def validate_loss_stopped_state!(review, state)
      unless Digest::SHA256.file(execution_path).hexdigest == review["execution_sha256"] &&
             Digest::SHA256.file(File.join(output_dir, "jobs.json")).hexdigest == review["jobs_sha256"]
        raise Error, "worker-loss review stopped state changed"
      end

      unless evidence_sha256(workdir) == review["workdir_evidence_sha256"]
        raise Error, "worker-loss review retained workdir evidence changed"
      end

      validate_loss_attempt_set!(review, state)
      halt = state["dispatch_halt"]
      unless state["status"] == "infrastructure_failed" && state.dig("circuit_breaker", "tripped") == true &&
             halt.is_a?(Hash) && halt["kind"] == "dynamic_worker_loss" &&
             halt.values_at("job_id", "attempt") == review.values_at("job_id", "attempt")
        raise Error, "review does not name the current worker-loss halt and breaker"
      end

      validate_reviewed_loss!(review, halt, state)
    end

    # Bind all retained attempts, including domain failures, to the review.
    # Unreviewed work or any unresolved execution/retry ownership raises Error.
    def validate_loss_attempt_set!(review, state)
      current = validate_action_jobs!
      if current.fetch("running", 0).positive? || current.fetch("interrupted", 0).positive? ||
         state["interruption"] || !state.fetch("retry_pending", {}).empty?
        raise Error, "worker-loss acknowledgement cannot authorize running, interrupted or retried work"
      end

      actual = plan.jobs.filter_map do |job|
        [job.id, Digest::SHA256.file(metadata_path(job)).hexdigest] if File.file?(metadata_path(job))
      end.to_h
      raise Error, "worker-loss review attempt set changed" unless actual == review["attempts"]
    end

    # Validate the named native loss after retained acknowledgement history has
    # been revalidated under both locks by validate_retained_identity!. Only
    # exact acknowledged attempts are exempt; other losses/identities raise Error.
    def validate_reviewed_loss!(review, halt, state)
      losses = plan.jobs.filter_map do |job|
        document = raw_metadata_for(job)
        next if document && acknowledged_worker_loss?(state, job, document)

        [job, document] if document&.dig("evidence", "kind") == "dynamic_worker_loss_in_doubt"
      end
      unless losses.length == 1 && losses.first.first.id == review["job_id"]
        raise Error, "unreviewed dynamic worker loss remains"
      end

      job, document = losses.first
      validate_worker_loss_evidence!(reviewed_loss_attempt(job, document), document.fetch("evidence"))
      return if document["status"] == "failed" && document["attempt"] == review["attempt"] &&
                document.dig("evidence", "reason") == halt["error"] &&
                document.dig("evidence", "outcome_known") == false

      raise Error, "reviewed worker-loss attempt differs"
    end

    # Reconstruct the native bound attempt token from retained metadata; invalid
    # worker/snapshot identity or timestamps raise the existing native errors.
    def reviewed_loss_attempt(job, document)
      DynamicAttempt.new(job:, attempt_id: document.fetch("attempt"),
                         started_at: Time.iso8601(document.fetch("started_at")),
                         worker_binding: DynamicWorkerBinding.from_metadata(document))
    end
  end
end
