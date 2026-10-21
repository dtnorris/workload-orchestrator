# frozen_string_literal: true

require_relative "worker_loss_review"

module WorkloadOrchestrator
  # Releases one reviewed loss halt, retaining every attempt and pending job.
  # The owner/state locks serialize the one auditable execution-state commit.
  module WorkerLossAcknowledgement
    # Validate exact review bytes under both locks. Dry run writes no state;
    # apply clears only this halt and breaker. Invalid evidence raises Error.
    def acknowledge_worker_loss!(bytes:, sha256:, dry_run: false)
      review = WorkerLossReview.new(bytes:, sha256:)
      validate_loss_paths!
      with_execution_lock do
        with_lock do
          validate_retained_identity!
          review.validate!(plan:, workdir:, output_dir:)
          state = read_execution
          existing = state.fetch("worker_loss_acknowledgements", []).find { |row| row["review_sha256"] == sha256 }
          return existing.merge("idempotent" => true) if existing

          validate_loss_stopped_state!(review.document, state)
          event = { "review_sha256" => sha256, "review_bytes" => bytes, "review" => review.document,
                    "at" => timestamp, "halt_before" => state.fetch("dispatch_halt"),
                    "breaker_before" => state.fetch("circuit_breaker").dup }
          return event.merge("dry_run" => true) if dry_run

          commit_loss_acknowledgement!(state, event)
          event.merge("dry_run" => false, "idempotent" => false)
        end
      end
    rescue JSON::ParserError, KeyError, TypeError, ArgumentError, SystemCallError => e
      raise Error, "invalid worker-loss acknowledgement: #{e.message}"
    end

    private

    # Require existing regular execution/control files before acquiring ownership.
    # Missing or linked paths raise Error without creating an execution.
    def validate_loss_paths!
      paths = %w[execution.json jobs.json plan.json .execution.lock .state.lock]
      regular = paths.all? do |name|
        File.file?(File.join(output_dir, name)) && !File.symlink?(File.join(output_dir, name))
      end
      return if regular && File.directory?(output_dir) && Pathname.new(output_dir).ascend.none?(&:symlink?)

      raise Error, "worker-loss acknowledgement requires regular retained execution files"
    end

    # Commit the audit and control transition in one durable state replacement.
    # Existing breaker history remains; no jobs/attempts/pauses are written.
    def commit_loss_acknowledgement!(state, event)
      (state["worker_loss_acknowledgements"] ||= []) << event
      state.delete("dispatch_halt")
      reset_breaker!(state.fetch("circuit_breaker"))
      state["status"] = "pending"
      state["updated_at"] = timestamp
      write_json(execution_path, state)
      File.open(execution_path, File::RDONLY, &:fsync)
      File.open(output_dir, File::RDONLY, &:fsync)
    end

    # Recheck durable authorization during preparation/action-check. Changes to
    # raw review bytes, its parsed audit copy or evidence raise Error.
    def validate_worker_loss_acknowledgements!(state)
      rows = state.fetch("worker_loss_acknowledgements", [])
      raise Error, "invalid worker-loss acknowledgement history" unless rows.is_a?(Array)

      rows.each do |row|
        review = WorkerLossReview.new(bytes: row.fetch("review_bytes"), sha256: row.fetch("review_sha256"))
        raise Error, "retained worker-loss review changed" unless review.document == row.fetch("review")

        review.validate!(plan:, workdir:, output_dir:)
      end
    rescue JSON::ParserError, KeyError, TypeError, ArgumentError => e
      raise Error, "invalid retained worker-loss review: #{e.message}"
    end

    # Exempt only the exact acknowledged attempt from crash-window restoration.
    # New or changed attempts are still restored to a blocking loss halt.
    def acknowledged_worker_loss?(state, job, document)
      state.fetch("worker_loss_acknowledgements", []).any? do |row|
        review = row.fetch("review")
        review["job_id"] == job.id && review["attempt"] == document["attempt"] &&
          review.fetch("attempts")[job.id] == Digest::SHA256.file(metadata_path(job)).hexdigest
      end
    end
  end
end
