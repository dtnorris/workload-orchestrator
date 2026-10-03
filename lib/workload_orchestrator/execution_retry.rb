# frozen_string_literal: true

require "digest"
require "json"
require "tmpdir"

module WorkloadOrchestrator
  # Retry authorization is one atomic execution-state write. Until it commits,
  # all failed or interrupted jobs stay terminal, even if an archive was already copied.
  module ExecutionRetry
    RECOVERY_ACTION_CONTRACT_VERSION = "wlo-recovery-action/v0.1"
    RECOVERY_HISTORY_CONTRACT_VERSION = "wlo-recovery-history/v0.1"
    RECOVERY_HISTORY_LIMIT = 50

    def retry_failed!(reason:, all: false, job_ids: [], acknowledge_circuit_breaker: false)
      authorize_retry!(
        reason:, all:, job_ids:, acknowledge_circuit_breaker:
      ).fetch("jobs").map { |row| row.fetch("job_id") }
    end

    def authorize_retry!(reason:, all: false, job_ids: [], acknowledge_circuit_breaker: false,
                         dry_run: false)
      ensure_prepared!
      with_execution_lock do
        with_lock do
          validate_existing!
          state = read_execution
          selected = retry_selection!(all, job_ids, reason, state)
          request = retry_request(state, selected, reason.strip, acknowledge_circuit_breaker)
          if (existing = idempotent_retry(state, request))
            return existing.merge("idempotent" => true, "dry_run" => false)
          end

          reject_already_queued!(state, selected)
          validate_retry_breaker!(state, acknowledge_circuit_breaker)
          return request.merge("result" => "preview", "dry_run" => true) if dry_run

          archives = selected.map do |job|
            relative = archive_attempt!(job)
            [relative, evidence_sha256(File.join(output_dir, relative))]
          end
          event = commit_retry!(state, selected, archives, request, acknowledge_circuit_breaker)
          rebuild_jobs_unlocked
          event.merge("idempotent" => false, "dry_run" => false)
        end
      end
    end

    def recovery_history
      ensure_prepared!
      with_execution_lock do
        with_lock do
          validate_existing!
          state = read_execution
          history = state.fetch("retry_history", [])
          {
            "contract_version" => RECOVERY_HISTORY_CONTRACT_VERSION,
            "plan_id" => state.fetch("plan_id"),
            "plan_sha256" => state.fetch("plan_sha256"),
            "total_actions" => history.length,
            "actions" => history.last(RECOVERY_HISTORY_LIMIT),
            "truncated" => history.length > RECOVERY_HISTORY_LIMIT,
            "repair_supported" => false
          }
        end
      end
    end

    def repair!(reason:, dry_run: false)
      raise Error, "repair requires a nonblank --reason" if reason.to_s.strip.empty?

      ensure_prepared!
      with_execution_lock do
        with_lock do
          validate_existing!
          state = read_execution
          result = {
            "contract_version" => RECOVERY_ACTION_CONTRACT_VERSION,
            "action" => "repair",
            "reason" => reason.strip,
            "result" => "unsupported",
            "plan_id" => state.fetch("plan_id"),
            "plan_sha256" => state.fetch("plan_sha256"),
            "detail" => "no deterministic retained-state repair is currently supported"
          }
          return result.merge("dry_run" => true) if dry_run

          raise Error, "no deterministic retained-state repair is supported; retained evidence was not changed"
        end
      end
    end

    private

    def validate_retry_selection!(all, job_ids, reason)
      raise Error, "retry requires a nonblank --reason" if reason.to_s.strip.empty?
      raise Error, "choose --all or one or more --job IDs" if all == !job_ids.empty?
      raise Error, "duplicate --job IDs" unless job_ids.uniq == job_ids
      raise Error, "running jobs remain; wait for a clean stop before retrying" if counts["running"].positive?

      unknown = job_ids - plan.jobs.map(&:id)
      raise Error, "unknown job IDs: #{unknown.join(', ')}" unless unknown.empty?
    end

    def retry_selection!(all, job_ids, reason, _state)
      validate_retry_selection!(all, job_ids, reason)
      selected = plan.jobs.select do |job|
        all ? raw_retryable?(job) : job_ids.include?(job.id)
      end
      raise Error, "no failed or interrupted jobs selected" if selected.empty?

      selected.each do |job|
        raise Error, "job #{job.id} is not failed or interrupted" unless raw_retryable?(job)
      end
      selected
    end

    def raw_retryable?(job)
      ExecutionStore::RETRYABLE_JOB_STATUSES.include?(raw_metadata_for(job)&.fetch("status", nil))
    end

    def raw_metadata_for(job)
      path = metadata_path(job)
      return nil unless File.file?(path)

      document = read_json(path, "job metadata")
      status = document.fetch("status").to_s
      allowed = ExecutionStore::TERMINAL_JOB_STATUSES + ["running"]
      raise Error, "invalid job status #{status.inspect} in #{path}" unless allowed.include?(status)

      document
    rescue KeyError => e
      raise Error, "invalid job metadata #{path}: #{e.message}"
    end

    def retry_request(state, selected, reason, acknowledge)
      jobs = selected.map { |job| retry_job_evidence(job) }
      identity = {
        "plan_sha256" => state.fetch("plan_sha256"),
        "action" => "retry_selected",
        "reason" => reason,
        "jobs" => jobs,
        "breaker_acknowledged" => acknowledge
      }
      {
        "contract_version" => RECOVERY_ACTION_CONTRACT_VERSION,
        "action_id" => Digest::SHA256.hexdigest(JSON.generate(identity)),
        "action" => "retry_selected",
        "reason" => reason,
        "jobs" => jobs,
        "breaker_acknowledged" => acknowledge,
        "breaker_before" => state.fetch("circuit_breaker").dup,
        "resume_requirements" => ["explicit_resume"]
      }
    end

    def retry_job_evidence(job)
      metadata = raw_metadata_for(job)
      attempt = Integer(metadata.fetch("attempt", 1))
      names = attempt_entry_names(job, attempt)
      row = {
        "job_id" => job.id,
        "prior_status" => metadata.fetch("status"),
        "prior_attempt" => attempt,
        "archive" => File.join("attempts", job.id, "attempt-#{attempt}"),
        "evidence_sha256" => evidence_sha256(run_dir(job), names: names),
        "metadata_sha256" => Digest::SHA256.file(metadata_path(job)).hexdigest
      }
      row["side_effects_uncertain"] = true if metadata.fetch("status") == "interrupted" ||
                                               metadata.dig("evidence", "kind").to_s.include?("in_doubt")
      row
    rescue ArgumentError, TypeError => e
      raise Error, "invalid attempt for #{job.id}: #{e.message}"
    end

    def idempotent_retry(state, request)
      event = state.fetch("retry_history", []).find do |row|
        row["action_id"] == request.fetch("action_id")
      end
      return unless event

      pending = state.fetch("retry_pending", {})
      return unless event.fetch("jobs").all? do |row|
        pending[row.fetch("job_id")] == row.fetch("prior_attempt", row["attempt"])
      end

      event
    end

    def reject_already_queued!(state, selected)
      pending = state.fetch("retry_pending", {})
      queued = selected.filter_map do |job|
        metadata = raw_metadata_for(job)
        job.id if pending[job.id] == metadata.fetch("attempt", 1)
      end
      return if queued.empty?

      raise Error, "retry already queued for job IDs: #{queued.join(', ')}"
    end

    def validate_retry_breaker!(state, acknowledge)
      tripped = state.fetch("circuit_breaker").fetch("tripped")
      raise Error, "retry requires --acknowledge-circuit-breaker: breaker is tripped" if tripped && !acknowledge
      raise Error, "circuit breaker is not tripped" if acknowledge && !tripped
    end

    def commit_retry!(state, selected, archives, request, acknowledge)
      requested_at = timestamp
      event = request.merge(
        "requested_at" => requested_at,
        "at" => requested_at,
        "result" => "authorized",
        "jobs" => request.fetch("jobs").zip(archives).map do |row, (archive, digest)|
          raise Error, "attempt archive digest does not match retained evidence" unless
            digest == row.fetch("evidence_sha256")

          row.merge("archive" => archive, "attempt" => row.fetch("prior_attempt"))
        end
      )
      (state["retry_history"] ||= []) << event
      event.fetch("jobs").each do |row|
        (state["retry_pending"] ||= {})[row.fetch("job_id")] = row.fetch("prior_attempt")
      end
      reset_breaker!(state.fetch("circuit_breaker")) if acknowledge
      if selected.any? { |job| raw_metadata_for(job).fetch("status") == "interrupted" } && state["interruption"]
        (state["interruption_history"] ||= []) << state.delete("interruption")
      end
      clear_dispatch_halt!(state, selected)
      FileUtils.mkdir_p(control_dir)
      File.write(pause_path, "#{timestamp}\n")
      state["status"] = "paused"
      state["updated_at"] = timestamp
      write_json(execution_path, state)
      event
    end

    def clear_dispatch_halt!(state, selected)
      halt = state["dispatch_halt"]
      return unless halt

      matching = selected.any? do |job|
        job.id == halt["job_id"] && metadata_for(job).fetch("attempt", 1) == halt["attempt"]
      end
      raise Error, "retry must include the failed job that stopped remote dispatch" unless matching

      (state["dispatch_halt_history"] ||= []) << halt
      state.delete("dispatch_halt")
    end

    def archive_attempt!(job)
      document = read_json(metadata_path(job), "job metadata")
      attempt = Integer(document.fetch("attempt", 1))
      raise Error, "invalid attempt for #{job.id}" unless attempt.positive?

      relative = File.join("attempts", job.id, "attempt-#{attempt}")
      destination = File.join(output_dir, relative)
      names = attempt_entry_names(job, attempt)
      if File.exist?(destination)
        validate_archive!(job, destination, names)
      else
        create_archive!(job, destination, names)
      end
      relative
    rescue ArgumentError, TypeError => e
      raise Error, "invalid attempt for #{job.id}: #{e.message}"
    end

    def validate_archive!(job, destination, names)
      matches = File.directory?(destination) && Dir.children(destination).sort == names.sort
      matches &&= names.all? { |name| identical_entry?(File.join(run_dir(job), name), File.join(destination, name)) }
      raise Error, "attempt archive conflicts for #{job.id}; retained evidence was not overwritten" unless matches
    end

    def create_archive!(job, destination, names)
      parent = File.dirname(destination)
      FileUtils.mkdir_p(parent)
      temporary = Dir.mktmpdir(".archive-", parent)
      names.each { |name| FileUtils.cp_r(File.join(run_dir(job), name), File.join(temporary, name)) }
      File.rename(temporary, destination)
    ensure
      FileUtils.remove_entry(temporary) if temporary && File.directory?(temporary)
    end

    def identical_entry?(source, destination)
      return File.binread(source) == File.binread(destination) if File.file?(source) && File.file?(destination)
      return false unless File.directory?(source) && File.directory?(destination)

      source_names = Dir.children(source).sort
      source_names == Dir.children(destination).sort && source_names.all? do |name|
        identical_entry?(File.join(source, name), File.join(destination, name))
      end
    end

    def attempt_entry_names(job, attempt)
      (%w[metadata.json stdout.log stderr.log import-source] + ["provider-attempt-#{attempt}"]).select do |name|
        File.exist?(File.join(run_dir(job), name))
      end
    end

    def evidence_sha256(root, names: nil)
      root = File.expand_path(root)
      paths = if names
                names.flat_map do |name|
                  entry = File.join(root, name)
                  File.directory?(entry) ? [entry, *Dir.glob(File.join(entry, "**", "*"), File::FNM_DOTMATCH)] : [entry]
                end
              else
                Dir.glob(File.join(root, "**", "*"), File::FNM_DOTMATCH)
              end
      digest = Digest::SHA256.new
      paths.select { |path| File.exist?(path) && !%w[. ..].include?(File.basename(path)) }.sort.each do |path|
        relative = path.delete_prefix("#{root}/")
        type = File.directory?(path) ? "directory" : "file"
        digest << type << "\0" << relative << "\0"
        digest << File.binread(path) << "\0" if type == "file"
      end
      digest.hexdigest
    end
  end
end
