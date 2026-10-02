# frozen_string_literal: true

require "tmpdir"

module WorkloadOrchestrator
  # Retry authorization is one atomic execution-state write. Until it commits,
  # all failed or interrupted jobs stay terminal, even if an archive was already copied.
  module ExecutionRetry
    def retry_failed!(reason:, all: false, job_ids: [], acknowledge_circuit_breaker: false)
      ensure_prepared!
      with_execution_lock do
        with_lock do
          validate_existing!
          state = read_execution
          selected = retry_selection!(all, job_ids, reason)
          validate_retry_breaker!(state, acknowledge_circuit_breaker)
          archives = selected.map { |job| archive_attempt!(job) }
          commit_retry!(state, selected, archives, reason.strip, acknowledge_circuit_breaker)
          rebuild_jobs_unlocked
          selected.map(&:id)
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

    def retry_selection!(all, job_ids, reason)
      validate_retry_selection!(all, job_ids, reason)
      selected = plan.jobs.select do |job|
        all ? retryable?(job) : job_ids.include?(job.id)
      end
      raise Error, "no failed or interrupted jobs selected" if selected.empty?

      selected.each do |job|
        raise Error, "job #{job.id} is not failed or interrupted" unless retryable?(job)
      end
      selected
    end

    def retryable?(job)
      ExecutionStore::RETRYABLE_JOB_STATUSES.include?(metadata_for(job)&.fetch("status", nil))
    end

    def validate_retry_breaker!(state, acknowledge)
      tripped = state.fetch("circuit_breaker").fetch("tripped")
      raise Error, "retry requires --acknowledge-circuit-breaker: breaker is tripped" if tripped && !acknowledge
      raise Error, "circuit breaker is not tripped" if acknowledge && !tripped
    end

    def commit_retry!(state, selected, archives, reason, acknowledge)
      event = {
        "at" => timestamp,
        "reason" => reason,
        "jobs" => selected.zip(archives).map do |job, archive|
          { "job_id" => job.id, "attempt" => metadata_for(job).fetch("attempt", 1), "archive" => archive }
        end,
        "breaker_acknowledged" => acknowledge,
        "breaker_before" => state.fetch("circuit_breaker").dup
      }
      (state["retry_history"] ||= []) << event
      event.fetch("jobs").each do |row|
        (state["retry_pending"] ||= {})[row.fetch("job_id")] = row.fetch("attempt")
      end
      reset_breaker!(state.fetch("circuit_breaker")) if acknowledge
      if selected.any? { |job| metadata_for(job).fetch("status") == "interrupted" } && state["interruption"]
        (state["interruption_history"] ||= []) << state.delete("interruption")
      end
      clear_dispatch_halt!(state, selected)
      FileUtils.mkdir_p(control_dir)
      File.write(pause_path, "#{timestamp}\n")
      state["status"] = "paused"
      state["updated_at"] = timestamp
      write_json(execution_path, state)
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
      names = (%w[metadata.json stdout.log stderr.log import-source] + ["provider-attempt-#{attempt}"]).select do |name|
        File.exist?(File.join(run_dir(job), name))
      end
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
  end
end
