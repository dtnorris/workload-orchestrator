# frozen_string_literal: true

require "tmpdir"

module WorkloadOrchestrator
  # Retry authorization is one atomic execution-state write. Until it commits,
  # all failed jobs stay terminal, even if an archive was already copied.
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
        all ? metadata_for(job)&.fetch("status") == "failed" : job_ids.include?(job.id)
      end
      raise Error, "no failed jobs selected" if selected.empty?

      selected.each do |job|
        raise Error, "job #{job.id} is not failed" unless metadata_for(job)&.fetch("status") == "failed"
      end
      selected
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
      FileUtils.mkdir_p(control_dir)
      File.write(pause_path, "#{timestamp}\n")
      state["status"] = "paused"
      state["updated_at"] = timestamp
      write_json(execution_path, state)
    end

    def archive_attempt!(job)
      document = read_json(metadata_path(job), "job metadata")
      attempt = Integer(document.fetch("attempt", 1))
      raise Error, "invalid attempt for #{job.id}" unless attempt.positive?

      relative = File.join("attempts", job.id, "attempt-#{attempt}")
      destination = File.join(output_dir, relative)
      names = %w[metadata.json stdout.log stderr.log].select { |name| File.file?(File.join(run_dir(job), name)) }
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
      matches &&= names.all? do |name|
        File.file?(File.join(destination, name)) &&
          File.binread(File.join(destination, name)) == File.binread(File.join(run_dir(job), name))
      end
      raise Error, "attempt archive conflicts for #{job.id}; retained evidence was not overwritten" unless matches
    end

    def create_archive!(job, destination, names)
      parent = File.dirname(destination)
      FileUtils.mkdir_p(parent)
      temporary = Dir.mktmpdir(".archive-", parent)
      names.each { |name| FileUtils.cp(File.join(run_dir(job), name), File.join(temporary, name)) }
      File.rename(temporary, destination)
    ensure
      FileUtils.remove_entry(temporary) if temporary && File.directory?(temporary)
    end
  end
end
