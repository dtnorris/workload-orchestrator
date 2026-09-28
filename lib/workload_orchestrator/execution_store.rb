# frozen_string_literal: true

require "fileutils"
require "json"
require "time"
require_relative "execution_retry"

module WorkloadOrchestrator
  class ExecutionStore
    include ExecutionRetry

    CONTRACT_VERSION = "wlo-execution-state/v0.1"
    TERMINAL_JOB_STATUSES = %w[complete failed].freeze

    attr_reader :output_dir, :plan, :workdir

    def initialize(output_dir:, plan:, workdir:, workers_sha256: nil)
      @output_dir = File.expand_path(output_dir)
      @plan = plan
      @workdir = File.expand_path(workdir)
      @workers_sha256 = workers_sha256
      @mutex = Mutex.new
    end

    def prepare!
      FileUtils.mkdir_p(output_dir)
      with_lock do
        if File.file?(execution_path)
          validate_existing!
        else
          ensure_unclaimed_output!
          File.binwrite(plan_path, plan.bytes)
          File.binwrite(profile_path, plan.execution_profile.bytes) if plan.execution_profile
          write_json(execution_path, initial_execution)
        end
        rebuild_jobs_unlocked
      end
      self
    end

    def with_execution_lock
      FileUtils.mkdir_p(output_dir)
      File.open(File.join(output_dir, ".execution.lock"), File::RDWR | File::CREAT, 0o644) do |file|
        unless file.flock(File::LOCK_EX | File::LOCK_NB)
          raise Error, "execution is active; wait for the runner to exit before retrying or resuming"
        end

        yield
      end
    end

    def start!
      update_execution do |state|
        state["started_at"] ||= timestamp
        state["status"] = "running"
      end
    end

    def finish!
      update_execution { |state| state["status"] = final_status_unlocked(state) }
      rebuild_jobs!
      status
    end

    def status
      read_execution.fetch("status")
    end

    def pause!
      ensure_prepared!
      FileUtils.mkdir_p(control_dir)
      File.write(pause_path, "#{timestamp}\n")
      update_execution { |state| state["status"] = "paused" unless terminal_execution?(state) }
    end

    def clear_pause!
      File.delete(pause_path) if File.file?(pause_path)
    end

    def paused?
      File.file?(pause_path)
    end

    def circuit_tripped?
      read_execution.dig("circuit_breaker", "tripped") == true
    end

    def acknowledge_circuit_breaker!
      update_execution do |state|
        breaker = state.fetch("circuit_breaker")
        raise Error, "circuit breaker is not tripped" unless breaker.fetch("tripped")

        reset_breaker!(breaker)
        state["status"] = "pending"
      end
    end

    def terminal?(job)
      metadata = metadata_for(job)
      metadata && TERMINAL_JOB_STATUSES.include?(metadata.fetch("status"))
    end

    def metadata_for(job)
      path = metadata_path(job)
      return nil unless File.file?(path)

      document = JSON.parse(File.read(path))
      status = document.fetch("status").to_s
      allowed = TERMINAL_JOB_STATUSES + ["running"]
      raise Error, "invalid job status #{status.inspect} in #{path}" unless allowed.include?(status)

      if status == "failed" && read_execution.fetch("retry_pending", {})[job.id] == document.fetch("attempt", 1)
        document.merge("status" => "pending")
      else
        document
      end
    rescue JSON::ParserError, KeyError => e
      raise Error, "invalid job metadata #{path}: #{e.message}"
    end

    def record_running!(job:, worker:, environment_keys:)
      prior = metadata_for(job)
      attempt = prior ? Integer(prior.fetch("attempt", 0)) + 1 : 1
      run_dir = run_dir(job)
      FileUtils.mkdir_p(run_dir)
      write_json(
        metadata_path(job),
        {
          "job_id" => job.id,
          "pool_id" => job.pool_id,
          "worker" => worker.name,
          "status" => "running",
          "attempt" => attempt,
          "started_at" => timestamp,
          "argv" => job.argv,
          "environment_keys" => environment_keys.sort
        }
      )
      rebuild_jobs!
      Time.now
    end

    def record_terminal!(job:, status:, started_at:, exit_status:, error: nil)
      raise Error, "invalid terminal status #{status.inspect}" unless TERMINAL_JOB_STATUSES.include?(status)

      document = metadata_for(job) || {}
      completed_at = Time.now
      document.merge!(
        "status" => status,
        "completed_at" => completed_at.iso8601,
        "elapsed_seconds" => (completed_at - started_at).round(3),
        "exit_status" => exit_status
      )
      document["error"] = error if error
      write_json(metadata_path(job), document)
      record_breaker_result!(status)
      rebuild_jobs!
    end

    def write_logs(job, stdout, stderr)
      FileUtils.mkdir_p(run_dir(job))
      File.write(File.join(run_dir(job), "stdout.log"), stdout)
      File.write(File.join(run_dir(job), "stderr.log"), stderr)
    end

    def counts
      plan.jobs.each_with_object(Hash.new(0)) do |job, result|
        metadata = metadata_for(job)
        result[metadata ? metadata.fetch("status") : "pending"] += 1
      end
    end

    def summary
      state = read_execution
      {
        "status" => state.fetch("status"),
        "plan_id" => state.fetch("plan_id"),
        "plan_sha256" => state.fetch("plan_sha256"),
        "workdir" => state.fetch("workdir"),
        "paused" => paused?,
        "circuit_breaker" => state.fetch("circuit_breaker"),
        "counts" => counts
      }
    end

    private

    def reset_breaker!(breaker)
      breaker["generation"] = Integer(breaker.fetch("generation")) + 1
      breaker["tripped"] = false
      breaker["reason"] = nil
      breaker["consecutive_failures"] = 0
      breaker["total_failures"] = 0
      breaker["acknowledged_at"] = timestamp
    end

    def initial_execution
      state = {
        "contract_version" => CONTRACT_VERSION,
        "plan_id" => plan.id,
        "plan_sha256" => plan.sha256,
        "workdir" => workdir,
        "status" => "pending",
        "created_at" => timestamp,
        "updated_at" => timestamp,
        "circuit_breaker" => {
          "generation" => 0,
          "tripped" => false,
          "reason" => nil,
          "consecutive_failures" => 0,
          "total_failures" => 0,
          "history" => []
        }
      }
      if plan.execution_profile
        state["execution_profile_sha256"] = plan.execution_profile.sha256
        state["workers_sha256"] = @workers_sha256
      end
      state
    end

    def profile_path
      File.join(output_dir, "execution-profile.json")
    end

    def validate_existing!
      state = read_json(execution_path, "execution state")
      expected = [CONTRACT_VERSION, plan.id, plan.sha256, workdir]
      actual = [state["contract_version"], state["plan_id"], state["plan_sha256"], state["workdir"]]
      raise Error, "existing output belongs to a different execution identity" unless actual == expected
      raise Error, "frozen plan copy is missing from existing output" unless File.file?(plan_path)
      raise Error, "frozen plan bytes changed in existing output" unless File.binread(plan_path) == plan.bytes
      unless state["execution_profile_sha256"] == plan.execution_profile&.sha256 &&
             state["workers_sha256"] == @workers_sha256
        raise Error, "existing output belongs to a different execution profile or worker binding"
      end
      return unless plan.execution_profile

      unless File.file?(profile_path) && File.binread(profile_path) == plan.execution_profile.bytes
        raise Error, "frozen execution profile is missing or changed in existing output"
      end
    end

    def ensure_unclaimed_output!
      entries = Dir.children(output_dir) - %w[.state.lock .execution.lock]
      return if entries.empty?

      raise Error, "output directory is not empty and has no WLO execution state: #{output_dir}"
    end

    def record_breaker_result!(job_status)
      update_execution do |state|
        breaker = state.fetch("circuit_breaker")
        next if breaker.fetch("tripped")

        update_breaker_counters!(breaker, job_status)
        trip_breaker!(breaker) if breaker_reason(breaker)
      end
    end

    def update_breaker_counters!(breaker, job_status)
      if job_status == "failed"
        breaker["consecutive_failures"] = Integer(breaker.fetch("consecutive_failures")) + 1
        breaker["total_failures"] = Integer(breaker.fetch("total_failures")) + 1
      else
        breaker["consecutive_failures"] = 0
      end
    end

    def breaker_reason(breaker)
      policy = plan.failure_policy
      if breaker.fetch("consecutive_failures") >= policy.fetch("max_consecutive_failures")
        return "consecutive failure limit reached"
      end
      return "total failure limit reached" if breaker.fetch("total_failures") >= policy.fetch("max_total_failures")

      nil
    end

    def trip_breaker!(breaker)
      breaker["tripped"] = true
      breaker["reason"] = breaker_reason(breaker)
      breaker.fetch("history") << {
        "generation" => breaker.fetch("generation"),
        "tripped_at" => timestamp,
        "reason" => breaker.fetch("reason"),
        "consecutive_failures" => breaker.fetch("consecutive_failures"),
        "total_failures" => breaker.fetch("total_failures")
      }
    end

    def final_status_unlocked(state)
      return "paused" if paused?
      return "circuit_broken" if state.dig("circuit_breaker", "tripped")

      current = counts
      return "running" if current["running"].positive?
      return "pending" if current["pending"].positive?
      return "workload_failed" if current["failed"].positive?

      "completed"
    end

    def terminal_execution?(state)
      %w[completed workload_failed].include?(state.fetch("status").to_s)
    end

    def update_execution
      with_lock do
        state = read_json(execution_path, "execution state")
        yield state
        state["updated_at"] = timestamp
        write_json(execution_path, state)
      end
    end

    def rebuild_jobs!
      with_lock { rebuild_jobs_unlocked }
    end

    def rebuild_jobs_unlocked
      rows = plan.jobs.map do |job|
        metadata = metadata_for(job)
        {
          "job_id" => job.id,
          "pool_id" => job.pool_id,
          "status" => metadata ? metadata.fetch("status") : "pending",
          "attempt" => metadata && metadata["attempt"],
          "worker" => metadata && metadata["worker"],
          "exit_status" => metadata && metadata["exit_status"]
        }
      end
      write_json(File.join(output_dir, "jobs.json"), { "jobs" => rows })
    end

    def read_execution
      read_json(execution_path, "execution state")
    end

    def read_json(path, label)
      JSON.parse(File.read(path))
    rescue Errno::ENOENT, JSON::ParserError => e
      raise Error, "invalid #{label} #{path}: #{e.message}"
    end

    def write_json(path, value)
      atomic_write(path, "#{JSON.pretty_generate(value)}\n")
    end

    def atomic_write(path, content)
      tmp = "#{path}.tmp.#{Process.pid}.#{Thread.current.object_id}"
      FileUtils.mkdir_p(File.dirname(path))
      File.write(tmp, content)
      File.rename(tmp, path)
    ensure
      File.delete(tmp) if defined?(tmp) && tmp && File.exist?(tmp)
    end

    def with_lock
      @mutex.synchronize do
        FileUtils.mkdir_p(output_dir)
        File.open(File.join(output_dir, ".state.lock"), File::RDWR | File::CREAT, 0o644) do |file|
          file.flock(File::LOCK_EX)
          yield
        end
      end
    end

    def ensure_prepared!
      raise Error, "execution state not found in #{output_dir}" unless File.file?(execution_path)
    end

    def control_dir
      File.join(output_dir, "control")
    end

    def pause_path
      File.join(control_dir, "pause")
    end

    def execution_path
      File.join(output_dir, "execution.json")
    end

    def plan_path
      File.join(output_dir, "plan.json")
    end

    def run_dir(job)
      File.join(output_dir, "runs", job.id)
    end

    def metadata_path(job)
      File.join(run_dir(job), "metadata.json")
    end

    def timestamp
      Time.now.iso8601
    end
  end
end
