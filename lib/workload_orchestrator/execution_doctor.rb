# frozen_string_literal: true

require "json"

module WorkloadOrchestrator
  # Read-only job explanation over the retained execution report, FO-04 pool
  # status and per-attempt evidence. It does not own scheduling or provider state.
  class ExecutionDoctor
    DEFAULT_LOG_LINES = 15
    MAX_LOG_LINES = 200
    MAX_TAIL_BYTES = 262_144
    STAGES = %w[
      dependency_waiting worker_discovery registry_validation worker_eligibility
      worker_capacity paused circuit_breaker dispatch command_launch
      command_execution command_failure command_exception foreground_cancellation
      worker_generation owner_process healthy unknown
    ].freeze
    ACTIONS = %w[
      inspect_registry inspect_pod wait_for_busy_worker resume_paused_execution
      inspect_triggering_failure inspect_dispatch_halt inspect_job_logs
      review_interrupted_attempt inspect_worker_generation inspect_manager
      inspect_execution_status
    ].freeze
    WAITING = {
      "NO_RUNNABLE_WORK" => ["dependency_waiting", "Pending dependencies are not terminal.", "inspect_execution_status"],
      "NO_ACCEPTED_REGISTRY_SNAPSHOT" => ["worker_discovery", "No accepted registry snapshot is retained.", "inspect_registry"],
      "REGISTRY_INVALID_OR_STALE" => ["registry_validation", nil, "inspect_registry"],
      "NO_COMPATIBLE_READY_WORKERS" => ["worker_eligibility", "No READY worker matches this pool.", "inspect_registry"],
      "READY_WORKERS_INCOMPATIBLE" => ["worker_eligibility", "READY workers exist, but none matches this pool.", "inspect_registry"],
      "WORKERS_NOT_READY" => ["worker_eligibility", "Compatible workers are retained as NOT_READY.", "inspect_pod"],
      "ALL_COMPATIBLE_WORKERS_BUSY" => ["worker_capacity", "All compatible READY workers are busy.", "wait_for_busy_worker"],
      "PAUSED" => ["paused", "Execution is paused; new dispatch is stopped.", "resume_paused_execution"],
      "CIRCUIT_BREAKER" => ["circuit_breaker", "The circuit breaker stopped new dispatch.", "inspect_triggering_failure"],
      "DISPATCH_HALTED" => ["dispatch", "Dispatch is halted by retained execution evidence.", "inspect_dispatch_halt"],
      "READY_TO_DISPATCH" => ["dispatch", "The job is eligible for dispatch.", "inspect_execution_status"]
    }.freeze

    def initialize(plan:, output:, clock: -> { Time.now.utc })
      @plan = plan
      @root = File.expand_path(output)
      @clock = clock
    end

    def diagnose(handle)
      job = resolve(handle)
      report = ExecutionReport.new(plan: @plan, output: @root, clock: @clock).document
      row = report.fetch("jobs").find { |candidate| candidate.fetch("job_id") == job.id }
      metadata = read_metadata(job.id)
      result = base_result(job, handle, row)
      classify!(result, report, row, metadata)
      validate!(result)
      result
    end

    def logs(handle, lines: DEFAULT_LOG_LINES)
      job = resolve(handle)
      count = Integer(lines)
      raise Error, "--lines must be between 1 and #{MAX_LOG_LINES}" unless count.between?(1, MAX_LOG_LINES)

      run_root = File.join(@root, "runs", job.id)
      {
        "subject" => subject(job, handle),
        "metadata" => compact_metadata(read_metadata(job.id)),
        "stderr" => log_evidence(File.join(run_root, "stderr.log"), count),
        "stdout" => log_evidence(File.join(run_root, "stdout.log"), count)
      }
    rescue ArgumentError, TypeError
      raise Error, "--lines must be between 1 and #{MAX_LOG_LINES}"
    end

    def self.short_handle(job_id)
      match = job_id.to_s.match(/(?:\A|-)adv(\d+)-(.+)\z/)
      return job_id.to_s unless match

      "#{match[1]}-#{match[2].split('-').last}"
    end

    private

    def resolve(handle)
      exact = @plan.jobs.find { |job| job.id == handle.to_s }
      return exact if exact

      matches = @plan.jobs.select { |job| self.class.short_handle(job.id) == handle.to_s }
      raise Error, "unknown job handle #{handle.inspect}" if matches.empty?
      if matches.length > 1
        raise Error, "ambiguous job handle #{handle.inspect}: #{matches.map(&:id).sort.join(', ')}"
      end
      matches.first
    end

    def base_result(job, supplied, row)
      {
        "subject" => subject(job, supplied), "stage" => "unknown",
        "status" => row.fetch("status"),
        "summary" => "Retained evidence does not identify one failing stage.",
        "evidence" => [], "next_action" => action("inspect_execution_status")
      }
    end

    def subject(job, supplied)
      { "type" => "job", "handle" => self.class.short_handle(job.id), "id" => job.id,
        "supplied_handle" => supplied.to_s }
    end

    def classify!(result, report, row, metadata)
      case row.fetch("status")
      when "complete" then healthy!(result, row)
      when "interrupted" then interrupted!(result, metadata)
      when "failed" then failed!(result, report, row, metadata)
      when "running" then running!(result, row, metadata)
      else pending!(result, report, row)
      end
    end

    def healthy!(result, row)
      result.merge!(
        "stage" => "healthy", "status" => "healthy", "summary" => "Job completed successfully.",
        "evidence" => [fact("attempt", "attempt" => row["attempt"], "exit_status" => row["exit_status"])],
        "next_action" => nil
      )
    end

    def interrupted!(result, metadata)
      evidence = metadata && metadata["evidence"] || {}
      result.merge!(
        "stage" => "foreground_cancellation", "summary" => "Foreground execution was cancelled.",
        "evidence" => [fact("interruption", "signal" => evidence["signal"],
                            "termination_mode" => evidence["termination_mode"],
                            "term_signal" => metadata && metadata["term_signal"],
                            "attempt" => metadata && metadata["attempt"])],
        "next_action" => action("review_interrupted_attempt")
      )
    end

    def failed!(result, report, row, metadata)
      evidence_kind = metadata&.dig("evidence", "kind")
      if evidence_kind == "remote_in_doubt" || generation_evidence?(metadata)
        return result.merge!(
          "stage" => "worker_generation", "summary" => metadata["error"] || "Worker generation evidence is in doubt.",
          "evidence" => [fact("worker_generation", "worker" => row["worker"], "attempt" => row["attempt"],
                          "kind" => evidence_kind)], "next_action" => action("inspect_worker_generation")
        )
      end
      if report.dig("circuit_breaker", "tripped")
        return result.merge!(
          "stage" => "circuit_breaker", "summary" => "The circuit breaker stopped new dispatch.",
          "evidence" => [fact("circuit_breaker", "reason" => report.dig("circuit_breaker", "reason"),
                          "job_id" => row.fetch("job_id"), "attempt" => row["attempt"])],
          "next_action" => action("inspect_job_logs")
        )
      end

      exception = metadata && metadata["exit_status"].nil? && !metadata["error"].to_s.empty?
      result.merge!(
        "stage" => exception ? "command_exception" : "command_failure",
        "summary" => exception ? "Command raised an execution exception." : "Command exited #{row['exit_status'].inspect}.",
        "evidence" => [fact("attempt", "attempt" => row["attempt"], "worker" => row["worker"],
                        "exit_status" => row["exit_status"], "error" => metadata && metadata["error"])],
        "next_action" => action("inspect_job_logs")
      )
    end

    def running!(result, row, metadata)
      result.merge!(
        "stage" => metadata ? "command_execution" : "command_launch",
        "summary" => metadata ? "Command is running." : "Command launch is being recorded.",
        "evidence" => [fact("attempt", "attempt" => row["attempt"], "worker" => row["worker"])],
        "next_action" => action("inspect_execution_status")
      )
    end

    def pending!(result, report, row)
      if report.fetch("status") == "owner_crashed"
        return result.merge!(
          "stage" => "owner_process", "summary" => "The execution owner is not active.",
          "evidence" => [fact("manager", "manager" => report["manager"],
                              "resource_disposition" => report["resource_disposition"])],
          "next_action" => action("inspect_manager")
        )
      end

      pool = report.fetch("pool_status").find { |candidate| candidate.fetch("pool_id") == row.fetch("pool_id") }
      return unless pool

      reason = pool.fetch("reason")
      stage, summary, action_id = WAITING.fetch(reason, ["unknown", nil, "inspect_execution_status"])
      summary ||= "Registry evidence is invalid or stale: #{pool['detail'] || 'no accepted detail'}"
      evidence = {
        "pool_id" => pool.fetch("pool_id"), "reason" => reason,
        "registry_revision" => pool["registry_revision"], "workers" => pool.fetch("workers"),
        "relevant_worker_ids" => pool.fetch("relevant_worker_ids")
      }
      evidence["detail"] = pool["detail"] if pool["detail"]
      action_extra = {}
      if action_id == "inspect_pod" && pool.fetch("relevant_worker_ids").length == 1
        action_extra["worker_id"] = pool.fetch("relevant_worker_ids").first
      end
      result.merge!("stage" => stage, "summary" => summary,
                    "evidence" => [fact("pool_status", evidence)],
                    "next_action" => action(action_id, action_extra))
    end

    def generation_evidence?(metadata)
      value = metadata && metadata["error"].to_s
      value.include?("generation") || value.include?("in doubt") || value.include?("worker disappeared")
    end

    def action(id, extra = {})
      raise Error, "invalid diagnostic action #{id.inspect}" unless ACTIONS.include?(id)

      { "action" => id }.merge(extra)
    end

    def fact(kind, values)
      { "kind" => kind }.merge(values.compact)
    end

    def validate!(result)
      raise Error, "invalid diagnostic stage" unless STAGES.include?(result.fetch("stage"))
      action_row = result["next_action"]
      if result.fetch("status") == "healthy"
        raise Error, "healthy diagnosis cannot have a next action" if action_row
      elsif !action_row || !ACTIONS.include?(action_row.fetch("action"))
        raise Error, "unhealthy diagnosis must have exactly one next action"
      end
    end

    def read_metadata(job_id)
      path = File.join(@root, "runs", job_id, "metadata.json")
      return nil unless File.file?(path)

      JSON.parse(File.read(path))
    rescue JSON::ParserError => e
      raise Error, "invalid job metadata #{path}: #{e.message}"
    end

    def compact_metadata(metadata)
      return nil unless metadata

      metadata.slice("status", "attempt", "worker", "exit_status", "term_signal", "failure_class", "error", "evidence")
    end

    def log_evidence(path, lines)
      { "path" => path.delete_prefix("#{@root}/"), "lines" => redact(tail_lines(path, lines)) }
    end

    def tail_lines(path, count)
      return [] unless File.file?(path)

      bytes = +""
      File.open(path, "rb") do |file|
        position = file.size
        while position.positive? && bytes.count("\n") <= count && bytes.bytesize < MAX_TAIL_BYTES
          size = [4096, position, MAX_TAIL_BYTES - bytes.bytesize].min
          position -= size
          file.seek(position)
          bytes.prepend(file.read(size))
        end
      end
      bytes.lines.last(count).map(&:chomp)
    rescue Errno::ENOENT
      []
    end

    def redact(lines)
      names = /(RUNPOD_API_KEY|OPENAI_API_KEY|ANTHROPIC_API_KEY|GOOGLE_API_KEY|AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY)/i
      lines.map do |line|
        line.gsub(/(#{names.source}\s*[=:]\s*)\S+/i, "\\1[REDACTED]")
            .gsub(/(Authorization:\s*Bearer\s+)\S+/i, "\\1[REDACTED]")
      end
    end
  end
end
