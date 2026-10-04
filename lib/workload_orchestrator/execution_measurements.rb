# frozen_string_literal: true

require "json"
require "time"

module WorkloadOrchestrator
  # Read-only measurements derived only from WLO-owned retained execution evidence.
  class ExecutionMeasurements
    CONTRACT_VERSION = "wlo-execution-measurements/v0.1"
    TERMINAL_EXECUTION_STATUSES = %w[completed workload_failed interrupted circuit_broken infrastructure_failed].freeze

    def initialize(plan:, output:, clock: -> { Time.now.utc })
      @plan = plan
      @root = File.expand_path(output)
      @clock = clock
    end

    def document
      measured_at = utc_now
      state = read_json(File.join(@root, "execution.json"))
      validate_identity!(state)
      jobs = read_json(File.join(@root, "jobs.json")).fetch("jobs")
      attempts = load_attempts
      window = execution_window(state, measured_at)
      samples = classify_attempts(attempts, measured_at)
      work = work_counts(jobs, state)
      throughput = throughput_measurement(samples, window)

      {
        "contract_version" => CONTRACT_VERSION,
        "read_only" => true,
        "measured_at_utc" => measured_at.iso8601,
        "execution" => {
          "plan_id" => state.fetch("plan_id"),
          "plan_sha256" => state.fetch("plan_sha256"),
          "status" => state.fetch("status"),
          "window" => window
        },
        "work" => work,
        "samples" => sample_counts(samples),
        "command_execution" => command_measurements(samples),
        "queue_wait" => unavailable(
          "retained execution evidence does not separate eligibility, capacity, dependency, and paused wait intervals"
        ),
        "throughput" => throughput,
        "utilization" => utilization_measurement(samples, window),
        "eta" => eta_measurement(work, throughput, measured_at)
      }
    rescue SystemCallError, JSON::ParserError, KeyError, ArgumentError, TypeError => e
      raise Error, "cannot measure execution: #{e.message}"
    end

    private

    def validate_identity!(state)
      return if state["plan_id"] == @plan.id && state["plan_sha256"] == @plan.sha256

      raise Error, "output execution identity does not match plan"
    end

    def execution_window(state, measured_at)
      start_value = state["started_at"]
      return unavailable("execution has not started") unless start_value

      started = parse_time(start_value)
      terminal = TERMINAL_EXECUTION_STATUSES.include?(state.fetch("status"))
      finish_value = state["last_run_finished_at"] if terminal || state.fetch("status") == "paused"
      ended = finish_value ? parse_time(finish_value) : measured_at
      ended = started if ended < started
      {
        "status" => "available",
        "started_at_utc" => started.iso8601,
        "ended_at_utc" => ended.iso8601,
        "end_basis" => finish_value ? "retained_last_run_finish" : "measurement_clock",
        "elapsed_seconds" => round_seconds(ended - started)
      }
    end

    def load_attempts
      paths = Dir.glob(File.join(@root, "attempts", "*", "attempt-*", "metadata.json"))
      paths.concat(Dir.glob(File.join(@root, "runs", "*", "metadata.json")))
      paths.sort.map { |path| read_json(path).merge("_path" => path) }
    end

    def classify_attempts(attempts, measured_at)
      attempts.map do |attempt|
        status = attempt.fetch("status").to_s
        timing = attempt_timing(attempt, measured_at)
        { "status" => status, "timing" => timing, "attempt" => attempt }
      end
    end

    def attempt_timing(attempt, measured_at)
      duration = numeric_duration(attempt["elapsed_seconds"])
      started = parse_optional_time(attempt["started_at"])
      finished = parse_optional_time(attempt["completed_at"])
      finished ||= measured_at if attempt.fetch("status") == "running"
      duration ||= round_seconds(finished - started) if started && finished && finished >= started
      {
        "duration_seconds" => duration,
        "started_at" => started,
        "finished_at" => finished,
        "interval_valid" => !started.nil? && !finished.nil? && finished >= started
      }
    rescue ArgumentError
      { "duration_seconds" => nil, "started_at" => nil, "finished_at" => nil, "interval_valid" => false }
    end

    def work_counts(jobs, state)
      counts = %w[pending running complete failed interrupted].to_h { |status| [status, 0] }
      jobs.each { |job| counts[job.fetch("status")] = counts.fetch(job.fetch("status"), 0) + 1 }
      retry_pending = state.fetch("retry_pending", {}).length
      {
        "total_jobs" => jobs.length,
        "pending" => counts.fetch("pending"),
        "running" => counts.fetch("running"),
        "completed" => counts.fetch("complete"),
        "failed" => counts.fetch("failed"),
        "interrupted" => counts.fetch("interrupted"),
        "retry_pending" => retry_pending,
        "remaining_jobs" => counts.fetch("pending") + counts.fetch("running")
      }
    end

    def sample_counts(samples)
      usable = ->(status) { samples.count { |row| row.fetch("status") == status && row.dig("timing", "duration_seconds") } }
      {
        "completed_successful_attempts" => usable.call("complete"),
        "failed_attempts" => samples.count { |row| row.fetch("status") == "failed" },
        "interrupted_attempts" => samples.count { |row| row.fetch("status") == "interrupted" },
        "currently_running_attempts" => samples.count { |row| row.fetch("status") == "running" },
        "attempts_excluded_missing_or_invalid_timing" => samples.count { |row| row.dig("timing", "duration_seconds").nil? }
      }
    end

    def command_measurements(samples)
      {
        "definition" => "retained per-attempt command duration; running attempts are observed through the measurement clock",
        "successful" => duration_summary(samples, "complete"),
        "failed" => duration_summary(samples, "failed"),
        "interrupted" => duration_summary(samples, "interrupted"),
        "running_observed" => duration_summary(samples, "running"),
        "all_observed" => duration_summary(samples),
        "active_intervals" => public_attempt_intervals(samples)
      }
    end

    def public_attempt_intervals(samples)
      samples.filter_map do |row|
        timing = row.fetch("timing")
        next unless timing.fetch("interval_valid")

        identity = row.dig("attempt", "worker_execution_identity")
        {
          "job_id" => row.dig("attempt", "job_id"),
          "attempt" => row.dig("attempt", "attempt"),
          "status" => row.fetch("status"),
          "started_at_utc" => timing.fetch("started_at").iso8601,
          "ended_at_utc" => timing.fetch("finished_at").iso8601,
          "worker_identity" => identity && identity.slice("worker_id", "generation_id")
        }
      end
    end

    def duration_summary(samples, status = nil)
      selected = status ? samples.select { |row| row.fetch("status") == status } : samples
      values = selected.filter_map { |row| row.dig("timing", "duration_seconds") }
      return unavailable("no valid duration samples").merge("sample_count" => 0) if values.empty?

      {
        "status" => "available",
        "sample_count" => values.length,
        "total_seconds" => round_seconds(values.sum),
        "mean_seconds" => round_seconds(values.sum / values.length)
      }
    end

    def throughput_measurement(samples, window)
      successful = samples.count do |row|
        row.fetch("status") == "complete" && row.dig("timing", "duration_seconds")
      end
      return unavailable("no completed successful attempt samples").merge("sample_count" => 0) if successful.zero?
      return unavailable("execution wall-clock window is unavailable").merge("sample_count" => successful) unless window["status"] == "available"

      seconds = Float(window.fetch("elapsed_seconds"))
      return unavailable("execution wall-clock denominator is zero").merge("sample_count" => successful) unless seconds.positive?

      {
        "status" => "available",
        "definition" => "completed successful jobs per execution wall-clock hour from first execution start through the measurement-window end",
        "sample_count" => successful,
        "denominator_seconds" => round_seconds(seconds),
        "jobs_per_hour" => (successful * 3600.0 / seconds).round(6)
      }
    end

    def utilization_measurement(samples, window)
      return unavailable("execution wall-clock window is unavailable").merge("sample_count" => 0) unless window["status"] == "available"

      window_start = parse_time(window.fetch("started_at_utc"))
      window_end = parse_time(window.fetch("ended_at_utc"))
      intervals = samples.filter_map do |row|
        timing = row.fetch("timing")
        next unless timing.fetch("interval_valid")

        left = [timing.fetch("started_at"), window_start].max
        right = [timing.fetch("finished_at"), window_end].min
        [left, right] if right > left
      end
      denominator = window_end - window_start
      return unavailable("execution wall-clock denominator is zero").merge("sample_count" => intervals.length) unless denominator.positive?

      busy = interval_union_seconds(intervals)
      {
        "status" => "available",
        "definition" => "fraction of the execution wall-clock window containing at least one observed active command attempt; not worker-slot or provider utilization",
        "sample_count" => intervals.length,
        "busy_wall_seconds" => round_seconds(busy),
        "observable_wall_seconds" => round_seconds(denominator),
        "busy_wall_fraction" => (busy / denominator).round(6)
      }
    end

    def eta_measurement(work, throughput, measured_at)
      remaining = work.fetch("remaining_jobs")
      if remaining.zero?
        return {
          "status" => "available", "kind" => "estimate", "basis" => "no_remaining_jobs",
          "remaining_job_count" => 0, "sample_count" => throughput.fetch("sample_count", 0),
          "estimated_remaining_seconds" => 0.0, "estimated_completion_at_utc" => measured_at.iso8601
        }
      end
      unless throughput["status"] == "available"
        return unavailable("zero usable completed successful samples").merge(
          "kind" => "estimate", "remaining_job_count" => remaining,
          "sample_count" => throughput.fetch("sample_count", 0)
        )
      end

      rate = Float(throughput.fetch("jobs_per_hour"))
      seconds = remaining * 3600.0 / rate
      {
        "status" => "available",
        "kind" => "estimate",
        "basis" => "remaining jobs divided by measured successful wall-clock throughput",
        "remaining_job_count" => remaining,
        "sample_count" => throughput.fetch("sample_count"),
        "throughput_jobs_per_hour" => rate,
        "estimated_remaining_seconds" => round_seconds(seconds),
        "estimated_completion_at_utc" => (measured_at + seconds).iso8601
      }
    end

    def interval_union_seconds(intervals)
      intervals.sort_by(&:first).reduce([]) do |merged, interval|
        if merged.empty? || interval.first > merged.last.last
          merged << interval.dup
        else
          merged.last[1] = [merged.last.last, interval.last].max
        end
        merged
      end.sum { |left, right| right - left }
    end

    def numeric_duration(value)
      return nil if value.nil?

      number = Float(value)
      number.finite? && !number.negative? ? number : nil
    rescue ArgumentError, TypeError
      nil
    end

    def parse_optional_time(value)
      value && parse_time(value)
    rescue ArgumentError
      nil
    end

    def parse_time(value)
      Time.iso8601(value.to_s).utc
    end

    def utc_now
      value = @clock.call
      value = Time.parse(value.to_s) unless value.is_a?(Time)
      value.utc
    end

    def round_seconds(value)
      Float(value).round(6)
    end

    def unavailable(reason)
      { "status" => "unavailable", "reason" => reason }
    end

    def read_json(path)
      JSON.parse(File.binread(path))
    end
  end
end
