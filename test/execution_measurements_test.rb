# frozen_string_literal: true

require_relative "test_helper"

class ExecutionMeasurementsTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2026-10-04T12:00:00Z")

  def setup
    @root = Dir.mktmpdir("wlo-measurements-")
    @output = File.join(@root, "output")
    FileUtils.mkdir_p(@output)
    @plan_path = write_plan(@root, jobs: %w[one two three four five].map { |id| fixture_job(id) })
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_empty_new_execution_has_counts_and_unavailable_rates
    write_execution("status" => "pending", "created_at" => "2026-10-04T11:00:00Z")
    write_jobs(%w[pending pending pending pending pending])

    result = measure
    assert_equal "wlo-execution-measurements/v0.1", result.fetch("contract_version")
    assert_equal 5, result.dig("work", "total_jobs")
    assert_equal 5, result.dig("work", "pending")
    assert_equal "unavailable", result.dig("execution", "window", "status")
    assert_equal "unavailable", result.dig("throughput", "status")
    assert_equal "unavailable", result.dig("eta", "status")
    assert_equal "unavailable", result.dig("queue_wait", "status")
  end

  def test_partial_execution_measures_attempt_buckets_throughput_utilization_and_eta
    write_execution(
      "status" => "running", "started_at" => "2026-10-04T11:00:00Z",
      "last_run_started_at" => "2026-10-04T11:00:00Z",
      "retry_pending" => { "four" => 1 }
    )
    write_jobs(%w[complete complete running pending failed])
    write_attempt("one", "status" => "complete", "started_at" => "2026-10-04T11:05:00Z",
                  "completed_at" => "2026-10-04T11:15:00Z", "elapsed_seconds" => 600,
                  "worker_execution_identity" => { "worker_id" => "worker-1", "generation_id" => "gen-1" })
    write_attempt("two", "status" => "complete", "started_at" => "2026-10-04T11:10:00Z",
                  "completed_at" => "2026-10-04T11:30:00Z", "elapsed_seconds" => 1200)
    write_attempt("three", "status" => "running", "started_at" => "2026-10-04T11:50:00Z")
    write_attempt("five", "status" => "failed", "started_at" => "2026-10-04T11:35:00Z",
                  "completed_at" => "2026-10-04T11:45:00Z", "elapsed_seconds" => 600)
    write_archived_attempt("four", 1, "status" => "interrupted",
                           "started_at" => "2026-10-04T11:31:00Z",
                           "completed_at" => "2026-10-04T11:34:00Z", "elapsed_seconds" => 180)

    result = measure
    assert_equal 2, result.dig("samples", "completed_successful_attempts")
    assert_equal 1, result.dig("samples", "failed_attempts")
    assert_equal 1, result.dig("samples", "interrupted_attempts")
    assert_equal 1, result.dig("samples", "currently_running_attempts")
    assert_equal 2.0, result.dig("throughput", "jobs_per_hour")
    assert_equal 2, result.dig("throughput", "sample_count")
    assert_equal 2, result.dig("eta", "remaining_job_count")
    assert_equal 3600.0, result.dig("eta", "estimated_remaining_seconds")
    assert_equal "2026-10-04T13:00:00Z", result.dig("eta", "estimated_completion_at_utc")
    assert_equal 2880.0, result.dig("utilization", "busy_wall_seconds")
    assert_equal 0.8, result.dig("utilization", "busy_wall_fraction")
    assert_equal 1, result.dig("work", "retry_pending")
    one = result.dig("command_execution", "active_intervals").find { |row| row["job_id"] == "one" }
    assert_equal({ "worker_id" => "worker-1", "generation_id" => "gen-1" }, one.fetch("worker_identity"))
  end

  def test_complete_execution_has_zero_eta_and_uses_retained_finish
    write_execution(
      "status" => "completed", "started_at" => "2026-10-04T10:00:00Z",
      "last_run_finished_at" => "2026-10-04T11:00:00Z"
    )
    write_jobs(%w[complete complete complete complete complete])
    %w[one two three four five].each_with_index do |id, index|
      write_attempt(id, "status" => "complete", "started_at" => "2026-10-04T10:00:00Z",
                    "completed_at" => "2026-10-04T10:10:00Z", "elapsed_seconds" => 600 + index)
    end

    result = measure
    assert_equal "retained_last_run_finish", result.dig("execution", "window", "end_basis")
    assert_equal 0.0, result.dig("eta", "estimated_remaining_seconds")
    assert_equal 0, result.dig("eta", "remaining_job_count")
  end

  def test_historical_missing_timing_is_counted_without_rewrite
    write_execution("status" => "workload_failed", "started_at" => "2026-10-04T10:00:00Z",
                    "last_run_finished_at" => "2026-10-04T11:00:00Z")
    write_jobs(%w[failed interrupted pending pending pending])
    write_attempt("one", "status" => "failed")
    write_attempt("two", "status" => "interrupted", "elapsed_seconds" => "invalid")
    before = retained_bytes

    result = measure
    assert_equal 2, result.dig("samples", "attempts_excluded_missing_or_invalid_timing")
    assert_equal "unavailable", result.dig("command_execution", "failed", "status")
    assert_equal before, retained_bytes
  end

  def test_cli_emits_the_machine_contract
    write_execution("status" => "pending")
    write_jobs(%w[pending pending pending pending pending])
    out = StringIO.new
    err = StringIO.new

    code = WorkloadOrchestrator::CLI.new(
      ["measurements", @plan_path, "--output", @output], out:, err:
    ).run

    assert_equal 0, code, err.string
    assert_equal "wlo-execution-measurements/v0.1", JSON.parse(out.string).fetch("contract_version")
  end

  private

  def measure
    WorkloadOrchestrator::ExecutionMeasurements.new(
      plan: @plan, output: @output, clock: -> { NOW }
    ).document
  end

  def write_execution(extra)
    write_json("execution.json", {
      "contract_version" => WorkloadOrchestrator::ExecutionStore::CONTRACT_VERSION,
      "plan_id" => @plan.id, "plan_sha256" => @plan.sha256,
      "status" => "pending", "circuit_breaker" => {}
    }.merge(extra))
  end

  def write_jobs(statuses)
    rows = %w[one two three four five].zip(statuses).map do |id, status|
      { "job_id" => id, "status" => status }
    end
    write_json("jobs.json", "jobs" => rows)
  end

  def write_attempt(job_id, values)
    write_json(File.join("runs", job_id, "metadata.json"), { "job_id" => job_id }.merge(values))
  end

  def write_archived_attempt(job_id, attempt, values)
    write_json(File.join("attempts", job_id, "attempt-#{attempt}", "metadata.json"),
               { "job_id" => job_id, "attempt" => attempt }.merge(values))
  end

  def write_json(name, value)
    path = File.join(@output, name)
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, JSON.pretty_generate(value) + "\n")
  end

  def retained_bytes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .sort.to_h { |path| [path.delete_prefix(@output), File.binread(path)] }
  end
end
