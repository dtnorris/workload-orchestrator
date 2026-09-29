# frozen_string_literal: true

require_relative "test_helper"
require "minitest/mock"
require_relative "support/operator_fixtures"

class ExecutionReportTest < Minitest::Test
  include WloTestSupport
  include OperatorFixtures

  def setup
    @root = Dir.mktmpdir("wlo-report-")
    @output = File.join(@root, "output")
    FileUtils.mkdir_p(@output)
    @plan = write_plan(@root, jobs: [job("one", code: "puts :ok")])
    plan = WorkloadOrchestrator::Plan.load(@plan)
    @report = WorkloadOrchestrator::ExecutionReport.new(plan: plan, output: @output)
    @state = {
      "plan_id" => plan.id, "plan_sha256" => plan.sha256, "status" => "running",
      "execution_profile_sha256" => "profile digest", "workers_sha256" => "workers digest",
      "last_run_started_at" => "2026-09-28T12:00:00Z",
      "circuit_breaker" => { "tripped" => false, "reason" => nil }
    }
    write_json("execution.json", @state)
    write_jobs(%w[complete failed running pending])
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_document_counts_progress_identity_and_latest_manager_record
    id = "12345678-1234-1234-1234-123456789abc"
    pointer = { "launch_id" => id, "status" => "running" }
    finished = pointer.merge("status" => "error", "pid" => 123, "exit_status" => 1,
                             "finished_at" => "2026-09-28T12:00:07Z")
    write_json("manager.json", pointer)
    write_json("manager/#{id}.json", finished)
    document = @report.document
    assert_equal({ "complete" => 1, "failed" => 1, "running" => 1, "pending" => 1 }, document.fetch("counts"))
    assert_equal 4, document.fetch("total")
    assert_equal 2, document.fetch("terminal")
    assert_equal 50.0, document.fetch("progress_percent")
    assert_equal finished, document.fetch("manager")
    assert_equal @state, document.slice(*@state.keys)
    refute document.fetch("executor_active")
    refute document.fetch("paused")
  end

  def test_progress_rounds_and_includes_zero_counts_without_a_manager
    write_jobs(%w[complete failed pending])
    document = @report.document
    assert_equal 66.7, document.fetch("progress_percent")
    assert_equal 0, document.dig("counts", "running")
    assert_nil document.fetch("manager")
    write_jobs([])
    assert_equal 100.0, @report.document.fetch("progress_percent")
    assert_equal 0, @report.document.fetch("terminal")
  end

  def test_remote_owner_crash_during_cleanup_is_reported_without_claiming_absence
    write_json("execution-profile.json", "pools" => [{ "backend" => "rpof" }])
    @state["status"] = "cleanup_pending"
    @state["resource_disposition"] = { "phase" => "awaiting_terminal_cleanup" }
    write_json("execution.json", @state)

    report = @report.document
    assert_equal "owner_crashed", report.fetch("status")
    assert_equal "awaiting_terminal_cleanup", report.dig("resource_disposition", "phase")
  end

  def test_human_report_shows_stale_jobs_controls_finished_timing_and_manager_paths
    @state.merge!("last_run_finished_at" => "2026-09-28T12:00:07Z",
                  "circuit_breaker" => { "tripped" => true, "reason" => "failure limit reached" })
    write_json("execution.json", @state)
    write_pause
    id = "12345678-1234-1234-1234-123456789abc"
    record = { "launch_id" => id, "pid" => 123, "status" => "error", "exit_status" => 1,
               "log_path" => File.join(@output, "manager", "#{id}.log"),
               "record_path" => File.join(@output, "manager", "#{id}.json") }
    write_json("manager.json", record)
    write_json("manager/#{id}.json", record)
    text = human
    assert_includes text, "Plan: fixture-plan"
    assert_includes text, "Execution: running | executor inactive"
    assert_includes text, "Progress: [2/4] terminal (50.0%)"
    assert_includes text, "Jobs: complete=1 failed=1 running=1 pending=1"
    assert_includes text, "Pause: requested\n"
    assert_includes text, "Breaker: failure limit reached"
    assert_includes text, "Last run started: 2026-09-28T12:00:00Z"
    assert_includes text, "Last run finished: 2026-09-28T12:00:07Z"
    assert_includes text, "Last run elapsed: 7s"
    assert_includes text, "Running: job-2 worker=local attempt=2 exit=nil"
    assert_includes text, "Failed: job-1 worker=local attempt=2 exit=3"
    assert_includes text, "reason=worker_disappeared"
    assert_includes text, "No executor holds the lock; running records may be stale."
    assert_includes text, "Last manager: PID 123 | recorded error | exit=1"
    assert_includes text, "Manager log: #{record.fetch('log_path')}"
    assert_includes text, "Manager record: #{record.fetch('record_path')}"
    assert_includes text, "Evidence: #{@output}"
  end

  def test_live_lock_controls_elapsed_time_and_stale_warning
    write_pause
    # Separate file descriptions exercise the actual flock check without a
    # manager process; cross-process exclusion stays in OperatorUxTest.
    File.open(File.join(@output, ".execution.lock"), "w") do |lock|
      assert lock.flock(File::LOCK_EX | File::LOCK_NB)
      assert @report.document.fetch("executor_active")
      Time.stub(:now, Time.iso8601("2026-09-28T12:00:09Z")) do
        text = human
        assert_includes text, "Pause: requested (draining active jobs)"
        assert_includes text, "Last run elapsed: 9s"
        refute_includes text, "running records may be stale"
      end
    end
    refute @report.document.fetch("executor_active")
    refute_includes human, "Last run elapsed:"
  end

  def test_status_and_summary_format_switches_in_process
    [["status"], ["status", "--json"], ["summary", "--json"]].each do |command, *flags|
      out, err, code = in_process_cli(command, @plan, "--output", @output, *flags)
      assert_equal 0, code, err
      assert_equal @report.document, JSON.parse(out)
    end
    [["summary"], ["status", "--human"]].each do |command, *flags|
      out, err, code = in_process_cli(command, @plan, "--output", @output, *flags)
      assert_equal 0, code, err
      assert_equal human, out
    end
  end

  private

  def write_json(name, document)
    path = File.join(@output, name)
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, JSON.generate(document))
  end

  def write_jobs(statuses)
    rows = statuses.each_with_index.map do |status, index|
      row = { "job_id" => "job-#{index}", "status" => status, "worker" => "local", "attempt" => 2,
              "exit_status" => status == "failed" ? 3 : nil }
      row["failure_reason"] = "worker_disappeared" if status == "failed"
      row
    end
    write_json("jobs.json", "jobs" => rows)
  end

  def write_pause
    FileUtils.mkdir_p(File.join(@output, "control"))
    File.write(File.join(@output, "control/pause"), "requested\n")
  end

  def human
    out = StringIO.new
    @report.print(out)
    out.string
  end
end
