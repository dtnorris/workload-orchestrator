# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require_relative "support/operator_fixtures"

class OperatorUxTest < Minitest::Test
  include WloTestSupport
  include OperatorFixtures

  def setup
    @root = Dir.mktmpdir("wlo operator ; ")
    @output = File.join(@root, "output")
    @workers = write_workers(@root)
    @plan = write_plan(@root, jobs: [job("one", code: "puts :ok")])
    @bin = File.expand_path("../bin/wlo", __dir__)
    @managers = []
  end

  def teardown
    # Release only this test's gated job; wait for any detached manager before removing fixtures.
    File.write(File.join(@root, "release"), "yes")
    @managers.each { |record| await_finished(record) }
    FileUtils.remove_entry(@root)
  end

  def test_detachment_duplicate_exclusion_pause_resume_and_retained_launch_evidence
    code = "File.write('entered', 'yes'); sleep 0.01 until File.exist?('release'); puts 'first finished'"
    @plan = write_plan(@root, jobs: [job("one", code: code), job("two", code: "puts :second")])
    profile = write_operator_profile
    @runtime_options = ["--execution-profile", profile]
    first = start
    await { File.exist?(File.join(@root, "entered")) }
    assert_equal first.fetch("pid"), Process.getsid(first.fetch("pid"))
    summary = report
    assert_equal Digest::SHA256.file(profile).hexdigest, summary.fetch("execution_profile_sha256")
    assert summary.fetch("executor_active")
    assert_equal 1, summary.dig("counts", "running")
    assert_equal 1, summary.dig("counts", "pending")
    %w[start run].each do |command|
      _, error, status = runtime(command)
      assert_equal 1, status
      assert_includes error, "execution is active"
    end
    assert_equal first, JSON.parse(File.read(File.join(@output, "manager.json")))
    assert_equal 0, in_process_cli("pause", "--output", @output).last
    File.write(File.join(@root, "release"), "yes")
    stopped = await_finished(first)
    assert_equal "paused", stopped.fetch("status")
    assert_equal 0, stopped.fetch("exit_status")
    assert_equal 1, report.dig("counts", "pending")

    second = start("--resume")
    assert_equal "completed", await_finished(second).fetch("status")
    refute_equal first.fetch("pid"), second.fetch("pid")
    assert File.file?(first.fetch("record_path"))
    assert_includes File.read(first.fetch("log_path")), "[1/2]"
    assert_includes File.read(second.fetch("log_path")), "[2/2]"
    completed = report
    assert_equal 2, completed.fetch("terminal")
    assert_equal [1, 1], completed.fetch("jobs").map { |row| row.fetch("attempt") }
    human, err, status = cli("summary", @plan, "--output", @output)
    assert_equal 0, status, err
    assert_includes human, "Progress: [2/2] terminal (100.0%)"
    assert_includes human, "Last run finished:"
    assert_includes human, "executor inactive"
  end

  def test_start_output_says_detached_work_outlives_the_invoking_cli
    @runtime_options = ["--execution-profile", write_operator_profile]

    out, err, status = runtime("start")

    assert_equal 0, status, err
    @managers << JSON.parse(File.read(File.join(@output, "manager.json")))
    assert_includes out, "continues after this CLI or terminal exits"
    assert_includes out, "Ctrl-C here is not a workload pause"
    assert_includes out, "WLO never tears down provider capacity"
  end

  # Real fork/setsid, Runner and job process; no repeated executable startup.
  # Retry authorization, archives and repair/resume are covered by RetryTest.
  def test_detached_failure_and_post_acknowledgement_error_are_recorded
    @plan = write_plan(@root, jobs: [job("one", code: "exit 3")],
                              failure_policy: { "max_consecutive_failures" => 1, "max_total_failures" => 1 })
    runner = WorkloadOrchestrator::Runner.new(
      plan: WorkloadOrchestrator::Plan.load(@plan), workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: @root, output_dir: @output
    )
    first = WorkloadOrchestrator::DetachedManager.new(runner).start
    @managers << first
    finished = await_finished(first)
    assert_equal "circuit_broken", finished.fetch("status")
    assert_equal 2, finished.fetch("exit_status")
    assert_equal finished, report.fetch("manager")
    assert_equal 3, report.fetch("jobs").first.fetch("exit_status")

    rejected = WorkloadOrchestrator::DetachedManager.new(runner).start
    @managers << rejected
    failed = await_finished(rejected)
    assert_equal "error", failed.fetch("status")
    assert_equal 1, failed.fetch("exit_status")
    assert_includes failed.fetch("error"), "circuit breaker is tripped"
    assert_includes File.read(rejected.fetch("log_path")), "circuit breaker is tripped"
    assert_equal failed, report.fetch("manager")
    assert_equal finished, JSON.parse(File.read(first.fetch("record_path")))
  end

  private

  def cli(*)
    out, err, status = Open3.capture3(RbConfig.ruby, @bin, *)
    [out, err, status.exitstatus]
  end

  def runtime(command, *)
    cli(command, @plan, "--workdir", @root, "--output", @output,
        "--workers-config", @workers, *@runtime_options.to_a, *)
  end

  def start(*)
    out, err, code = runtime("start", *)
    assert_equal 0, code, "#{out}\n#{err}"
    record = JSON.parse(File.read(File.join(@output, "manager.json")))
    @managers << record
    record
  end

  def report
    out, err, status = in_process_cli("status", @plan, "--output", @output)
    assert_equal 0, status, err
    JSON.parse(out)
  end

  def await_finished(record)
    result = nil
    await do
      result = JSON.parse(File.read(record.fetch("record_path")))
      result.key?("finished_at")
    end
    result
  end

  def await
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
    until yield
      raise "timed out waiting for fixture manager" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end
end
