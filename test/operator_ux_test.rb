# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class OperatorUxTest < Minitest::Test
  include WloTestSupport

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
    first = start
    await { File.exist?(File.join(@root, "entered")) }
    assert_equal first.fetch("pid"), Process.getsid(first.fetch("pid"))
    summary = report
    assert summary.fetch("executor_active")
    assert_equal 1, summary.dig("counts", "running")
    assert_equal 1, summary.dig("counts", "pending")
    assert_equal 1, runtime("start").last
    assert_equal 1, runtime("run").last
    assert_equal first, JSON.parse(File.read(File.join(@output, "manager.json")))
    assert_equal 0, cli("pause", "--output", @output).last
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
    assert_equal 2, report.fetch("terminal")
    human, err, status = cli("summary", @plan, "--output", @output)
    assert_equal 0, status, err
    assert_includes human, "Progress: [2/2] terminal (100.0%)"
    assert_includes human, "Last run finished:"
    assert_includes human, "executor inactive"
  end

  def test_failure_breaker_error_and_explicit_retry_work_in_detached_mode
    @plan = write_plan(@root, jobs: [job("one", code: "exit(File.exist?('repaired') ? 0 : 3)")],
                              failure_policy: { "max_consecutive_failures" => 1, "max_total_failures" => 1 })
    first = start
    assert_equal "circuit_broken", await_finished(first).fetch("status")
    assert_equal 2, report.dig("manager", "exit_status")
    rejected = start
    assert_equal "error", await_finished(rejected).fetch("status")
    assert_includes File.read(rejected.fetch("log_path")), "circuit breaker is tripped"
    _, err, status = runtime("retry-failed", "--all", "--reason", "fixture repair", "--acknowledge-circuit-breaker")
    assert_equal 0, status, err
    File.write(File.join(@root, "repaired"), "yes")
    final = start("--resume")
    assert_equal "completed", await_finished(final).fetch("status")
    assert_equal 2, report.fetch("jobs").first.fetch("attempt")
    assert File.file?(File.join(@output, "attempts/one/attempt-1/metadata.json"))
  end

  def test_invalid_start_preserves_existing_evidence
    launch = start
    await_finished(launch)
    before = Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |p| File.file?(p) }
                .to_h { |path| [path, File.binread(path)] }
    File.write(@plan, File.read(@plan).sub("fixture-plan", "different-plan"))
    _, error, code = runtime("start")
    assert_equal 1, code
    assert_includes error, "different execution identity"
    before.each { |path, bytes| assert_equal bytes, File.binread(path), path }
  end

  def test_profiled_start_uses_same_identity_and_zero_cost_gate
    document = JSON.parse(File.read(@plan))
    document["contract_version"] = "wlo-execution-plan/v0.2"
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan, JSON.generate(document))
    profile = File.join(@root, "profile.json")
    value = {
      "contract_version" => "wlo-execution-profile/v0.1",
      "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                    "worker_names" => ["local"], "max_concurrency" => 1 }]
    }
    File.write(profile, JSON.generate(value))
    write_workers(@root, rate: 1)
    assert_equal 1, runtime("start", "--execution-profile", profile).last
    refute File.exist?(@output)
    write_workers(@root)
    launch = start("--execution-profile", profile)
    assert_equal "completed", await_finished(launch).fetch("status")
    assert_equal Digest::SHA256.file(profile).hexdigest, report.fetch("execution_profile_sha256")
    File.write(profile, JSON.pretty_generate(value))
    assert_equal 1, runtime("start", "--resume", "--execution-profile", profile).last
  end

  private

  def cli(*)
    out, err, status = Open3.capture3(RbConfig.ruby, @bin, *)
    [out, err, status.exitstatus]
  end

  def runtime(command, *)
    cli(command, @plan, "--workdir", @root, "--output", @output, "--workers-config", @workers, *)
  end

  def start(*)
    out, err, code = runtime("start", *)
    assert_equal 0, code, "#{out}\n#{err}"
    record = JSON.parse(File.read(File.join(@output, "manager.json")))
    @managers << record
    record
  end

  def report
    out, err, status = cli("status", @plan, "--output", @output)
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
