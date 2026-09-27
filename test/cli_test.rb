# frozen_string_literal: true

require_relative "test_helper"
require "stringio"

class CliTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-cli-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @workers_path = write_workers(@tmp)
    @plan_path = write_plan(@tmp, jobs: [job("job-1", code: "puts :ok")])
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_validate_plan_run_and_status
    assert_equal 0, run_cli("validate", @plan_path).first

    code, out, = run_cli(
      "plan", @plan_path,
      "--workdir", @workdir,
      "--workers-config", @workers_path
    )
    assert_equal 0, code
    assert_includes out, "Zero-cost gate: PASS"

    code, = run_cli(
      "run", @plan_path,
      "--workdir", @workdir,
      "--output", @output,
      "--workers-config", @workers_path
    )
    assert_equal 0, code

    code, out, = run_cli("status", @plan_path, "--output", @output)
    assert_equal 0, code
    document = JSON.parse(out)
    assert_equal "completed", document.fetch("status")
    assert_equal "complete", document.fetch("jobs").first.fetch("status")
  end

  def test_pause_and_resume_commands
    plan = WorkloadOrchestrator::Plan.load(@plan_path)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: plan, workdir: @workdir)
    store.prepare!

    assert_equal 0, run_cli("pause", "--output", @output).first
    _code, out, = run_cli("status", @plan_path, "--output", @output)
    assert_equal true, JSON.parse(out).fetch("paused")

    code, = run_cli(
      "resume", @plan_path,
      "--workdir", @workdir,
      "--output", @output,
      "--workers-config", @workers_path
    )
    assert_equal 0, code
  end

  private

  def run_cli(*argv)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(argv, out: out, err: err).run
    [code, out.string, err.string]
  end
end
