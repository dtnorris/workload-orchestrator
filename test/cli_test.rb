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
    assert_includes out, "Scheduling: pool-major (legacy)"
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

  def test_plan_reports_group_major_scheduling
    grouped_plan = write_plan(
      @tmp,
      jobs: [
        job("a", code: "exit 0", group_id: "adventure-a"),
        job("b", code: "exit 0", group_id: "adventure-b")
      ],
      name: "grouped.json"
    )

    code, out, = run_cli(
      "plan", grouped_plan,
      "--workdir", @workdir,
      "--workers-config", @workers_path
    )

    assert_equal 0, code
    assert_includes out, "Scheduling: group-major (2 groups)"
  end

  def test_plan_reports_v03_priority_scheduling
    profile = write_local_execution_profile
    cases = {
      "grouped" => [
        [job("a", code: "exit 0", group_id: "adventure-a"),
         job("b", code: "exit 0", group_id: "adventure-b")],
        "Scheduling: work-conserving priority (2 reporting groups)"
      ],
      "ungrouped" => [[job("a", code: "exit 0"), job("b", code: "exit 0")],
                      "Scheduling: work-conserving priority"]
    }

    cases.each do |name, (jobs, expected)|
      plan = write_priority_plan(name, jobs)
      code, out, = run_cli(
        "plan", plan, "--workdir", @workdir, "--workers-config", @workers_path,
        "--execution-profile", profile
      )

      assert_equal 0, code
      assert_includes out, expected
    end
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

  def write_priority_plan(name, jobs)
    path = write_plan(@tmp, jobs: jobs, name: "#{name}.json")
    document = JSON.parse(File.read(path))
    document["contract_version"] = WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(path, "#{JSON.pretty_generate(document)}\n")
    path
  end

  def write_local_execution_profile
    path = File.join(@tmp, "execution-profile.json")
    File.write(path, JSON.generate(
      "contract_version" => "wlo-execution-profile/v0.1",
      "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                    "worker_names" => ["local"], "max_concurrency" => 1 }]
    ))
    path
  end

  def run_cli(*argv)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(argv, out: out, err: err).run
    [code, out.string, err.string]
  end
end
