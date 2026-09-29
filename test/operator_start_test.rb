# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require_relative "support/operator_fixtures"

class OperatorStartTest < Minitest::Test
  include WloTestSupport
  include OperatorFixtures

  def setup
    @root = Dir.mktmpdir("wlo-start-")
    @output = File.join(@root, "output")
    @workers = write_workers(@root)
    @plan = write_plan(@root, jobs: [job("one", code: "puts :ok")])
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_invalid_start_preserves_existing_evidence
    prepare_evidence
    before = evidence
    File.write(@plan, File.read(@plan).sub("fixture-plan", "different-plan"))
    _, error, code = in_process_cli(*start_args)
    assert_equal 1, code
    assert_includes error, "different execution identity"
    assert_equal before, evidence
  end

  def test_profile_drift_is_rejected_before_resume_changes_evidence
    profile = write_operator_profile
    prepare_evidence(profile: profile)
    before = evidence
    File.write(profile, JSON.pretty_generate(JSON.parse(File.read(profile))))
    _, error, code = in_process_cli(*start_args("--resume", "--execution-profile", profile))
    assert_equal 1, code
    assert_includes error, "different execution profile or worker binding"
    assert_equal before, evidence
  end

  def test_profile_and_acknowledgement_gates_reject_before_creating_output
    write_operator_profile
    _, error, code = in_process_cli(*start_args)
    assert_equal 1, code
    assert_includes error, "requires --execution-profile"
    refute File.exist?(@output)
    _, error, code = in_process_cli(*start_args("--acknowledge-circuit-breaker"))
    assert_equal 1, code
    assert_includes error, "breaker acknowledgement requires --resume"
    refute File.exist?(@output)
  end

  # One executable smoke check for the paid-worker rejection path. No provider
  # is contacted and no job runs; the other gate cases call the CLI in-process.
  def test_binary_start_enforces_profiled_zero_cost_gate
    profile = write_operator_profile
    write_workers(@root, rate: 1)
    binary = File.expand_path("../bin/wlo", __dir__)
    _, error, status = Open3.capture3(RbConfig.ruby, binary, *start_args("--execution-profile", profile))
    assert_equal 1, status.exitstatus
    assert_includes error, "refuses paid worker"
    refute File.exist?(@output)
  end

  private

  def start_args(*)
    ["start", @plan, "--workdir", @root, "--output", @output, "--workers-config", @workers, *]
  end

  # Persist realistic evidence without launching a successful job just to set up
  # a rejection. Invalid starts still use the real detached preparation path.
  def prepare_evidence(profile: nil)
    plan = WorkloadOrchestrator::Plan.load(@plan)
    plan = WorkloadOrchestrator::ExecutionProfile.load(profile).bind(plan) if profile
    workers = WorkloadOrchestrator::WorkerSet.load(@workers)
    store = WorkloadOrchestrator::ExecutionStore.new(
      plan: plan, workdir: @root, output_dir: @output,
      workers_sha256: profile && workers.execution_sha256(plan)
    )
    store.with_execution_lock { store.prepare! }
    started = store.record_running!(job: plan.jobs.first, worker: workers.fetch("local"), environment_keys: [])
    store.write_logs(plan.jobs.first, "retained stdout\n", "retained stderr\n")
    store.record_terminal!(job: plan.jobs.first, status: "complete", started_at: started, exit_status: 0)
    store.pause!
  end

  def evidence
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix("#{@output}/"), File.binread(path)] }
  end
end
