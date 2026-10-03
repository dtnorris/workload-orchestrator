# frozen_string_literal: true

require_relative "test_helper"

class ExecutionDoctorTest < Minitest::Test
  include WloTestSupport

  def setup
    @root = Dir.mktmpdir("wlo-doctor-")
    @output = File.join(@root, "output")
    FileUtils.mkdir_p(@output)
    @job_id = "opaque-job.7f4a"
    @plan_path = write_plan(@root, jobs: [job(@job_id, code: "exit 7")])
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    write_state
  end

  def teardown
    FileUtils.rm_rf(@root)
  end

  def test_failed_job_resolves_exact_opaque_id_and_exposes_one_action
    write_jobs("failed", exit_status: 7)
    write_metadata("status" => "failed", "attempt" => 2, "worker" => "A2", "exit_status" => 7)

    result = doctor.diagnose(@job_id)

    assert_equal "wlo-execution-diagnostic/v0.1", result.fetch("contract_version")
    assert_equal @job_id, result.dig("subject", "id")
    assert_equal "command_failure", result.fetch("stage")
    assert_equal({ "action" => "inspect_job_logs" }, result.fetch("next_action"))
    assert_equal 7, result.dig("evidence", 0, "exit_status")
  end

  def test_healthy_interrupted_running_and_exception_stages
    write_jobs("complete", exit_status: 0)
    assert_equal "healthy", doctor.diagnose(@job_id).fetch("stage")
    assert_nil doctor.diagnose(@job_id).fetch("next_action")

    write_jobs("interrupted")
    write_metadata("status" => "interrupted", "attempt" => 1, "term_signal" => 15,
                   "evidence" => { "signal" => "INT", "termination_mode" => "process_group" })
    assert_equal "foreground_cancellation", doctor.diagnose(@job_id).fetch("stage")

    write_jobs("running")
    assert_equal "command_execution", doctor.diagnose(@job_id).fetch("stage")

    write_jobs("failed")
    write_metadata("status" => "failed", "attempt" => 1, "exit_status" => nil, "error" => "spawn failed")
    assert_equal "command_exception", doctor.diagnose(@job_id).fetch("stage")
  end

  def test_breaker_and_worker_generation_take_precedence
    write_jobs("failed", exit_status: 1)
    write_metadata("status" => "failed", "attempt" => 1, "exit_status" => 1,
                   "error" => "worker generation disappeared", "evidence" => { "kind" => "remote_in_doubt" })
    assert_equal "worker_generation", doctor.diagnose(@job_id).fetch("stage")

    write_metadata("status" => "failed", "attempt" => 1, "exit_status" => 1)
    write_state("circuit_breaker" => { "tripped" => true, "reason" => "failure limit reached" })
    assert_equal "circuit_breaker", doctor.diagnose(@job_id).fetch("stage")
  end

  def test_logs_are_bounded_redacted_and_missing_streams_are_empty
    run = File.join(@output, "runs", @job_id)
    FileUtils.mkdir_p(run)
    File.write(
      File.join(run, "stderr.log"),
      "EXAMPLE_API_KEY=secret\nSERVICE_ACCESS_TOKEN: token\nAuthorization: Bearer bearer-value\nlast\n"
    )
    write_metadata("status" => "failed", "attempt" => 3, "exit_status" => 1, "error" => "bad")

    result = doctor.logs(@job_id, lines: 4)

    assert_equal 4, result.dig("stderr", "lines").length
    assert_equal [
      "EXAMPLE_API_KEY=[REDACTED]",
      "SERVICE_ACCESS_TOKEN: [REDACTED]",
      "Authorization: Bearer [REDACTED]",
      "last"
    ], result.dig("stderr", "lines")
    assert_empty result.dig("stdout", "lines")
    assert_equal 3, result.dig("metadata", "attempt")
    assert_raises(WorkloadOrchestrator::Error) { doctor.logs(@job_id, lines: 0) }
  end

  def test_log_redaction_does_not_hide_ordinary_non_secret_variables
    run = File.join(@output, "runs", @job_id)
    FileUtils.mkdir_p(run)
    lines = [
      "TOKEN_COUNT=128", "KEYBOARD_LAYOUT=us", "PUBLIC_API_URL=https://example.invalid",
      "MODEL_NAME=generic", "NOT_API_KEY_COUNT=3"
    ]
    File.write(File.join(run, "stderr.log"), "#{lines.join("\n")}\n")

    assert_equal lines, doctor.logs(@job_id, lines: lines.length).dig("stderr", "lines")
  end

  def test_pending_priority_job_reports_worker_discovery_or_pause
    document = JSON.parse(File.read(@plan_path))
    document["contract_version"] = WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan_path, JSON.generate(document))
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    write_state
    write_jobs("pending")

    result = doctor.diagnose(@job_id)
    assert_equal "worker_discovery", result.fetch("stage")
    assert_equal "inspect_registry", result.dig("next_action", "action")

    FileUtils.mkdir_p(File.join(@output, "control"))
    File.write(File.join(@output, "control", "pause"), "requested\n")
    assert_equal "paused", doctor.diagnose(@job_id).fetch("stage")
  end

  def test_every_fo04_waiting_reason_has_deterministic_stage_and_action
    expected = {
      "NO_RUNNABLE_WORK" => %w[dependency_waiting inspect_execution_status],
      "NO_ACCEPTED_REGISTRY_SNAPSHOT" => %w[worker_discovery inspect_registry],
      "REQUIRED_WORKER_SOURCE_UNAVAILABLE" => %w[registry_validation inspect_registry],
      "REGISTRY_INVALID_OR_STALE" => %w[registry_validation inspect_registry],
      "NO_COMPATIBLE_READY_WORKERS" => %w[worker_eligibility inspect_registry],
      "READY_WORKERS_INCOMPATIBLE" => %w[worker_eligibility inspect_registry],
      "WORKERS_NOT_READY" => %w[worker_eligibility inspect_worker],
      "ALL_COMPATIBLE_WORKERS_BUSY" => %w[worker_capacity wait_for_busy_worker],
      "PAUSED" => %w[paused resume_paused_execution],
      "CIRCUIT_BREAKER" => %w[circuit_breaker inspect_triggering_failure],
      "DISPATCH_HALTED" => %w[dispatch inspect_dispatch_halt],
      "READY_TO_DISPATCH" => %w[dispatch inspect_execution_status]
    }

    expected.each do |reason, (stage, action)|
      row = WorkloadOrchestrator::ExecutionDoctor::WAITING.fetch(reason)
      assert_equal stage, row.fetch(0), reason
      assert_equal action, row.fetch(2), reason
    end
  end

  def test_diagnosis_and_logs_do_not_mutate_retained_evidence
    write_jobs("failed", exit_status: 7)
    write_metadata("status" => "failed", "attempt" => 1, "exit_status" => 7)
    before = Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
                .to_h { |path| [path, File.binread(path)] }

    doctor.diagnose(@job_id)
    doctor.logs(@job_id)

    after = before.keys.to_h { |path| [path, File.binread(path)] }
    assert_equal before, after
  end

  def test_unknown_id_fails_closed_without_domain_specific_parsing
    assert_raises(WorkloadOrchestrator::Error) { doctor.diagnose("missing") }
    error = assert_raises(WorkloadOrchestrator::Error) { doctor.diagnose("0946-openness") }
    assert_includes error.message, "unknown job id"
  end

  private

  def doctor
    WorkloadOrchestrator::ExecutionDoctor.new(plan: @plan, output: @output)
  end

  def write_state(overrides = {})
    state = {
      "plan_id" => @plan.id, "plan_sha256" => @plan.sha256, "status" => "running",
      "circuit_breaker" => { "tripped" => false, "reason" => nil }
    }.merge(overrides)
    File.write(File.join(@output, "execution.json"), JSON.generate(state))
  end

  def write_jobs(status, exit_status: nil)
    row = { "job_id" => @job_id, "pool_id" => "local-pool", "status" => status,
            "worker" => "A2", "attempt" => 1, "exit_status" => exit_status }
    File.write(File.join(@output, "jobs.json"), JSON.generate("jobs" => [row]))
  end

  def write_metadata(document)
    run = File.join(@output, "runs", @job_id)
    FileUtils.mkdir_p(run)
    File.write(File.join(run, "metadata.json"), JSON.generate(document))
  end
end
