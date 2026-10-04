# frozen_string_literal: true

require_relative "test_helper"

class ExecutionActionCheckTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-action-check-")
    @output = File.join(@tmp, "output")
    @plan_path = write_plan(@tmp, jobs: [fixture_job("one"), fixture_job("two")])
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    @store = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @tmp, output_dir: @output)
    @worker = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp)).fetch("local")
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_new_run_and_missing_retained_execution_never_create_output
    assert_check "run", "execute", "new_execution", status: nil
    %w[resume retry-failed recovery repair].each do |action|
      assert_check action, "blocked", "no_retained_execution", status: nil
    end
    refute_path_exists @output
  end

  def test_empty_output_and_existing_lock_files_are_not_claimed_or_changed
    FileUtils.mkdir_p(@output)
    File.write(File.join(@output, ".state.lock"), "")
    File.write(File.join(@output, ".execution.lock"), "")
    assert_check "run", "execute", "new_execution", status: nil
    File.write(File.join(@output, "unowned"), "evidence")
    assert_check "run", "blocked", "invalid_retained_execution", status: nil
  end

  def test_retained_pending_run_and_resume_are_admissible
    @store.prepare!
    %w[run resume].each { |action| assert_check action, "execute", "retained_pending_execution" }
  end

  def test_pause_marker_and_retained_pause_require_explicit_resume
    @store.prepare!
    @store.pause!
    assert_check "run", "blocked", "paused", status: "paused"
    assert_check "resume", "execute", "resume_paused_execution", status: "paused"
    @store.clear_pause!
    assert_check "run", "blocked", "paused", status: "paused"
    assert_check "resume", "execute", "resume_paused_execution", status: "paused"
    change_state { |state| state["status"] = "pending" }
    File.write(File.join(@output, "control", "pause"), "pause requested")
    assert_check "run", "blocked", "paused"
    assert_check "resume", "execute", "resume_paused_execution"
  end

  def test_complete_execution_is_a_noop_only_for_run_and_resume
    @store.prepare!
    @plan.jobs.each { |job| terminal(job, "complete") }
    @store.finish!
    %w[run resume].each { |action| assert_check action, "already_complete", "completed", status: "completed" }
    assert_check "recovery", "execute", "retained_execution", status: "completed"
    assert_check "retry-failed", "blocked", "no_retryable_jobs", status: "completed"
    assert_check "repair", "blocked", "repair_unsupported", status: "completed"
  end

  def test_failed_work_requires_explicit_retry_but_history_is_readable
    @store.prepare!
    terminal(@plan.jobs.first, "failed")
    terminal(@plan.jobs.last, "complete")
    @store.finish!
    %w[run resume].each { |action| assert_check action, "blocked", "workload_failed", status: "workload_failed" }
    assert_check "retry-failed", "execute", "retry_selection_required", status: "workload_failed"
    assert_check "recovery", "execute", "retained_execution", status: "workload_failed"
  end

  def test_interrupted_attempt_and_execution_require_review
    @store.prepare!
    terminal(@plan.jobs.first, "interrupted")
    @store.record_interruption!("INT")
    %w[run resume].each { |action| assert_check action, "blocked", "interrupted_attempts", status: "interrupted" }
    assert_check "retry-failed", "execute", "retry_selection_required", status: "interrupted"
    assert_check "recovery", "execute", "retained_execution", status: "interrupted"
  end

  def test_breaker_and_dispatch_halt_do_not_get_acknowledged_or_cleared
    @store.prepare!
    terminal(@plan.jobs.first, "failed")
    terminal(@plan.jobs.last, "failed")
    @store.finish!
    %w[run resume].each { |action| assert_check action, "blocked", "circuit_breaker", status: "circuit_broken" }
    assert_check "retry-failed", "execute", "retry_selection_required", status: "circuit_broken"
    @store.acknowledge_circuit_breaker!
    @store.record_dispatch_halt!(kind: "command_launch", error: "failed", job: @plan.jobs.first)
    %w[run resume].each { |action| assert_check action, "blocked", "dispatch_halt", status: "infrastructure_failed" }
    assert_check "retry-failed", "execute", "retry_selection_required", status: "infrastructure_failed"
    assert_check "recovery", "execute", "retained_execution", status: "infrastructure_failed"
  end

  def test_authorized_retry_is_pending_without_changing_original_attempt_evidence
    @store.prepare!
    terminal(@plan.jobs.first, "interrupted")
    @store.record_interruption!("INT")
    @store.authorize_retry!(reason: "reviewed", job_ids: ["one"])
    assert_check "run", "blocked", "paused", status: "paused"
    assert_check "resume", "execute", "resume_paused_execution", status: "paused"
    # The real retry command retains ownership of idempotence and job selection.
    assert_check "retry-failed", "execute", "retry_selection_required", status: "paused"
    assert_equal "interrupted", JSON.parse(File.read(File.join(@output, "runs/one/metadata.json"))).fetch("status")
  end

  def test_active_owner_blocks_every_action_without_touching_evidence
    @store.prepare!
    @store.start!
    @store.with_execution_lock do
      %w[run resume retry-failed recovery repair].each do |action|
        assert_check action, "blocked", "executor_active", status: nil
      end
    end
  end

  def test_active_owner_is_detected_before_initial_state_exists
    @store.with_execution_lock { assert_check "run", "blocked", "executor_active", status: nil }
  end

  def test_orphaned_execution_and_running_attempts_fail_closed
    @store.prepare!
    @store.start!
    %w[run resume].each { |action| assert_check action, "blocked", "inactive_running_state", status: "running" }
    assert_check "recovery", "execute", "retained_execution", status: "running"
    terminal(@plan.jobs.first, "failed")
    @store.record_running!(job: @plan.jobs.last, worker: @worker, environment_keys: [])
    @store.send(:rebuild_jobs!)
    assert_check "retry-failed", "blocked", "running_attempts", status: "running"
    change_state { |state| state["status"] = "pending" }
    assert_check "resume", "blocked", "running_attempts"
  end

  def test_identity_plan_bytes_and_workdir_mismatch_fail_closed
    @store.prepare!
    original = File.binread(File.join(@output, "execution.json"))
    %w[contract_version plan_id plan_sha256 workdir].each do |field|
      change_state { |state| state[field] = "different" }
      assert_check "run", "blocked", "invalid_retained_execution", status: nil
      File.binwrite(File.join(@output, "execution.json"), original)
    end
    File.write(File.join(@output, "plan.json"), "changed")
    assert_check "resume", "blocked", "invalid_retained_execution", status: nil
  end

  def test_missing_malformed_or_incomplete_evidence_fails_closed
    @store.prepare!
    originals = %w[execution.json plan.json jobs.json].to_h { |name| [name, File.binread(File.join(@output, name))] }
    originals.each do |name, bytes|
      File.delete(File.join(@output, name))
      assert_check "run", "blocked", "invalid_retained_execution", status: nil
      ["{", "null", "[]", "{}"].each do |bad|
        File.write(File.join(@output, name), bad)
        assert_check "run", "blocked", "invalid_retained_execution", status: nil
      end
      File.binwrite(File.join(@output, name), bytes)
    end
    change_state { |state| state["status"] = "unknown" }
    assert_check "resume", "blocked", "invalid_retained_execution", status: nil
  end

  def test_job_summary_must_match_plan_and_authoritative_metadata
    @store.prepare!
    path = File.join(@output, "jobs.json")
    original = File.binread(path)
    [{ "jobs" => [] }, { "jobs" => [nil] }, { "jobs" => [{ "job_id" => "unknown" }] }].each do |bad|
      File.write(path, JSON.generate(bad))
      assert_check "resume", "blocked", "invalid_retained_execution", status: nil
    end
    File.binwrite(path, original)
    terminal(@plan.jobs.first, "failed")
    File.binwrite(path, original)
    assert_check "resume", "blocked", "invalid_retained_execution", status: nil
  end

  def test_repair_remains_unsupported_for_valid_retained_state
    @store.prepare!
    assert_check "repair", "blocked", "repair_unsupported"
    assert_check "retry-failed", "blocked", "no_retryable_jobs"
  end

  def test_cli_json_human_output_and_argument_errors
    before = snapshot
    code, out, err = cli("run", "--json")
    assert_equal 0, code, err
    document = JSON.parse(out)
    assert_equal %w[action contract_version disposition execution_status reason], document.keys.sort
    assert_equal "wlo-execution-action-check/v0.1", document.fetch("contract_version")
    assert_equal "run", document.fetch("action")
    assert_equal "execute", document.fetch("disposition")
    assert_nil document.fetch("execution_status")
    code, out, err = cli("resume", "--json")
    assert_equal 0, code, err
    assert_equal "blocked", JSON.parse(out).fetch("disposition")
    refute_empty JSON.parse(out).fetch("message").strip
    assert_equal 0, cli("run").first
    assert_equal 1, cli("unknown", "--json").first
    assert_equal 1, cli("run", "--acknowledge-circuit-breaker").first
    assert_equal before, snapshot
  end

  def test_dynamic_plan_cli_never_loads_workers_or_sources_and_can_read_recovery
    document = JSON.parse(File.read(@plan_path))
    document["contract_version"] = "wlo-execution-plan/v0.3"
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan_path, JSON.generate(document))
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    @store = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @tmp, output_dir: @output)
    code, out, err = cli("run", "--json", "--workers-config", "/nonexistent/never-read.yml")
    assert_equal 0, code, err
    assert_equal "execute", JSON.parse(out).fetch("disposition")
    @store.prepare!
    terminal(@plan.jobs.first, "failed")
    assert_equal 0, cli("retry-failed", "--json").first
    out, err = StringIO.new, StringIO.new
    args = [@plan_path, "--workdir", @tmp, "--output", @output, "--json"]
    code = WorkloadOrchestrator::CLI.new(["recovery", *args], out:, err:).run
    assert_equal 0, code, err.string
    assert_equal "wlo-recovery-history/v0.1", JSON.parse(out.string).fetch("contract_version")
    code = WorkloadOrchestrator::CLI.new([
      "retry-failed", *args, "--job", "one", "--reason", "reviewed", "--dry-run"
    ], out: StringIO.new, err:).run
    assert_equal 0, code, err.string
  end

  def test_busy_state_lock_does_not_wait_or_create_files
    @store.prepare!
    File.open(File.join(@output, ".state.lock"), File::RDONLY) do |lock|
      lock.flock(File::LOCK_EX)
      assert_check "resume", "blocked", "state_busy", status: nil
    end
  end

  def test_malformed_attempts_controls_and_imports_are_blocked
    @store.prepare!
    terminal(@plan.jobs.first, "failed")
    path = File.join(@output, "runs/one/metadata.json")
    original = File.binread(path)
    [nil, [], {}, { "status" => "failed" }, JSON.parse(original).merge("attempt" => 0)].each do |bad|
      File.write(path, JSON.generate(bad))
      assert_check "retry-failed", "blocked", "invalid_retained_execution", status: nil
    end
    File.binwrite(path, original)
    state_path = File.join(@output, "execution.json")
    original = File.binread(state_path)
    %w[circuit_breaker retry_pending retry_history interruption dispatch_halt terminal_import].each do |field|
      change_state { |state| state[field] = "malformed" }
      assert_check "recovery", "blocked", "invalid_retained_execution", status: nil
      File.binwrite(state_path, original)
    end
    change_state { |state| state["terminal_import"] = { "phase" => "applying" } }
    assert_check "resume", "blocked", "invalid_retained_execution", status: nil
  end

  def test_partial_worker_loss_halt_is_inspected_without_repairing_crash_window
    @store.prepare!
    started = @store.record_running!(job: @plan.jobs.first, worker: @worker, environment_keys: [])
    @store.record_terminal!(job: @plan.jobs.first, status: "failed", started_at: started,
                            exit_status: nil, evidence: { "kind" => "dynamic_worker_loss_in_doubt" })
    assert_check "resume", "blocked", "dispatch_halt"
    refute JSON.parse(File.read(File.join(@output, "execution.json"))).key?("dispatch_halt")
  end

  def test_legacy_attempt_without_number_stays_readable_without_rewrite
    @store.prepare!
    terminal(@plan.jobs.first, "failed")
    path = File.join(@output, "runs/one/metadata.json")
    metadata = JSON.parse(File.read(path))
    metadata.delete("attempt")
    File.write(path, JSON.generate(metadata))
    @store.send(:rebuild_jobs!)
    assert_check "recovery", "execute", "retained_execution"
    assert_check "retry-failed", "execute", "retry_selection_required"
  end

  private

  def cli(action, *options)
    out, err = StringIO.new, StringIO.new
    code = WorkloadOrchestrator::CLI.new([
      "action-check", @plan_path, "--workdir", @tmp, "--output", @output,
      "--action", action, *options
    ], out:, err:).run
    [code, out.string, err.string]
  end

  def assert_check(action, disposition, reason, status: "pending")
    before = snapshot
    result = @store.action_check(action)
    assert_equal "wlo-execution-action-check/v0.1", result.fetch("contract_version")
    assert_equal action, result.fetch("action")
    assert_equal disposition, result.fetch("disposition")
    assert_equal reason, result.fetch("reason")
    status.nil? ? assert_nil(result.fetch("execution_status")) : assert_equal(status, result.fetch("execution_status"))
    refute_empty result.fetch("message").strip if disposition == "blocked"
    assert_equal before, snapshot, "action-check mutated filesystem evidence"
  end

  def snapshot
    Dir.glob(File.join(@tmp, "**", "*"), File::FNM_DOTMATCH).sort.to_h do |path|
      stat = File.stat(path)
      [path, [stat.mode, stat.mtime, File.file?(path) ? File.binread(path) : nil]]
    end
  end

  def terminal(job, status)
    started = @store.record_running!(job:, worker: @worker, environment_keys: [])
    @store.record_terminal!(job:, status:, started_at: started, exit_status: status == "complete" ? 0 : 1)
  end

  def change_state
    path = File.join(@output, "execution.json")
    state = JSON.parse(File.read(path))
    yield state
    File.write(path, JSON.generate(state))
  end
end
