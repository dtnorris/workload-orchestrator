# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

# Offline retained evidence exercises the real Runner before any preparation.
class RunnerInterruptionGuardTest < Minitest::Test
  include WloTestSupport

  # Build zero-cost command fixtures; the executor records every attempted launch.
  def setup
    @tmp = Dir.mktmpdir("wlo-runner-guard-")
    @output = File.join(@tmp, "output")
    @workers_path = write_workers(@tmp)
    @workers = WorkloadOrchestrator::WorkerSet.load(@workers_path)
    jobs = [fixture_job("complete"), fixture_job("failed"),
            job("pending", code: 'File.write("launched", "offline fixture")')]
    @plan_path = write_plan(@tmp, jobs: jobs)
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    @executed = []
    @runner = build_runner
    @store = @runner.store
    @store.with_execution_lock { @store.prepare! }
  end

  # Remove only this test's temporary fixture tree.
  def teardown
    FileUtils.remove_entry(@tmp)
  end

  # Status-only interruption must refuse before pause clearing or dispatch.
  def test_status_only_interruption_refuses_before_preparation
    @store.pause!
    edit_execution do |state|
      state["status"] = "interrupted"
      state.delete("interruption")
    end

    assert_equal 0, @store.counts["interrupted"]

    check = @store.action_check("resume")
    assert_equal "blocked", check.fetch("disposition")
    assert_equal "recovery_required", check.fetch("reason")

    assert_refused { build_runner.run(resume: true) }

    @store.clear_pause!
    assert_refused { build_runner.run(resume: true) }
  end

  # TERM with no individually interrupted attempts must refuse before unpausing.
  def test_execution_only_term_prevents_resume_without_changing_bytes
    retain_term
    assert_equal 0, @store.counts["interrupted"]
    assert_equal 1, @store.counts["pending"]
    assert_refused { @runner.run(resume: true) }
  end

  # Foreground run refuses with or without a pause, including all-terminal work.
  def test_direct_run_refuses_execution_term_before_preparation
    retain_term
    assert_refused { @runner.run }
    @store.clear_pause!
    assert_refused { build_runner.run }
    record_terminal(@plan.jobs.last, "complete")
    assert_equal 0, @store.counts["pending"]
    assert_refused { build_runner.run }
  end

  # Every continuation scope and breaker acknowledgement obey the same guard.
  def test_resume_cannot_release_controls_or_acknowledge_a_breaker
    retain_term
    edit_execution { |state| state.fetch("circuit_breaker")["tripped"] = true }
    [{}, { preserve_pauses: true }, { release_pause: "unowned-token" }].each do |scope|
      assert_refused { build_runner.run(resume: true, acknowledge_circuit_breaker: true, **scope) }
    end
  end

  # A missing summary cannot be silently rebuilt during refusal.
  def test_missing_summary_and_partial_repair_pause_remain_untouched
    retain_term
    File.delete(File.join(@output, "jobs.json"))
    File.binwrite(File.join(@output, "execution.json.interruption-reconciliation.tmp"), "partial audit\x00")
    assert_refused { @runner.run(resume: true) }
  end

  # Dynamic setup and the detached startup callback must never precede refusal.
  def test_dynamic_restore_and_startup_callback_are_not_reached
    retain_term
    source = WorkloadOrchestrator::StaticWorkerSource.from_file(
      File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)
    )
    runner = build_runner(worker_source: source)
    unexpected = ->(*) { flunk "mutating preparation or reconciliation reached" }
    runner.store.stub(:prepare!, unexpected) do
      runner.store.stub(:restore_dynamic_worker_loss_halt!, unexpected) do
        assert_refused { runner.run(resume: true) { flunk "startup published" } }
      end
    end
  end

  # Existing individually interrupted attempts also refuse before control changes.
  def test_attempt_interruption_without_execution_marker_requires_retry
    record_terminal(@plan.jobs.first, "interrupted")
    @store.pause!
    assert_refused { @runner.run(resume: true) }
  end

  # Explicit retry archives the interrupted evidence and clears the active marker.
  def test_authorized_interrupted_retry_remains_resumable
    record_terminal(@plan.jobs.first, "interrupted")
    @store.record_interruption!("TERM")
    prior = JSON.parse(File.read(File.join(@output, "execution.json"))).fetch("interruption")
    @store.authorize_retry!(reason: "offline reviewed retry", job_ids: ["complete"])
    state = JSON.parse(File.read(File.join(@output, "execution.json")))
    refute state.key?("interruption")
    assert_equal [prior], state.fetch("interruption_history")
    archive = File.join(@output, "attempts", "complete", "attempt-1", "metadata.json")
    bytes = File.binread(archive)
    assert_equal "completed", build_runner.run(resume: true)
    assert_equal bytes, File.binread(archive)
    assert_equal 3, @executed.length
  end

  # Fresh output still creates and completes an ordinary execution.
  def test_fresh_execution_remains_valid
    FileUtils.remove_entry(@output)
    assert_equal "completed", build_runner.run
    assert_equal 3, @executed.length
  end

  # A normal manual pause grants the established explicit resume authority.
  def test_pause_without_interruption_remains_resumable
    @store.pause!
    assert_equal "completed", build_runner.run(resume: true)
    refute @store.paused?
    assert_equal 3, @executed.length
  end

  # Invalid/missing retained state refuses with WLO errors before clearing pause.
  def test_malformed_and_absent_retained_state_remain_unchanged
    @store.pause!
    path = File.join(@output, "execution.json")
    ["{", "[]", "null", "{}", '{"status":"paused"}'].each do |bytes|
      File.binwrite(path, bytes)
      assert_refused("execution") { build_runner.run(resume: true) }
    end
    File.delete(path)
    assert_refused("has no WLO execution state") { build_runner.run(resume: true) }
  end

  # Another owner excludes observation and all mutation through the real lock.
  def test_active_execution_lock_prevents_resume
    retain_term
    File.open(File.join(@output, ".execution.lock"), File::RDONLY) do |lock|
      lock.flock(File::LOCK_EX)
      assert_refused("execution is active") { build_runner.run(resume: true) }
    end
  end

  # The guard must acquire ownership before inspecting retained interruption.
  def test_observer_runs_under_execution_ownership
    retain_term
    observer = @store.method(:unresolved_interruption?)
    check = lambda do
      File.open(File.join(@output, ".execution.lock"), File::RDONLY) do |lock|
        refute lock.flock(File::LOCK_EX | File::LOCK_NB)
      end
      observer.call
    end
    @store.stub(:unresolved_interruption?, check) do
      assert_refused { @runner.run(resume: true) }
    end
  end

  # A state writer completes before the guard reads; its TERM cannot be missed.
  def test_conflicting_state_lock_serializes_interruption_observation
    @store.pause!
    runner = build_runner(foreground: false)
    entered = Queue.new
    thread = nil
    observer = runner.store.method(:unresolved_interruption?)
    check = lambda do
      entered << true
      observer.call
    end
    runner.store.stub(:unresolved_interruption?, check) do
      File.open(File.join(@output, ".state.lock"), File::RDONLY) do |lock|
        lock.flock(File::LOCK_EX)
        thread = Thread.new do
          runner.run(resume: true)
        rescue WorkloadOrchestrator::Error => e
          e.message
        end
        Timeout.timeout(2) do
          entered.pop
          Thread.pass until thread.stop?
        end
        edit_execution do |state|
          state["interruption"] = { "signal" => "TERM", "at" => "2000-01-01T00:00:00Z" }
          state["status"] = "interrupted"
        end
        @before_unlock = evidence_bytes
      end
      assert_includes Timeout.timeout(2) { thread.value }, "interrupted attempts require explicit retry"
    ensure
      thread&.join(2)
    end
    assert_equal @before_unlock, evidence_bytes
    assert_empty @executed
  end

  # Real CLI processes traverse foreground and detached entry points offline.
  def test_cli_run_resume_and_detached_resume_refuse_without_publication
    retain_term
    binary = File.expand_path("../bin/wlo", __dir__)
    [["run"], ["resume"], ["start", "--resume"], ["start"]].each do |command, *flags|
      before = evidence_bytes
      stdout, stderr, status = Open3.capture3(
        RbConfig.ruby, binary, command, @plan_path, *flags,
        "--workdir", @tmp, "--output", @output, "--workers-config", @workers_path
      )
      assert_equal 1, status.exitstatus, "#{command} #{flags}: #{stdout} #{stderr}"
      assert_includes stderr, "ERROR: interrupted attempts require explicit retry authorization before resume"
      assert_equal before, evidence_bytes
      refute File.exist?(File.join(@output, "manager.json"))
      refute File.exist?(File.join(@tmp, "launched"))
    end
  end

  # Public read-only action checks keep their existing refusal and control policy.
  def test_public_action_checks_remain_unchanged
    retain_term
    before = evidence_bytes
    %w[run resume].each do |action|
      result = @store.action_check(action)
      assert_equal "blocked", result.fetch("disposition")
      assert_equal "interrupted_attempts", result.fetch("reason")
    end
    assert_equal before, evidence_bytes
  end

  private

  # Construct a normal Runner with a fake command boundary and no paid workers.
  def build_runner(**)
    executor = lambda do |_environment, *argv, **|
      @executed << argv.last
      command_result
    end
    WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: @workers, workdir: @tmp, output_dir: @output,
      command_executor: executor, out: StringIO.new, **
    )
  end

  # Retain terminal results and binary attempt evidence, plus a repair pause.
  def retain_term
    record_terminal(@plan.jobs.first, "failed")
    @store.authorize_retry!(reason: "previous offline retry", job_ids: ["complete"])
    @plan.jobs.first(2).zip(%w[complete failed]).each do |entry, status|
      record_terminal(entry, status)
    end
    @store.pause!
    @store.record_interruption!("TERM")
    # A valid alternative serialization exposes even an identical summary rebuild.
    path = File.join(@output, "jobs.json")
    File.write(path, JSON.generate(JSON.parse(File.read(path))))
  end

  # Persist a fixture attempt through normal store writes, including binary logs.
  def record_terminal(entry, status)
    started = @store.record_running!(job: entry, worker: @workers.fetch("local"), environment_keys: [])
    @store.write_logs(entry, "retained\x00\xff".b, "retained stderr\n")
    @store.record_terminal!(job: entry, status: status, started_at: started, exit_status: status == "complete" ? 0 : 1)
  end

  # Simulate an external retained-state writer only inside this disposable fixture.
  def edit_execution
    path = File.join(@output, "execution.json")
    state = JSON.parse(File.read(path))
    yield state
    File.write(path, JSON.generate(state))
  end

  # Assert WLO's established error, no launches and exact retained file bytes.
  def assert_refused(message = "interrupted attempts require explicit retry authorization before resume", &)
    before = evidence_bytes
    error = assert_raises(WorkloadOrchestrator::Error, &)
    assert_includes error.message, message
    assert_equal before, evidence_bytes
    assert_empty @executed
  end

  # Include controls, locks, current/archived attempts and binary result bytes.
  def evidence_bytes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix("#{@output}/"), File.binread(path)] }
  end
end
