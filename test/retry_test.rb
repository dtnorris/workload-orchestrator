# frozen_string_literal: true

require_relative "test_helper"

class RetryTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-retry-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @workers_path = write_workers(@tmp)
    @workers = WorkloadOrchestrator::WorkerSet.load(@workers_path)
    fail_until_repaired = 'puts "stdout"; warn "stderr"; exit(File.exist?("repaired") ? 0 : 7)'
    @plan_path = write_plan(@tmp, jobs: [
                              job("done", code: 'File.open("done-count", "a") { |f| f.puts "ran" }'),
                              job("bad-1", code: fail_until_repaired), job("bad-2", code: fail_until_repaired),
                              job("later", code: "puts :later")
                            ])
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_breaker_repair_retry_and_resume_preserve_attempt_evidence
    assert_equal "circuit_broken", runner.run
    old = evidence("bad-1")
    assert_equal 1, JSON.parse(old.fetch("metadata.json")).fetch("attempt")
    File.write(File.join(@workdir, "repaired"), "yes")

    code, out, err = retry_cli("--all", "--acknowledge-circuit-breaker")
    assert_equal 0, code, err
    assert_includes out, "Execution remains paused"
    assert_equal "paused", runner.run
    assert_equal({ "complete" => 1, "pending" => 3 }, store.counts)
    assert_equal old, evidence("bad-1", archive: 1)
    assert_equal old, evidence("bad-1")
    event = state.fetch("retry_history").fetch(0)
    assert_equal "repaired cause", event.fetch("reason")
    assert event.fetch("breaker_before").fetch("tripped")
    assert event.fetch("breaker_acknowledged")
    assert_equal 1, state.fetch("circuit_breaker").fetch("generation")
    assert_equal 1, state.fetch("circuit_breaker").fetch("history").size

    assert_equal "completed", runner.run(resume: true)
    assert_equal 2, store.metadata_for(@plan.jobs[1]).fetch("attempt")
    assert_equal "complete", store.metadata_for(@plan.jobs[1]).fetch("status")
    assert_equal old, evidence("bad-1", archive: 1)
    assert_equal ["ran"], File.readlines(File.join(@workdir, "done-count"), chomp: true)
    assert_equal "completed", runner.run(resume: true)
    assert_equal 2, store.metadata_for(@plan.jobs[1]).fetch("attempt")
    assert_equal @plan.bytes, File.binread(File.join(@output, "plan.json"))
  end

  def test_selected_retry_leaves_other_failed_jobs_terminal_and_can_retry_again
    assert_equal "circuit_broken", runner.run
    first = evidence("bad-1")
    assert_equal 0, retry_cli("--job", "bad-1", "--acknowledge-circuit-breaker").first
    assert_equal "workload_failed", runner.run(resume: true)
    second = evidence("bad-1")
    assert_equal 2, JSON.parse(second.fetch("metadata.json")).fetch("attempt")
    assert_equal 1, store.metadata_for(@plan.jobs[2]).fetch("attempt")
    assert_equal 0, retry_cli("--job", "bad-1").first
    File.write(File.join(@workdir, "repaired"), "yes")
    assert_equal "workload_failed", runner.run(resume: true)
    assert_equal 3, store.metadata_for(@plan.jobs[1]).fetch("attempt")
    assert_equal first, evidence("bad-1", archive: 1)
    assert_equal second, evidence("bad-1", archive: 2)
    assert_equal 2, state.fetch("retry_history").length
  end

  def test_invalid_requests_do_not_change_evidence_or_authorization
    assert_equal "circuit_broken", runner.run
    before = File.binread(File.join(@output, "execution.json"))
    old = evidence("bad-1")
    invalid = [[], ["--all", "--job", "bad-1"], ["--job", "missing"],
               ["--job", "bad-1", "--job", "done"], ["--job", "later"],
               ["--job", "bad-1", "--job", "bad-1"], ["--all", "--reason", " "]]
    invalid.each do |args|
      assert_equal 1, retry_cli(*args, "--acknowledge-circuit-breaker").first, args.inspect
      assert_equal before, File.binread(File.join(@output, "execution.json"))
    end
    code, _out, err = retry_cli("--all")
    assert_equal 1, code
    assert_includes err, "--acknowledge-circuit-breaker"
    assert_equal old, evidence("bad-1")
    refute File.exist?(File.join(@output, "attempts"))
    refute store.paused?
  end

  def test_retry_refuses_new_output_and_changed_identity
    assert_equal 1, retry_cli("--all").first
    refute File.exist?(@output)
    runner.run
    other_workdir = File.join(@tmp, "other")
    FileUtils.mkdir_p(other_workdir)
    assert_equal 1, retry_cli("--all", "--workdir", other_workdir, "--acknowledge-circuit-breaker").first
    File.write(@plan_path, File.read(@plan_path).sub("puts :later", "puts :changed"))
    assert_equal 1, retry_cli("--all", "--acknowledge-circuit-breaker").first
    refute File.exist?(File.join(@output, "attempts"))
  end

  def test_retry_and_second_runner_refuse_active_execution_lock
    runner.run
    store.with_execution_lock do
      code, _out, err = retry_cli("--all", "--acknowledge-circuit-breaker")
      assert_equal 1, code
      assert_includes err, "execution is active"
      assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true) }
    end
    assert_equal 0, retry_cli("--all", "--acknowledge-circuit-breaker").first
  end

  def test_running_metadata_refuses_retry_even_without_execution_lock
    runner.run
    store.record_running!(job: @plan.jobs.last, worker: @workers.fetch("local"), environment_keys: [])
    code, _out, err = retry_cli("--all", "--acknowledge-circuit-breaker")
    assert_equal 1, code
    assert_includes err, "running jobs remain"
    refute File.exist?(File.join(@output, "attempts"))
  end

  def test_copy_interruption_does_not_partially_authorize_retry
    runner.run
    before = File.binread(File.join(@output, "execution.json"))
    subject = store
    original = subject.method(:archive_attempt!)
    subject.define_singleton_method(:archive_attempt!) do |job|
      raise IOError, "simulated copy failure" if job.id == "bad-2"

      original.call(job)
    end
    assert_raises(IOError) do
      subject.retry_failed!(all: true, reason: "repair", acknowledge_circuit_breaker: true)
    end
    assert_equal before, File.binread(File.join(@output, "execution.json"))
    assert_equal "failed", store.metadata_for(@plan.jobs[1]).fetch("status")
    assert_equal evidence("bad-1"), evidence("bad-1", archive: 1)
    assert_equal 0, retry_cli("--all", "--acknowledge-circuit-breaker").first
    assert_equal 1, state.fetch("retry_history").length
  end

  def test_existing_conflicting_archive_fails_closed
    runner.run
    archive = File.join(@output, "attempts", "bad-1", "attempt-1")
    FileUtils.mkdir_p(archive)
    File.write(File.join(archive, "metadata.json"), "do not overwrite")
    code, _out, err = retry_cli("--all", "--acknowledge-circuit-breaker")
    assert_equal 1, code
    assert_includes err, "archive conflicts"
    assert_equal "do not overwrite", File.read(File.join(archive, "metadata.json"))
    assert store.circuit_tripped?
    refute state.key?("retry_history")
  end

  def test_no_failures_and_spurious_acknowledgement_are_rejected
    store.prepare!
    assert_equal 1, retry_cli("--all").first
    runner.run
    store.acknowledge_circuit_breaker!
    assert_equal 1, retry_cli("--all", "--acknowledge-circuit-breaker").first
    assert_equal 0, retry_cli("--all").first
    assert_equal 1, retry_cli("--all").first
    code, out, = cli("status", @plan_path, "--output", @output)
    assert_equal 0, code
    rows = JSON.parse(out).fetch("jobs")
    assert_equal "pending", rows[1].fetch("status")
    assert_equal 1, rows[1].fetch("attempt")
  end

  private

  def runner
    WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: @workers, workdir: @workdir, output_dir: @output, out: StringIO.new
    )
  end

  def store
    runner.store
  end

  def state
    JSON.parse(File.read(File.join(@output, "execution.json")))
  end

  def evidence(id, archive: nil)
    path = archive ? File.join(@output, "attempts", id, "attempt-#{archive}") : File.join(@output, "runs", id)
    Dir.children(path).to_h { |name| [name, File.binread(File.join(path, name))] }
  end

  def retry_cli(*)
    cli("retry-failed", @plan_path, "--workdir", @workdir, "--output", @output,
        "--reason", "repaired cause", *)
  end

  def cli(*args)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(args, out: out, err: err).run
    [code, out.string, err.string]
  end
end
