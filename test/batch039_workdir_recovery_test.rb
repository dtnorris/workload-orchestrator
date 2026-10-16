# frozen_string_literal: true

require_relative "test_helper"
require_relative "../script/recover-batch039-workdir"

# Exercise the one-time migration with isolated evidence and a faked Git read.
class Batch039WorkdirRecoveryTest < Minitest::Test
  include WloTestSupport

  # Build a small retained execution; only Git is faked at its process boundary.
  # Synthetic pins substitute for private production evidence, never CLI inputs.
  def setup
    @tmp = Dir.mktmpdir("wlo-workdir-migration-")
    @output = File.join(@tmp, "retained")
    @target = File.join(@tmp, "new-workdir")
    path = write_plan(@tmp, jobs: [fixture_job("complete"), fixture_job("failed"), fixture_job("pending")])
    @plan = WorkloadOrchestrator::Plan.load(path)
    @store = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @tmp, output_dir: @output)
    @store.with_execution_lock { @store.prepare! }
    worker = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp)).fetch("local")
    @plan.jobs.first(2).zip(%w[complete failed]).each do |job, status|
      started = @store.record_running!(job: job, worker: worker, environment_keys: [])
      @store.record_terminal!(job: job, status: status, started_at: started, exit_status: status == "complete" ? 0 : 1)
    end
    @store.record_interruption!("TERM")
    File.binwrite(File.join(@output, "runs", "complete", "stdout.log"), "result\x00\xFF".b)
    FileUtils.mkdir_p(File.join(@output, "attempts", "failed", "attempt-1"))
    FileUtils.cp(File.join(@output, "runs", "failed", "metadata.json"),
                 File.join(@output, "attempts", "failed", "attempt-1", "metadata.json"))
    FileUtils.mkdir_p(File.join(@target, "workloads", @plan.id))
    File.binwrite(File.join(@target, "workloads", @plan.id, "execution-plan.json"), @plan.bytes)
    @pins = { "snapshot_sha256" => fingerprint, "plan_sha256" => @plan.sha256,
              "source_workdir" => @tmp, "target_workdir" => @target, "afw_head" => "a" * 40 }
    @subject = Batch039WorkdirRecovery.new(output: @output)
    pins = @pins
    @subject.define_singleton_method(:preconditions) { pins }
    @subject.define_singleton_method(:target_git) do |*arguments|
      [arguments.first == "rev-parse" ? "#{pins.fetch('afw_head')}\n" : "", "", FakeCommandStatus.new(0)]
    end
  end

  # Remove only the test-owned temporary tree.
  def teardown
    FileUtils.rm_rf(@tmp)
  end

  # Preview is byte-preserving, including binary result and archived evidence.
  def test_preview_is_read_only
    before = evidence_bytes
    result = @subject.recover
    refute result.fetch("apply")
    assert_equal before, evidence_bytes
  end

  # A real atomic replacement preserves every byte except execution.json and
  # retains the interruption, failed jobs and breaker without enabling resume.
  def test_apply_embeds_audit_and_preserves_controls_attempts_and_results
    before = evidence_bytes
    original = JSON.parse(before.fetch("execution.json"))
    result = @subject.recover(apply: true, reason: "reviewed workdir relocation")
    after = evidence_bytes
    state = JSON.parse(after.fetch("execution.json"))
    assert_equal(before.except("execution.json"),
                 after.except("execution.json"))
    assert_equal(original.except("workdir"),
                 state.except("workdir", "workdir_migration"))
    assert_equal @target, state.fetch("workdir")
    assert_equal "reviewed workdir relocation", state.dig("workdir_migration", "reason")
    assert_equal Digest::SHA256.hexdigest(after.fetch("execution.json")), result.fetch("execution_after_sha256")
    migrated = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @target, output_dir: @output)
    assert_equal({ "complete" => 1, "failed" => 1, "pending" => 1 }, migrated.counts)
    assert_equal "retained_execution", migrated.action_check("recovery").fetch("reason")
    assert_equal "interrupted_attempts", migrated.action_check("resume").fetch("reason")
    assert_equal "invalid_retained_execution", @store.action_check("recovery").fetch("reason")
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "repeat") }
  end

  # Neither altered identity nor a changed attempt, result, archive, partial
  # migration or extra file can be adopted by asserting the original checksum.
  def test_changed_or_ambiguous_snapshots_are_rejected_without_mutation
    original = evidence_bytes
    mutations = {
      "execution.json" => original.fetch("execution.json").sub(@tmp, @target),
      "plan.json" => "unrelated plan",
      "runs/complete/metadata.json" => original.fetch("runs/complete/metadata.json").sub('"complete"', '"pending"'),
      "runs/complete/stdout.log" => "changed result",
      "attempts/failed/attempt-1/metadata.json" => "changed archive",
      "terminal-import.json" => "ambiguous import",
      "execution.json.workdir-migration.tmp" => "partial migration"
    }
    mutations.each do |path, bytes|
      File.binwrite(File.join(@output, path), bytes)
      changed = evidence_bytes
      assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
      assert_equal changed, evidence_bytes
      if original.key?(path)
        File.binwrite(File.join(@output, path),
                      original.fetch(path))
      else
        File.delete(File.join(@output, path))
      end
    end
  end

  # Refuse links, even if their target is readable and within the fixture.
  def test_symlinked_evidence_is_rejected
    path = File.join(@output, "runs", "complete", "stdout.log")
    File.delete(path)
    File.symlink(File.join(@output, "plan.json"), path)
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert File.symlink?(path)
  end

  # A stale/wrong destination plan or Git state never changes retained bytes.
  def test_target_drift_and_missing_reason_fail_closed
    before = evidence_bytes
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true) }
    File.binwrite(File.join(@target, "workloads", @plan.id, "execution-plan.json"), "wrong plan")
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    File.binwrite(File.join(@target, "workloads", @plan.id, "execution-plan.json"), @plan.bytes)
    @subject.define_singleton_method(:target_git) do |*_arguments|
      ["wrong head or dirty checkout", "", FakeCommandStatus.new(0)]
    end
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    pins = @pins
    @subject.define_singleton_method(:target_git) do |*arguments|
      [arguments.first == "rev-parse" ? "#{pins.fetch('afw_head')}\n" : " M tracked-file\n", "",
       FakeCommandStatus.new(0)]
    end
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal before, evidence_bytes
  end

  # Lock contention blocks the migration before any metadata replacement.
  def test_active_execution_or_state_writer_blocks_migration
    before = evidence_bytes
    %w[.execution.lock .state.lock].each do |name|
      File.open(File.join(@output, name), File::RDONLY) do |lock|
        lock.flock(File::LOCK_EX)
        assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
      end
    end
    assert_equal before, evidence_bytes
  end

  private

  # Return exact relative-path-to-byte evidence for fixture comparisons.
  def evidence_bytes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix("#{@output}/"), File.binread(path)] }
  end

  # Independently construct the documented compact JSON manifest fingerprint.
  def fingerprint
    rows = evidence_bytes.sort.map { |path, bytes| [path, bytes.bytesize, Digest::SHA256.hexdigest(bytes)] }
    Digest::SHA256.hexdigest(JSON.generate(rows))
  end
end
