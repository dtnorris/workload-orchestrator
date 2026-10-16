# frozen_string_literal: true

require_relative "test_helper"
require_relative "../script/reconcile-batch039-interruption"

# Synthetic snapshot pins and host command fakes keep all recovery tests offline.
class Batch039InterruptionRecoveryTest < Minitest::Test
  include WloTestSupport

  # Construct real WLO terminal evidence, a historical manager and a migration.
  # Only external Git/host commands and private snapshot pins are substituted.
  def setup
    @tmp = Dir.mktmpdir("wlo-interruption-")
    @output = File.join(@tmp, "retained")
    @target = File.join(@tmp, "target")
    @verification = File.join(@tmp, "verification.json")
    @plan = WorkloadOrchestrator::Plan.load(
      write_plan(@tmp, jobs: %w[complete failed pending].map { |id| fixture_job(id) })
    )
    @store = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @tmp, output_dir: @output)
    @store.with_execution_lock { @store.prepare! }
    worker = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp)).fetch("local")
    @plan.jobs.first(2).zip(%w[complete failed]).each do |job, status|
      started = @store.record_running!(job: job, worker: worker, environment_keys: [])
      @store.record_terminal!(job: job, status: status, started_at: started, exit_status: status == "complete" ? 0 : 1)
    end
    @store.record_interruption!("TERM")
    edit_json("execution.json") { |state| state["interruption"]["at"] = "2000-01-01T00:00:00Z" }
    File.binwrite(File.join(@output, "runs/complete/stdout.log"), "result\x00\xff".b)
    FileUtils.mkdir_p(File.join(@output, "manager"))
    FileUtils.mkdir_p(File.join(@output, "control"))
    owner = { "launch_id" => "fixture-launch", "pid" => 77_885, "started_at" => "1999-12-31T00:00:00Z",
              "status" => "interrupted", "finished_at" => "2000-01-01T00:00:00Z", "exit_status" => 2 }
    write_json("manager/fixture-launch.json", owner)
    write_json("manager.json", owner.except("finished_at", "exit_status").merge("status" => "running"))
    FileUtils.mkdir_p(File.join(@target, "workloads", @plan.id))
    File.binwrite(File.join(@target, "workloads", @plan.id, "execution-plan.json"), @plan.bytes)
    @pins = { "snapshot_sha256" => fingerprint, "plan_sha256" => @plan.sha256,
              "source_workdir" => @tmp, "target_workdir" => @target, "afw_head" => "a" * 40 }
    migration = Batch039WorkdirRecovery.new(output: @output)
    bind_authority(migration)
    migration.recover(apply: true, reason: "fixture migration")
    @subject = Batch039InterruptionRecovery.new(output: @output, verification: @verification)
    bind_authority(@subject)
    @host = { "IOPlatformExpertDevice" => '"IOPlatformUUID" = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"',
              "kern.boottime" => "{ sec = 978307200, usec = 0 } fixture\n",
              "pid=" => "1\n2\n" }
    host = @host
    @subject.define_singleton_method(:host_command) { |*arguments| host.fetch(arguments.last) }
    write_verification
  end

  # Delete only fixture-owned files after each case.
  def teardown
    FileUtils.rm_rf(@tmp)
  end

  # Preview is read-only; the missing-reconciliation negative control remains.
  def test_preview_preserves_every_byte_and_public_resume_is_blocked
    before = evidence_bytes
    result = @subject.recover(reason: "reviewed")
    refute result.fetch("apply")
    assert_equal before, evidence_bytes
    assert_equal "interrupted_attempts", migrated_store.action_check("resume").fetch("reason")
  end

  # Atomic reconciliation preserves terminal and pending records, history and
  # binary output. The normal public check allows only explicit later resume.
  def test_apply_preserves_jobs_and_embeds_exact_before_evidence_and_pause
    before = evidence_bytes
    original = JSON.parse(before.fetch("execution.json"))
    result = @subject.recover(apply: true, reason: "reviewed TERM")
    state = JSON.parse(evidence_bytes.fetch("execution.json"))
    assert_equal before.except("execution.json"), evidence_bytes.except("execution.json", "control/pause")
    assert_equal before.fetch("execution.json"), state.dig("interruption_reconciliation", "execution_before")
    assert_equal [original.fetch("interruption")], state.fetch("interruption_history")
    assert_equal original.except("interruption", "status", "updated_at"),
                 state.except("interruption_history", "interruption_reconciliation", "status", "updated_at")
    assert_equal "paused", result.fetch("status")
    assert_equal({ "complete" => 1, "failed" => 1, "pending" => 1 }, migrated_store.counts)
    assert_equal "execute", migrated_store.action_check("resume").fetch("disposition")
    assert_equal "paused", migrated_store.action_check("run").fetch("reason")
    assert_refused_without_change { @subject.recover(apply: true, reason: "repeat") }
  end

  # Exact snapshot pins exclude every unsafe semantic/evidence variation, even
  # when ordinary WLO validation would accept an altered terminal result.
  def test_changed_evidence_and_unsafe_execution_states_refuse_without_mutation
    original = evidence_bytes
    mutations = {
      "execution.json" => %w[running interruption retry_pending circuit_breaker dispatch_halt workdir plan_sha256],
      "jobs.json" => ["summary"], "plan.json" => ["plan"],
      "runs/complete/stdout.log" => ["result"], "runs/failed/metadata.json" => %w[attempt interrupted]
    }
    mutations.each do |path, cases|
      cases.each do |variant|
        File.binwrite(File.join(@output, path), original.fetch(path) + variant)
        assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
        File.binwrite(File.join(@output, path), original.fetch(path))
      end
    end
    %w[runs/complete/metadata.json manager/fixture-launch.json].each do |path|
      File.delete(File.join(@output, path))
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
      File.binwrite(File.join(@output, path), original.fetch(path))
    end
  end

  # Real JSON mutations cover individually interrupted/running jobs, queued
  # retries, changed breaker/halt, identity, summary and partial recovery.
  def test_semantic_mutations_refuse_without_rewriting_evidence
    mutations = [
      ["runs/failed/metadata.json", ->(row) { row["status"] = "interrupted" }],
      ["runs/failed/metadata.json", ->(row) { row["status"] = "running" }],
      ["execution.json", ->(row) { row["retry_pending"] = { "failed" => 1 } }],
      ["execution.json", ->(row) { row["circuit_breaker"]["tripped"] = true }],
      ["execution.json", ->(row) { row["dispatch_halt"] = { "kind" => "worker_registry" } }],
      ["execution.json", ->(row) { row["plan_id"] = "foreign" }],
      ["execution.json", ->(row) { row["workdir"] = @tmp }],
      ["execution.json", ->(row) { row["interruption_reconciliation"] = { "partial" => true } }],
      ["jobs.json", ->(row) { row["jobs"].first["status"] = "pending" }]
    ]
    mutations.each do |path, mutate|
      original = File.binread(File.join(@output, path))
      edit_json(path, &mutate)
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
      File.binwrite(File.join(@output, path), original)
    end
  end

  # Active execution and state writers are excluded using WLO's actual locks.
  def test_lock_contention_blocks_reconciliation
    %w[.execution.lock .state.lock].each do |name|
      File.open(File.join(@output, name), File::RDONLY) do |lock|
        lock.flock(File::LOCK_EX)
        assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
      end
    end
  end

  # Missing authorization, old/future records, unconfirmed source host and
  # unresolved remote processes never become termination authority.
  def test_operator_verification_is_fresh_explicit_and_bound_to_this_copy
    assert_refused_without_change { @subject.recover(apply: true) }
    changes = { "checked_at" => "2000-01-01T00:00:00Z", "source_host_confirmed" => false,
                "remote_processes_absent" => false, "source_host_uuid" => "other",
                "output" => "/foreign", "snapshot_sha256" => "tampered", "remote_evidence" => "" }
    changes.each do |key, value|
      write_verification(key => value)
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
    end
    write_verification("checked_at" => (Time.now + 100).utc.iso8601)
    assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
  end

  # No acceptance from a terminal manager or absent PID on the same boot, and
  # no acceptance for a historical PID that now belongs to another process.
  def test_same_boot_pid_reuse_and_unknown_host_fail_closed
    [["kern.boottime", "{ sec = 1, usec = 0 } fixture\n"],
     ["kern.boottime", "unknown"], ["IOPlatformExpertDevice", "other\n"],
     ["pid=", "1\n77885\n"], ["pid=", ""], ["pid=", "1\n1\n"], ["pid=", "invalid\n"]].each do |key, value|
      original = @host.fetch(key)
      @host[key] = value
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
      @host[key] = original
    end
  end

  # Qualify the real host command adapter against denied/missing facilities.
  def test_unavailable_host_inspection_does_not_fall_back
    @subject.singleton_class.remove_method(:host_command)
    failure = ->(*) { ["", "operation not permitted", FakeCommandStatus.new(127)] }
    Open3.stub(:capture3, failure) do
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
    end
    missing = ->(*) { raise Errno::ENOENT, "ps" }
    Open3.stub(:capture3, missing) do
      assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
    end
  end

  # A real fixture child supplies live/dead PID observations. Reboot/source
  # identity remain deterministic host fakes, never assertions about this Mac.
  def test_fixture_owned_live_and_terminated_processes
    reader, writer = IO.pipe
    child = fork do
      writer.close
      reader.read
      exit! 0
    end
    reader.close
    owners = @subject.send(:manager_owners!).map { |row| row.merge("pid" => child) }
    @subject.define_singleton_method(:manager_owners!) { owners }
    @host["pid="] = "1\n#{child}\n"
    Process.kill(0, child)
    assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
    writer.close
    Process.wait(child)
    child = nil
    @host["pid="] = "1\n"
    assert_equal "source_host_reboot_after_interruption",
                 @subject.recover(reason: "reviewed").dig("liveness", "local_termination")
  ensure
    writer&.close unless writer&.closed?
    Process.wait(child) if child
  end

  # Revalidation under locks catches drift between the first and final checks.
  def test_race_during_verification_is_rejected
    host = @host
    output = @output
    @subject.define_singleton_method(:host_command) do |*arguments|
      File.binwrite(File.join(output, "runs/complete/stdout.log"), "concurrent mutation")
      host.fetch(arguments.last)
    end
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal "interrupted", JSON.parse(evidence_bytes.fetch("execution.json")).fetch("status")
    refute evidence_bytes.key?("control/pause")
  end

  # A process exit before mutation leaves the original bytes. Death after the
  # pause or temporary write leaves explicit partial evidence, never success.
  def test_real_process_interruption_at_recovery_write_boundaries
    original = evidence_bytes
    %w[before pause partial temporary].each do |stage|
      child = fork do
        @subject.define_singleton_method(:durable_write_new) do |path, bytes|
          exit! 77 if stage == "before"
          if stage == "partial" && path.end_with?(".tmp")
            File.binwrite(path, bytes.byteslice(0, 30))
            exit! 77
          end
          super(path, bytes)
          exit! 77 if stage == "pause" || path.end_with?(".tmp")
        end
        @subject.recover(apply: true, reason: "fixture crash")
        exit! 78
      end
      _, status = Process.wait2(child)
      assert_equal 77, status.exitstatus
      assert_equal original.fetch("execution.json"), evidence_bytes.fetch("execution.json")
      if stage == "before"
        assert_equal original, evidence_bytes
      else
        assert_refused_without_change { @subject.recover(apply: true, reason: "partial") }
        File.delete(File.join(@output, "control/pause"))
        temporary = File.join(@output, "execution.json.interruption-reconciliation.tmp")
        FileUtils.rm_f(temporary)
      end
    end
  end

  # Refuse an owner becoming live during staging, retaining the original
  # interruption rather than publishing a stale liveness decision.
  def test_late_liveness_race_leaves_safe_partial_state
    host = @host
    @subject.define_singleton_method(:durable_write_new) do |path, bytes|
      super(path, bytes)
      host["pid="] = "1\n77885\n" if path.end_with?(".tmp")
    end
    before = evidence_bytes.fetch("execution.json")
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal before, evidence_bytes.fetch("execution.json")
    assert evidence_bytes.key?("control/pause")
    assert_refused_without_change { @subject.recover(apply: true, reason: "partial") }
  end

  # Detect non-cooperating result mutation after staging; never adopt new
  # result bytes as reviewed evidence or publish reconciliation success.
  def test_late_result_race_leaves_interruption_and_pause
    output = @output
    @subject.define_singleton_method(:durable_write_new) do |path, bytes|
      super(path, bytes)
      File.binwrite(File.join(output, "runs/complete/stdout.log"), "foreign mutation") if path.end_with?(".tmp")
    end
    before = evidence_bytes.fetch("execution.json")
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal before, evidence_bytes.fetch("execution.json")
    assert evidence_bytes.key?("control/pause")
  end

  # Exclusive creation is insufficient if a non-cooperating writer tampers
  # with the staged candidate. Never rename unverified candidate bytes.
  def test_staged_execution_tampering_is_rejected
    @subject.define_singleton_method(:durable_write_new) do |path, bytes|
      super(path, bytes)
      File.binwrite(path, "foreign staged state") if path.end_with?(".tmp")
    end
    before = evidence_bytes.fetch("execution.json")
    error = assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_includes error.message, "staged execution changed"
    assert_equal before, evidence_bytes.fetch("execution.json")
    assert evidence_bytes.key?("control/pause")
  end

  # An operator withdrawal during the final process probe supersedes the
  # earlier acceptance record; retaining a fresh timestamp is insufficient.
  def test_verification_withdrawn_during_final_probe_is_rejected
    verification = @verification
    host = @host
    calls = 0
    @subject.define_singleton_method(:host_command) do |*arguments|
      calls += 1 if arguments.first == "ps"
      if calls == 3
        document = JSON.parse(File.binread(verification))
        document["remote_processes_absent"] = false
        File.write(verification, JSON.generate(document))
      end
      host.fetch(arguments.last)
    end
    before = evidence_bytes.fetch("execution.json")
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal before, evidence_bytes.fetch("execution.json")
    assert evidence_bytes.key?("control/pause")
  end

  # Exercise ownership validation independently of the stronger snapshot pin.
  # Missing historical completion cannot be rescued by a missing current PID.
  def test_incomplete_and_contradictory_manager_ownership
    path = "manager/fixture-launch.json"
    original = File.binread(File.join(@output, path))
    %w[finished_at started_at pid launch_id].each do |key|
      edit_json(path) { |owner| owner.delete(key) }
      assert_raises(WorkloadOrchestrator::Error, KeyError, TypeError) { @subject.send(:manager_owners!) }
      File.binwrite(File.join(@output, path), original)
    end
    edit_json("manager.json") { |pointer| pointer["pid"] = 1 }
    assert_raises(WorkloadOrchestrator::Error) { @subject.send(:manager_owners!) }
  end

  # A working hardware/boot probe cannot mask specifically denied ps access.
  def test_unavailable_ps_is_a_blocker_after_valid_host_boot_evidence
    @subject.singleton_class.remove_method(:host_command)
    host = @host
    command = lambda do |*arguments|
      if arguments.first == "ps"
        ["", "operation not permitted", FakeCommandStatus.new(127)]
      else
        [host.fetch(arguments.last), "", FakeCommandStatus.new(0)]
      end
    end
    before = evidence_bytes
    Open3.stub(:capture3, command) do
      error = assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
      assert_includes error.message, "host inspection unavailable (ps -axo pid=)"
    end
    assert_equal before, evidence_bytes
  end

  # Failure after atomic commit is reported as failure, but the complete audit
  # and pause remain visible; replay refuses instead of falsifying new success.
  def test_failure_after_commit_retains_complete_audit
    output = @output
    @subject.define_singleton_method(:sync_directory) do |path|
      super(path)
      raise IOError, "fixture post-commit failure" if path == output
    end
    assert_raises(IOError) { @subject.recover(apply: true, reason: "reviewed") }
    assert_equal "execute", migrated_store.action_check("resume").fetch("disposition")
    assert_equal "paused", migrated_store.action_check("run").fetch("reason")
    assert_refused_without_change { @subject.recover(apply: true, reason: "repeat") }
  end

  # Foreign evidence aliases and changed AFW authority fail before mutation.
  def test_unsafe_paths_and_target_drift
    path = File.join(@output, "runs/complete/stdout.log")
    bytes = File.binread(path)
    File.delete(path)
    File.symlink(File.join(@output, "plan.json"), path)
    assert_raises(WorkloadOrchestrator::Error) { @subject.recover(apply: true, reason: "reviewed") }
    File.delete(path)
    File.binwrite(path, bytes)
    @subject.define_singleton_method(:target_git) { |*| ["wrong or dirty", "", FakeCommandStatus.new(0)] }
    assert_refused_without_change { @subject.recover(apply: true, reason: "reviewed") }
  end

  private

  # Substitute synthetic fixed pins and the read-only Git process boundary.
  def bind_authority(subject)
    pins = @pins
    subject.define_singleton_method(:preconditions) { pins }
    subject.define_singleton_method(:target_git) do |*arguments|
      [arguments.first == "rev-parse" ? "#{pins.fetch('afw_head')}\n" : "", "", FakeCommandStatus.new(0)]
    end
  end

  # Produce a fresh, private operator fixture; never a production attestation.
  def write_verification(changes = {})
    document = { "contract_version" => Batch039InterruptionRecovery::VERIFICATION_CONTRACT,
                 "snapshot_sha256" => @pins.fetch("snapshot_sha256"), "output" => @output,
                 "checked_at" => Time.now.utc.iso8601, "source_host_uuid" => "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
                 "operator" => "offline fixture", "source_host_confirmed" => true,
                 "remote_processes_absent" => true, "remote_evidence" => "fixture: no remote processes created" }
    File.write(@verification, JSON.generate(document.merge(changes)), mode: "w", perm: 0o600)
  end

  # Return a normal store without preparing or rewriting retained state.
  def migrated_store
    WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @target, output_dir: @output)
  end

  # Assert a precise refusal category and exact byte preservation.
  def assert_refused_without_change(&)
    before = evidence_bytes
    assert_raises(WorkloadOrchestrator::Error, JSON::ParserError, KeyError, &)
    assert_equal before, evidence_bytes
  end

  # Mutate JSON only inside a fixture to construct negative evidence.
  def edit_json(relative)
    document = JSON.parse(File.binread(File.join(@output, relative)))
    yield document
    write_json(relative, document)
  end

  # Serialize fixture JSON with the same newline convention as the store.
  def write_json(relative, document)
    File.binwrite(File.join(@output, relative), "#{JSON.pretty_generate(document)}\n")
  end

  # Return all regular file bytes for independent preservation comparisons.
  def evidence_bytes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix("#{@output}/"), File.binread(path)] }
  end

  # Compute synthetic original snapshot provenance using the documented format.
  def fingerprint
    rows = evidence_bytes.sort.map { |path, bytes| [path, bytes.bytesize, Digest::SHA256.hexdigest(bytes)] }
    Digest::SHA256.hexdigest(JSON.generate(rows))
  end
end
