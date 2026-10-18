# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

# Durable, execution-bound voluntary scheduling consent, without providers.
class SourceOffersTest < Minitest::Test
  include WloTestSupport

  # Observe selection and claim completion without replacing either boundary.
  class ClaimBarrierRunner < WorkloadOrchestrator::Runner
    attr_accessor :selected, :settled

    private

    # Notify before the actual job claim; retain the native launch path.
    def launch_dynamic_assignment(assignment)
      selected << true
      super
    ensure
      settled << true
    end
  end

  # Deterministically delay a second observation without replacing polling.
  class BlockingSource
    attr_reader :entered, :release

    # Retain immutable publisher bytes and synchronization barriers.
    def initialize(bytes)
      @bytes = bytes
      @calls = 0
      @entered, @release = Queue.new, Queue.new
    end

    # First observation succeeds; the next waits for the test's release barrier.
    def latest_snapshot
      @calls += 1
      if @calls > 1
        entered << true
        release.pop
      end
      @bytes
    end
  end

  # Prepare a disposable existing v0.3 execution and observational command sources.
  def setup
    @root = File.realpath(Dir.mktmpdir("wlo-source-offers-"))
    @output = File.join(@root, "execution")
    @now = Time.now.utc
    @monotonic = 100.0
    path = File.join(@root, "input.json")
    File.write(path, JSON.generate(
      "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "offers-test",
      "failure_policy" => {"max_consecutive_failures" => 2, "max_total_failures" => 3},
      "pools" => [{"pool_id" => "pool", "required_labels" => ["inference"]}],
      "jobs" => [{"job_id" => "job", "pool_id" => "pool", "argv" => [RbConfig.ruby, "-e", "exit 0"]}]
    ))
    @plan = WorkloadOrchestrator::Plan.load(path)
    @store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: @plan, workdir: @root).prepare!
    @sources = WorkloadOrchestrator::WorkerSourceSet.new(%w[local remote required].map do |name|
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(name:, policy: name == "required" ? "required" : "optional",
        source: WorkloadOrchestrator::CommandWorkerSource.new("never-execute", [], workdir: @root))
    end)
    @offers = controls
    @offers.register!(@sources)
    @session = @offers.start_manager!
  end

  # Remove only this test's disposable state after all owned threads have joined.
  def teardown
    FileUtils.remove_entry(@root)
  end

  # Legacy permission is explicit; deliberate adoption excludes without a lease.
  def test_legacy_adoption_and_no_offer_are_distinct_from_qualification
    assert_equal "legacy/unmanaged", @offers.snapshot("local").fetch("mode")
    assert_nil @offers.snapshot("local").fetch("offer_eligible")
    frozen = File.binread(File.join(@output, "plan.json"))
    adopted = @offers.request!("local", action: "adopt")
    assert_equal "managed", adopted.fetch("mode")
    refute adopted.fetch("scheduling_eligible")
    assert_equal "drain", adopted.dig("requested", "intent")
    refute adopted.fetch("effective")
    assert_equal "uncertain", adopted.fetch("drain_result")
    assert_equal adopted, @offers.request!("local", action: "adopt")
    assert_equal adopted, @offers.request!("local", action: "drain")
    assert_equal frozen, File.binread(File.join(@output, "plan.json"))
    assert_equal "legacy/unmanaged", @offers.snapshot("remote").fetch("mode")
    assert_nil @store.metadata_for(@plan.jobs.first)
  end

  # Offer observation/repetition cannot renew; explicit renewal increments intent.
  def test_finite_offer_expiry_repetition_and_explicit_renewal
    @offers.request!("local", action: "adopt")
    offered = @offers.request!("local", action: "offer", duration: "10")
    assert offered.fetch("offer_eligible")
    @now += 2
    @monotonic += 2
    assert_equal offered.fetch("requested"), controls.request!("local", action: "offer", duration: "10").fetch("requested")
    @now += 9
    @monotonic += 9
    refute controls.snapshot("local").fetch("offer_eligible")
    refute controls.request!("local", action: "offer", duration: "10").fetch("offer_eligible")
    renewed = controls.request!("local", action: "offer", duration: "10", renew: true)
    assert renewed.fetch("offer_eligible")
    assert_equal offered.dig("requested", "revision") + 1, renewed.dig("requested", "revision")
    assert_operator renewed.fetch("offer_expires_at"), :>, offered.fetch("offer_expires_at")
    drained = controls.request!("local", action: "drain")
    refute drained.fetch("offer_eligible")
    assert controls.request!("local", action: "offer", duration: "5").fetch("offer_eligible")
  end

  # Monotonic expiry prevents rollback resurrection. Restart conservatively
  # invalidates an offer's incarnation, retaining its original expiry verbatim.
  def test_clock_rollback_and_runner_restart_never_extend_consent
    @offers.request!("local", action: "adopt")
    original = @offers.request!("local", action: "offer", duration: "10")
    @now -= 1
    refute controls.snapshot("local").fetch("offer_eligible")
    @now += 2
    @monotonic += 11
    refute controls.snapshot("local").fetch("offer_eligible")
    controls.start_manager!
    assert_equal original.fetch("offer_expires_at"), controls.snapshot("local").fetch("offer_expires_at")
    refute controls.snapshot("local").fetch("offer_eligible")
    assert controls.request!("local", action: "offer", duration: "10", renew: true).fetch("offer_eligible")
  end

  # Invalid names, duration syntax, required policy and unadopted offers refuse
  # before changing execution/control bytes or invoking configured commands.
  def test_invalid_public_requests_are_read_only
    before = retained_files
    [["missing", "adopt", nil], ["../local", "adopt", nil], ["required", "adopt", nil],
     ["local", "offer", "10"], ["local", "offer", "0"], ["local", "offer", "-1"],
     ["local", "offer", "1.5"], ["local", "offer", "604801"]].each do |name, action, duration|
      assert_raises(WorkloadOrchestrator::Error) { @offers.request!(name, action:, duration:) }
      assert_equal before, retained_files
    end
  end

  # Deletion/corruption after adoption and interruption between tombstone and
  # request publication must never restore unmanaged scheduling.
  def test_missing_malformed_and_unknown_managed_evidence_fail_closed
    @offers.request!("local", action: "adopt")
    path = File.join(@output, "source-offers.json")
    original = File.binread(path)
    [nil, "{bad", "null", JSON.generate(JSON.parse(original).merge("sources" => {}))].each do |bytes|
      bytes ? File.binwrite(path, bytes) : File.delete(path)
      assert_raises(WorkloadOrchestrator::Error) { controls.snapshot("local") }
      assert_raises(WorkloadOrchestrator::Error) { controls.request!("local", action: "offer", duration: "1") }
      assert_raises(WorkloadOrchestrator::Error) { controls.dispatch("local") { flunk "corrupt authority dispatched" } }
      File.binwrite(path, original)
    end
  end

  # Copied outputs, changed configuration, replaced lock files and symlinked
  # records are rejected rather than acquiring a second authority namespace.
  def test_copied_foreign_stale_and_unsafe_bindings_are_rejected
    @offers.request!("local", action: "adopt")
    copy = File.join(@root, "copy")
    FileUtils.cp_r(@output, copy)
    assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::SourceOffers.new(copy).snapshot("local") }
    assert_raises(WorkloadOrchestrator::Error) { @offers.register!(WorkloadOrchestrator::WorkerSourceSet.single(@sources.entries.first.source)) }
    path = File.join(@output, "source-offers.json")
    File.rename(path, "#{path}.saved")
    File.symlink("#{path}.saved", path)
    assert_raises(WorkloadOrchestrator::Error) { controls.snapshot("local") }
    File.delete(path)
    File.rename("#{path}.saved", path)
    File.rename(File.join(@output, ".source-offers.lock"), File.join(@output, "old.lock"))
    assert_raises(WorkloadOrchestrator::Error) { controls.snapshot("local") }
  end

  # Force a request to wait behind an actual unstarted dispatch. The same lock
  # protects spawn and acknowledgment; a delayed selected job then rechecks.
  def test_dispatch_lock_linearizes_spawn_request_and_acknowledgment
    @offers.request!("local", action: "adopt")
    @offers.request!("local", action: "offer", duration: "20")
    entered, spawn, events = Queue.new, Queue.new, Queue.new
    dispatch = Thread.new do
      controls.dispatch("local") do |release|
        entered << true
        spawn.pop
        events << :spawn
        release.call
      end
    end
    Timeout.timeout(2) { entered.pop }
    requested = Thread.new do
      controls.request!("local", action: "drain")
      events << :request
      controls.acknowledge!(@session)
      events << :ack
    end
    spawn << true
    Timeout.timeout(2) { [dispatch, requested].each(&:value) }
    assert_equal %i[spawn request ack], 3.times.map { events.pop }
    controls.dispatch("local") { flunk "selected-before-request assignment escaped exclusion" }
    @store.with_execution_lock do
      controls.with_manager_ownership do |session|
        controls.acknowledge!(session)
        assert controls.snapshot("local").fetch("effective")
      end
    end
    refute controls.snapshot("local").fetch("effective")
    @store.with_execution_lock do
      refute controls.snapshot("local").fetch("effective"), "new lock owner inherited a stale acknowledgment"
    end
  ensure
    spawn << true if spawn
    [dispatch, requested].compact.each(&:join)
  end

  # Fresh CLI objects reconstruct public state without executing discovery.
  def test_public_cli_controls_and_diagnostics
    %w[source-adopt source-offer source-drain source-status].each do |command|
      arguments = [command, "--output", @output, "--source", "local"]
      arguments += ["--duration", "30"] if command == "source-offer"
      out, error = StringIO.new, StringIO.new
      assert_equal 0, WorkloadOrchestrator::CLI.new(arguments, out:, err: error).run, error.string
      row = JSON.parse(out.string)
      assert_equal "local", row.fetch("source_name")
      assert_equal "managed", row.fetch("mode")
      assert_equal 0, row.fetch("active_attempt_count")
      refute row.fetch("effective")
    end
  end

  # An actual selected assignment blocked on its OS job claim remains pending.
  # Live adoption is acknowledged without polling or mislabeling it as running.
  def test_native_selected_assignment_rechecks_after_a_delayed_claim
    output = File.join(@root, "claim-race")
    plan_document = JSON.parse(@plan.bytes)
    plan_document.fetch("jobs").first["env"] = ENV.keys.to_h { |name| [name, nil] }
    path = File.join(@root, "race-plan.json")
    File.write(path, JSON.generate(plan_document))
    plan = WorkloadOrchestrator::Plan.load(path)
    registry = JSON.parse(File.read(File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)))
    now = Time.iso8601(registry.fetch("published_at")) + 1
    sources = WorkloadOrchestrator::WorkerSourceSet.new([
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(name: "local", policy: "optional",
        source: WorkloadOrchestrator::StaticWorkerSource.new(JSON.generate(registry)))
    ])
    runner = ClaimBarrierRunner.new(plan:, workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @root, output_dir: output, worker_source: sources, foreground: false,
      worker_registry_clock: -> { now }, worker_poll_interval: 0.01, out: StringIO.new)
    runner.selected, runner.settled = Queue.new, Queue.new
    runner.store.prepare!
    thread = nil
    WorkloadOrchestrator::JobClaim.new(output_dir: output).synchronize("job") do
      thread = Thread.new { runner.run }
      Timeout.timeout(2) { runner.selected.pop }
      controls = WorkloadOrchestrator::SourceOffers.new(output)
      controls.request!("local", action: "adopt")
      Timeout.timeout(2) { sleep 0.01 until controls.snapshot("local").fetch("effective") }
      assert_nil runner.store.metadata_for(plan.jobs.first), "selected is not running"
      assert_equal 0, controls.snapshot("local").fetch("active_attempt_count")
      report = WorkloadOrchestrator::ExecutionReport.new(plan:, output:, clock: -> { now }).document
      assert_equal "SOURCE_OFFER_EXCLUDED", report.dig("pool_status", 0, "reason")
      assert_equal 1, report.dig("pool_status", 0, "workers", "offer_excluded_idle")
      live = WorkloadOrchestrator::LiveExecutionReport.new(plan:, output:).document(workers: runner.worker_registry_poller.current_workers)
      assert_equal 0, live.fetch("eligible_idle_workers")
    end
    Timeout.timeout(2) { runner.settled.pop }
    assert_nil runner.store.metadata_for(plan.jobs.first), "selected-before-drain job escaped final recheck"
    WorkloadOrchestrator::SourceOffers.new(output).request!("local", action: "offer", duration: "30")
    assert_equal "completed", Timeout.timeout(2) { thread.value }
    assert_equal 1, runner.store.metadata_for(plan.jobs.first).fetch("attempt")
  ensure
    runner&.store&.pause! if thread&.alive?
    thread&.join
  end

  # A live exclusion acknowledgment is independent of runtime observation. A
  # delayed publisher or inherited active record cannot establish quiescence.
  def test_delayed_poll_and_inherited_running_records_are_uncertain
    registry = JSON.parse(File.read(File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)))
    registry["published_at"] = (@now - 1).iso8601
    registry["expires_at"] = (@now + 60).iso8601
    source = BlockingSource.new(JSON.generate(registry))
    poller = WorkloadOrchestrator::WorkerRegistryPoller.new(source:,
      checkpoint_path: File.join(@output, "dynamic-workers", "sources", "local", "checkpoint.json"),
      source_name: "local", policy: "optional", clock: -> { @now }).poll_once
    @offers.request!("local", action: "adopt")
    @offers.acknowledge!(@session)
    thread = nil
    @store.with_execution_lock do
      @offers.with_manager_ownership do |owner_session|
        @offers.acknowledge!(owner_session)
        assert_equal "fully_drained", @offers.snapshot("local").fetch("drain_result")
        thread = Thread.new { poller.poll_once }
        Timeout.timeout(2) { source.entered.pop }
        delayed = @offers.snapshot("local")
        assert delayed.fetch("effective")
        assert_equal "polling", delayed.dig("runtime_health", "last_poll_result")
        assert_equal "uncertain", delayed.fetch("drain_result")
        source.release << true
        Timeout.timeout(2) { thread.value }
        attempt = @store.record_dynamic_running!(job: @plan.jobs.first, worker: poller.ready_workers.first, environment_keys: [])
        session = controls.start_manager!
        controls.acknowledge!(session)
        inherited = controls.snapshot("local")
        assert_equal 1, inherited.fetch("active_attempt_count")
        assert_nil inherited.fetch("inference_running")
        assert_equal "uncertain", inherited.fetch("drain_result")
        @store.record_dynamic_terminal!(attempt:, status: "complete", exit_status: 0)
        assert_equal "fully_drained", controls.snapshot("local").fetch("drain_result")
      end
    end
  ensure
    source&.release&.push(true)
    thread&.join
  end

  # Explicit retry archives unknown outcomes; neither a successful replacement
  # nor re-offer proves that the old inference stopped. Preserve all its bytes.
  def test_archived_in_doubt_attempt_keeps_drain_uncertain_after_authorized_retry
    registry = JSON.parse(File.read(File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)))
    registry["published_at"] = (@now - 1).iso8601
    registry["expires_at"] = (@now + 60).iso8601
    poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: WorkloadOrchestrator::StaticWorkerSource.new(JSON.generate(registry)),
      checkpoint_path: File.join(@output, "dynamic-workers", "sources", "local", "checkpoint.json"),
      source_name: "local", policy: "optional", clock: -> { @now }).poll_once
    attempt = @store.record_dynamic_running!(job: @plan.jobs.first, worker: poller.ready_workers.first, environment_keys: [])
    @store.record_dynamic_worker_loss!(attempt:, evidence: {
      "kind" => "dynamic_worker_loss_in_doubt", "job_id" => attempt.job.id, "attempt_id" => attempt.attempt_id,
      "outcome_known" => false, "reason" => "worker_disappeared", "observed_at" => @now.iso8601,
      "worker_execution_identity" => attempt.worker_binding.execution_identity,
      "worker_registry_binding" => attempt.worker_binding.registry_binding
    })
    @store.retry_failed!(reason: "synthetic explicit retry", job_ids: [attempt.job.id])
    replacement = @store.record_dynamic_running!(job: @plan.jobs.first, worker: poller.ready_workers.first, environment_keys: [])
    @store.record_dynamic_terminal!(attempt: replacement, status: "complete", exit_status: 0)
    archive = File.join(@output, "attempts", attempt.job.id, "attempt-1", "metadata.json")
    original = File.binread(archive)
    @offers.request!("local", action: "adopt")
    @store.with_execution_lock do
      @offers.with_manager_ownership do |session|
        @offers.acknowledge!(session)
        row = @offers.snapshot("local")
        assert row.fetch("effective")
        assert_equal 0, row.fetch("active_attempt_count")
        assert_equal "uncertain", row.fetch("drain_result")
        @offers.request!("local", action: "offer", duration: "10")
        assert_equal original, File.binread(archive)
        assert_equal 2, @store.metadata_for(@plan.jobs.first).fetch("attempt")
      end
    end
  end

  # A failed source acknowledgment must reach the runner and halt dispatch;
  # fault injection affects only the real manager thread's acknowledgment call.
  def test_runner_acknowledgment_thread_failure_blocks_dispatch
    runner = WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @root, output_dir: @output, worker_source: @sources, out: StringIO.new
    )
    failure = lambda do |_session|
      raise WorkloadOrchestrator::Error, "injected acknowledgment failure"
    end
    @offers.stub(:acknowledge!, failure) do
      WorkloadOrchestrator::SourceOffers.stub(:new, @offers) do
        error = assert_raises(WorkloadOrchestrator::Error) { runner.send(:start_source_controls) }
        assert_equal "injected acknowledgment failure", error.message
        assert runner.send(:stop_dispatch?), "source acknowledgment failure permitted dispatch"
      end
    end
  ensure
    runner&.send(:stop_source_controls)
  end

  private

  # Construct a fresh process-equivalent reader with shared deterministic clocks.
  def controls
    WorkloadOrchestrator::SourceOffers.new(@output, clock: -> { @now }, monotonic: -> { @monotonic })
  end

  # Hash substantive retained files for refusal and side-effect assertions.
  def retained_files
    %w[execution.json plan.json jobs.json source-offers.json].to_h do |name|
      [name, File.binread(File.join(@output, name))]
    end
  end
end
