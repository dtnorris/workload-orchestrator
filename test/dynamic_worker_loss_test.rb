# frozen_string_literal: true

require_relative "test_helper"

class DynamicWorkerLossTest < Minitest::Test
  include WloTestSupport

  FIXTURE = File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)
  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(*snapshots)
      super()
      @snapshots = snapshots
      @calls = 0
    end

    def latest_snapshot
      snapshot = @snapshots.fetch([calls, @snapshots.length - 1].min)
      @calls += 1
      snapshot
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-dynamic-worker-loss-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @plan = WorkloadOrchestrator::Plan.load(
      write_plan(@tmp, jobs: [fixture_job("job-1")])
    )
    @store = WorkloadOrchestrator::ExecutionStore.new(
      output_dir: @output,
      plan: @plan,
      workdir: @workdir
    )
    @store.prepare!
    @worker_a = fixture_document.fetch("workers").first
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_dynamic_attempt_persists_exact_binding_before_dispatch
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]))
    poller.poll_once

    attempt = start_attempt(poller.current_workers.first)
    metadata = metadata_for

    assert_equal 1, attempt.attempt_id
    assert_equal identity_hash(@worker_a), metadata.fetch("worker_execution_identity")
    assert_equal 7, metadata.dig("worker_registry_binding", "registry_revision")
    assert_equal poller.registry.sha256,
                 metadata.dig("worker_registry_binding", "registry_snapshot_sha256")
    assert_equal attempt.worker_binding.tuple, poller.current_workers.first.execution_identity
  end

  def test_snapshot_is_complete_and_recursively_immutable
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]))
    poller.poll_once
    worker = poller.current_workers.first
    attempt = start_attempt(worker)
    expected = identity_hash(@worker_a).merge(
      "registry_revision" => 7,
      "registry_snapshot_sha256" => poller.registry.sha256,
      "published_at" => worker.published_at.iso8601,
      "expires_at" => worker.expires_at.iso8601,
      "state" => "READY",
      "labels" => @worker_a.fetch("labels"),
      "capabilities" => @worker_a.fetch("capabilities")
    )

    assert_equal expected, metadata_for.fetch("worker_snapshot")
    assert_equal expected, attempt.worker_binding.worker_snapshot
    assert_raises(FrozenError) { attempt.worker_binding.worker_snapshot["labels"] << "changed" }
    assert_raises(FrozenError) do
      attempt.worker_binding.worker_snapshot.dig("capabilities", "ollama", "models", 0)["model"].replace("changed")
    end
    assert_equal expected, WorkloadOrchestrator::DynamicWorkerBinding.from_metadata(metadata_for).worker_snapshot
  end

  def test_conflicting_metadata_writes_are_rejected_before_changing_bytes
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]))
    poller.poll_once
    start_attempt(poller.current_workers.first)
    path = File.join(@output, "runs/job-1/metadata.json")
    before = File.binread(path)
    unchanged = metadata_for
    @store.send(:write_json, path, unchanged)
    assert_equal before, File.binread(path)

    %w[worker_snapshot worker_execution_identity worker_registry_binding].each do |key|
      conflicting = deep_copy(unchanged)
      conflicting.delete(key)
      assert_raises(WorkloadOrchestrator::Error) { @store.send(:write_json, path, conflicting) }
      assert_equal before, File.binread(path)
    end
    conflicting = deep_copy(unchanged)
    conflicting["worker_snapshot"]["labels"] << "changed"
    assert_raises(WorkloadOrchestrator::Error) { @store.send(:write_json, path, conflicting) }
    assert_equal before, File.binread(path)
    conflicting = deep_copy(unchanged).merge("attempt" => 2)
    assert_raises(WorkloadOrchestrator::Error) { @store.send(:write_json, path, conflicting) }
    assert_equal before, File.binread(path)
    assert_raises(WorkloadOrchestrator::Error) { start_attempt(poller.current_workers.first) }
    assert_equal before, File.binread(path)
  end

  def test_legacy_binding_loads_without_fabricating_snapshot
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]))
    poller.poll_once
    binding = WorkloadOrchestrator::DynamicWorkerBinding.from_worker(poller.current_workers.first)
    legacy = binding.metadata.reject { |key, _| key == "worker_snapshot" }
    loaded = WorkloadOrchestrator::DynamicWorkerBinding.from_metadata(legacy)

    assert_nil loaded.worker_snapshot
    assert_equal legacy, loaded.metadata
    assert_equal binding.tuple, loaded.tuple
  end

  def test_snapshot_conflicting_with_binding_is_rejected
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]))
    poller.poll_once
    start_attempt(poller.current_workers.first)
    document = metadata_for
    document["worker_snapshot"]["endpoint"] = "http://wrong.invalid:11434"

    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::DynamicWorkerBinding.from_metadata(document)
    end
  end

  def test_idle_disappearance_only_removes_capacity
    poller = poller_for(
      snapshot(revision: 7, workers: [@worker_a]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    reconciler = reconciler_for

    poller.poll_once
    assert_empty reconciler.reconcile!(poller)
    poller.poll_once

    assert_empty reconciler.reconcile!(poller)
    assert_empty poller.current_workers
    assert_equal({ "pending" => 1 }, @store.counts)
    refute @store.dispatch_halted?
  end

  def test_readiness_change_does_not_end_an_in_flight_attempt
    not_ready = deep_copy(@worker_a).merge("state" => "NOT_READY")
    poller = poller_for(
      snapshot(revision: 7, workers: [@worker_a]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [not_ready])
    )
    poller.poll_once
    start_attempt(poller.current_workers.first)
    original_snapshot = metadata_for.fetch("worker_snapshot")
    poller.poll_once

    assert_equal original_snapshot, metadata_for.fetch("worker_snapshot")
    assert_empty reconciler_for.reconcile!(poller)
    assert_equal "running", metadata_for.fetch("status")
    assert_empty poller.ready_workers
    refute @store.dispatch_halted?
  end

  def test_in_flight_disappearance_is_durable_in_doubt_with_exact_evidence
    poller, attempt = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )

    transitions = reconciler_for.reconcile!(poller)
    metadata = metadata_for
    evidence = metadata.fetch("evidence")

    assert_equal [{
      "job_id" => "job-1", "attempt_id" => 1,
      "reason" => "worker_disappeared", "result" => "recorded_in_doubt"
    }], transitions
    assert_equal "failed", metadata.fetch("status")
    assert_equal "non_operational", metadata.fetch("failure_class")
    assert_equal "infrastructure_failed", @store.status
    assert_equal "dynamic_worker_loss", @store.dispatch_halt.fetch("kind")
    assert_equal attempt.worker_binding.execution_identity, evidence.fetch("worker_execution_identity")
    assert_equal attempt.worker_binding.registry_binding, evidence.fetch("worker_registry_binding")
    assert_equal false, evidence.fetch("outcome_known")
    assert_equal "worker_disappeared", evidence.fetch("reason")
    assert_equal poller.accepted_checkpoint.slice("registry_id", "revision", "snapshot_sha256"),
                 evidence.fetch("last_accepted_registry")
    assert_equal "disappeared", evidence.dig("reconciliation_event", "event")
    refute evidence.key?("observed_replacement_identity")
    assert_equal({ "failed" => 1 }, @store.counts)
  end

  def test_generation_replacement_at_reused_endpoint_is_not_continuation
    replacement = deep_copy(@worker_a).merge("generation_id" => "generation-b")
    poller, attempt = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [replacement])
    )

    transition = reconciler_for.reconcile!(poller).fetch(0)
    evidence = metadata_for.fetch("evidence")

    assert_equal "worker_generation_replaced", transition.fetch("reason")
    assert_equal @worker_a.fetch("endpoint"), replacement.fetch("endpoint")
    assert_equal attempt.worker_binding.execution_identity, evidence.fetch("worker_execution_identity")
    assert_equal identity_hash(replacement), evidence.fetch("observed_replacement_identity")
    assert_equal "changed", evidence.dig("reconciliation_event", "event")
    assert_equal ["generation"], evidence.dig("reconciliation_event", "details", "kinds")
    refute_equal attempt.worker_binding.tuple, poller.current_workers.first.execution_identity
    assert poller.current_workers.first.ready?
  end

  def test_endpoint_change_does_not_migrate_the_bound_attempt
    changed = deep_copy(@worker_a).merge("endpoint" => "http://127.0.0.1:11442")
    poller, = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [changed])
    )

    transition = reconciler_for.reconcile!(poller).fetch(0)
    evidence = metadata_for.fetch("evidence")

    assert_equal "worker_endpoint_changed", transition.fetch("reason")
    assert_equal @worker_a.fetch("endpoint"), evidence.dig("worker_execution_identity", "endpoint")
    assert_equal changed.fetch("endpoint"), evidence.dig("observed_replacement_identity", "endpoint")
    assert_equal ["endpoint"], evidence.dig("reconciliation_event", "details", "kinds")
  end

  def test_capability_change_does_not_change_the_bound_attempt
    changed = deep_copy(@worker_a)
    changed["labels"] = changed.fetch("labels") + ["replacement"]
    changed["capability_fingerprint"] = capability_fingerprint(changed)
    poller, = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [changed])
    )

    original_snapshot = metadata_for.fetch("worker_snapshot")
    transition = reconciler_for.reconcile!(poller).fetch(0)
    evidence = metadata_for.fetch("evidence")

    assert_equal original_snapshot, metadata_for.fetch("worker_snapshot")
    assert_equal @worker_a.fetch("labels"), original_snapshot.fetch("labels")
    assert_equal @worker_a.fetch("capabilities"), original_snapshot.fetch("capabilities")
    assert_equal "worker_capability_changed", transition.fetch("reason")
    assert_equal @worker_a.fetch("capability_fingerprint"),
                 evidence.dig("worker_execution_identity", "capability_fingerprint")
    assert_equal changed.fetch("capability_fingerprint"),
                 evidence.dig("observed_replacement_identity", "capability_fingerprint")
  end

  def test_explicit_retry_archives_loss_and_binds_a_new_attempt_to_replacement
    replacement = deep_copy(@worker_a).merge("generation_id" => "generation-b")
    poller, old_attempt = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [replacement])
    )
    reconciler_for.reconcile!(poller)
    old_metadata = metadata_for

    @store.retry_failed!(reason: "operator verified retry is safe", all: true)
    @store.clear_pause!
    retry_attempt = start_attempt(poller.current_workers.first)
    archive = JSON.parse(File.read(File.join(@output, "attempts/job-1/attempt-1/metadata.json")))

    assert_equal 2, retry_attempt.attempt_id
    assert_equal identity_hash(replacement), metadata_for.fetch("worker_execution_identity")
    archive_path = File.join(@output, "attempts/job-1/attempt-1/metadata.json")
    archive_bytes = File.binread(archive_path)
    conflicting_archive = deep_copy(archive).merge("attempt" => 2)
    assert_raises(WorkloadOrchestrator::Error) { @store.send(:write_json, archive_path, conflicting_archive) }
    assert_equal archive_bytes, File.binread(archive_path)
    assert_equal old_metadata, archive
    assert_equal old_attempt.worker_binding.worker_snapshot, archive.fetch("worker_snapshot")
    assert_equal retry_attempt.worker_binding.worker_snapshot, metadata_for.fetch("worker_snapshot")
    refute_equal archive.fetch("worker_snapshot"), metadata_for.fetch("worker_snapshot")
    assert_equal archive.dig("worker_snapshot", "endpoint"), metadata_for.dig("worker_snapshot", "endpoint")
    assert_equal "dynamic_worker_loss_in_doubt", archive.dig("evidence", "kind")
    refute @store.dispatch_halted?

    assert_equal :late_evidence,
                 @store.record_dynamic_terminal!(attempt: old_attempt, status: "complete", exit_status: 0)
    assert_equal 2, metadata_for.fetch("attempt")
    assert_equal "running", metadata_for.fetch("status")
    archived_after_late = JSON.parse(File.read(File.join(@output, "attempts/job-1/attempt-1/metadata.json")))
    assert_equal "late_dynamic_attempt_terminal", archived_after_late.dig("late_evidence", 0, "kind")
    assert_equal old_attempt.worker_binding.worker_snapshot, archived_after_late.fetch("worker_snapshot")
  end

  def test_completion_recorded_first_wins_over_later_disappearance
    poller = poller_for(
      snapshot(revision: 7, workers: [@worker_a]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    poller.poll_once
    attempt = start_attempt(poller.current_workers.first)

    assert_equal :recorded,
                 @store.record_dynamic_terminal!(attempt: attempt, status: "complete", exit_status: 0)
    poller.poll_once

    assert_empty reconciler_for.reconcile!(poller)
    assert_equal "complete", metadata_for.fetch("status")
    refute metadata_for.key?("evidence")
    refute @store.dispatch_halted?
  end

  def test_loss_recorded_first_retains_late_terminal_as_evidence
    poller, attempt = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    reconciler_for.reconcile!(poller)

    result = @store.record_dynamic_terminal!(
      attempt: attempt,
      status: "complete",
      exit_status: 0,
      evidence: { "transport" => "late fixture" }
    )
    metadata = metadata_for

    assert_equal :late_evidence, result
    assert_equal "failed", metadata.fetch("status")
    assert_equal "dynamic_worker_loss_in_doubt", metadata.dig("evidence", "kind")
    assert_equal "late_dynamic_attempt_terminal", metadata.dig("late_evidence", 0, "kind")
    assert_equal "complete", metadata.dig("late_evidence", 0, "status")
    assert_equal({ "transport" => "late fixture" }, metadata.dig("late_evidence", 0, "evidence"))
  end

  def test_resume_checkpoint_marks_persisted_running_attempt_in_doubt
    checkpoint = File.join(@output, "dynamic-workers/checkpoint.json")
    first = poller_for(snapshot(revision: 7, workers: [@worker_a]), checkpoint: checkpoint)
    first.poll_once
    start_attempt(first.current_workers.first)
    lost = poller_for(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: []),
      checkpoint: checkpoint
    )
    lost.poll_once
    resumed = poller_for(
      snapshot(revision: 9, published_at: "2030-01-01T00:00:40Z", workers: []),
      checkpoint: checkpoint
    )

    transition = reconciler_for.reconcile!(resumed).fetch(0)

    assert_equal "worker_disappeared", transition.fetch("reason")
    assert_equal "failed", metadata_for.fetch("status")
    assert_equal 8, metadata_for.dig("evidence", "last_accepted_registry", "revision")
  end

  def test_resume_honors_historical_disappearance_even_if_identity_reappears
    checkpoint = File.join(@output, "dynamic-workers/checkpoint.json")
    poller = poller_for(
      snapshot(revision: 7, workers: [@worker_a]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: []),
      snapshot(revision: 9, published_at: "2030-01-01T00:00:40Z", workers: [@worker_a]),
      checkpoint: checkpoint
    )
    poller.poll_once
    start_attempt(poller.current_workers.first)
    poller.poll_once
    poller.poll_once
    resumed = poller_for(
      snapshot(revision: 10, published_at: "2030-01-01T00:00:50Z", workers: [@worker_a]),
      checkpoint: checkpoint
    )

    transition = reconciler_for.reconcile!(resumed).fetch(0)
    evidence = metadata_for.fetch("evidence")

    assert_equal "worker_disappeared", transition.fetch("reason")
    assert_equal 8, evidence.dig("reconciliation_event", "revision")
    assert_equal "disappeared", evidence.dig("reconciliation_event", "event")
    refute evidence.key?("observed_replacement_identity")
  end

  def test_resume_checkpoint_does_not_rewrite_a_persisted_terminal_attempt
    checkpoint = File.join(@output, "dynamic-workers/checkpoint.json")
    first = poller_for(snapshot(revision: 7, workers: [@worker_a]), checkpoint: checkpoint)
    first.poll_once
    attempt = start_attempt(first.current_workers.first)
    @store.record_dynamic_terminal!(attempt: attempt, status: "complete", exit_status: 0)
    lost = poller_for(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: []),
      checkpoint: checkpoint
    )
    lost.poll_once

    assert_empty reconciler_for.reconcile!(lost)
    assert_equal "complete", metadata_for.fetch("status")
  end

  def test_dynamic_runner_reconciles_the_durable_checkpoint_before_polling
    checkpoint = File.join(@output, "dynamic-workers/checkpoint.json")
    first = poller_for(snapshot(revision: 7, workers: [@worker_a]), checkpoint: checkpoint)
    first.poll_once
    start_attempt(first.current_workers.first)
    lost = poller_for(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: []),
      checkpoint: checkpoint
    )
    lost.poll_once
    source = SequenceSource.new(
      snapshot(revision: 9, published_at: "2030-01-01T00:00:40Z", workers: [])
    )
    runner = WorkloadOrchestrator::Runner.new(
      plan: @plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_registry_sleeper: ->(*) { raise "unexpected sleep" }
    )

    assert_equal "infrastructure_failed", runner.run(resume: true)
    assert_equal 0, source.calls
    assert_equal "failed", metadata_for.fetch("status")
    assert_equal "worker_disappeared", metadata_for.dig("evidence", "reason")
  end

  def test_resume_repairs_loss_metadata_written_before_its_dispatch_halt
    poller, = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    reconciler_for.reconcile!(poller)
    state_path = File.join(@output, "execution.json")
    state = JSON.parse(File.read(state_path))
    state.delete("dispatch_halt")
    state["status"] = "running"
    File.write(state_path, "#{JSON.pretty_generate(state)}\n")
    source = SequenceSource.new(
      snapshot(revision: 9, published_at: "2030-01-01T00:00:40Z", workers: [])
    )
    runner = WorkloadOrchestrator::Runner.new(
      plan: @plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW }
    )

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true) }

    assert_includes error.message, "explicitly retry"
    assert_equal "dynamic_worker_loss", runner.store.dispatch_halt.fetch("kind")
    assert_equal "infrastructure_failed", runner.store.status
    assert_equal 0, source.calls
  end

  def test_classification_is_independent_of_registry_array_order
    replacement = deep_copy(@worker_a).merge(
      "worker_id" => "endpoint-reuser",
      "generation_id" => "generation-b"
    )
    unrelated = deep_copy(@worker_a).merge(
      "worker_id" => "unrelated",
      "generation_id" => "generation-c",
      "endpoint" => "http://127.0.0.1:11443"
    )

    first = classification_for([replacement, unrelated], "ordered")
    second = classification_for([unrelated, replacement], "reversed")

    assert_equal "worker_disappeared", first.fetch("reason")
    assert_equal first.fetch("reason"), second.fetch("reason")
    assert_equal first.fetch("observed_replacement_identity"),
                 second.fetch("observed_replacement_identity")
  end

  def test_consumer_demand_counts_uncertainty_through_retry_and_archive
    poller, old_attempt = running_attempt_then(
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    reconciler_for.reconcile!(poller)
    demand = @store.consumer_demand(pool_id: "local-pool", now: NOW)
    assert_equal 1, demand.fetch("bound_count")
    assert_equal 1, demand.fetch("uncertain_count")
    refute demand.fetch("quiescent")
    @store.authorize_retry!(reason: "reviewed", job_ids: ["job-1"])
    retry_demand = @store.consumer_demand(pool_id: "local-pool", now: NOW)
    assert_equal 1, retry_demand.fetch("bound_count")
    replacement = poller_for(snapshot(revision: 9, published_at: "2030-01-01T00:00:30Z", workers: [@worker_a]))
    replacement.poll_once
    second = start_attempt(replacement.current_workers.first)
    @store.record_dynamic_terminal!(attempt: second, status: "complete", exit_status: 0)
    @store.finish!
    archived = @store.consumer_demand(pool_id: "local-pool", now: NOW)
    assert_equal 1, archived.fetch("bound_count")
    assert_equal 1, archived.fetch("uncertain_count")
    refute archived.fetch("quiescent")
    # Existing ownership retains late evidence without resolving the in-doubt marker.
    @store.record_dynamic_terminal!(attempt: old_attempt, status: "complete", exit_status: 0)
    assert_equal 1, @store.consumer_demand(pool_id: "local-pool", now: NOW).fetch("bound_count")
  end

  private

  def running_attempt_then(next_snapshot)
    poller = poller_for(snapshot(revision: 7, workers: [@worker_a]), next_snapshot)
    poller.poll_once
    attempt = start_attempt(poller.current_workers.first)
    poller.poll_once
    [poller, attempt]
  end

  def start_attempt(worker)
    @store.record_dynamic_running!(job: @plan.jobs.first, worker: worker, environment_keys: %w[OLLAMA_HOST])
  end

  def reconciler_for(store = @store)
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: store)
  end

  def poller_for(*snapshots, checkpoint: File.join(@tmp, "checkpoint.json"))
    WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: SequenceSource.new(*snapshots),
      checkpoint_path: checkpoint,
      clock: -> { NOW }
    )
  end

  def classification_for(workers, name)
    root = File.join(@tmp, name)
    workdir = File.join(root, "work")
    output = File.join(root, "output")
    FileUtils.mkdir_p(workdir)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: output, plan: @plan, workdir: workdir)
    store.prepare!
    poller = poller_for(
      snapshot(revision: 7, workers: [@worker_a]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: workers),
      checkpoint: File.join(root, "checkpoint.json")
    )
    poller.poll_once
    store.record_dynamic_running!(job: @plan.jobs.first, worker: poller.current_workers.first,
                                  environment_keys: [])
    poller.poll_once
    reconciler_for(store).reconcile!(poller)
    JSON.parse(File.read(File.join(output, "runs/job-1/metadata.json"))).fetch("evidence")
  end

  def metadata_for
    JSON.parse(File.read(File.join(@output, "runs/job-1/metadata.json")))
  end

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def snapshot(revision:, workers:, published_at: "2030-01-01T00:00:00Z",
               expires_at: "2030-01-01T00:05:00Z")
    JSON.generate(fixture_document.merge(
                    "revision" => revision,
                    "published_at" => published_at,
                    "expires_at" => expires_at,
                    "workers" => workers
                  ))
  end

  def identity_hash(worker)
    {
      "registry_id" => fixture_document.fetch("registry_id"),
      "worker_id" => worker.fetch("worker_id"),
      "generation_id" => worker.fetch("generation_id"),
      "endpoint" => worker.fetch("endpoint"),
      "capability_fingerprint" => worker.fetch("capability_fingerprint")
    }
  end

  def capability_fingerprint(worker)
    models = worker.dig("capabilities", "ollama", "models").map do |model|
      model.slice("context_length", "digest", "fully_gpu_resident", "model")
    end
    Digest::SHA256.hexdigest(JSON.generate(
                               "gpu_id" => worker.dig("capabilities", "gpu_id"),
                               "labels" => worker.fetch("labels"),
                               "ollama_models" => models
                             ))
  end

  def deep_copy(value)
    JSON.parse(JSON.generate(value))
  end
end
