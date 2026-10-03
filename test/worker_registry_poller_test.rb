# frozen_string_literal: true

require_relative "test_helper"

class WorkerRegistryPollerTest < Minitest::Test
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
    @tmp = Dir.mktmpdir("wlo-worker-poller-")
    @checkpoint = File.join(@tmp, "checkpoint.json")
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_worker_arrives_and_then_disappears
    worker = fixture_document.fetch("workers").first
    source = SequenceSource.new(
      snapshot(revision: 7, workers: []),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [worker]),
      snapshot(revision: 9, published_at: "2030-01-01T00:00:40Z", workers: [])
    )
    poller = build_poller(source)

    poller.poll_once
    assert_empty poller.current_workers
    assert poller.current_workers.frozen?

    poller.poll_once
    assert_equal ["worker-1"], poller.ready_workers.map(&:worker_id)
    assert_equal ["worker-1"], poller.last_reconciliation.fetch("arrived").map { |row| row.fetch("worker_id") }

    poller.poll_once
    assert_empty poller.current_workers
    removed = poller.last_reconciliation.fetch("disappeared").fetch(0)
    assert_equal "worker-1", removed.fetch("worker_id")
    assert_equal 5, removed.fetch("execution_identity").length
    poller.poll_once
    historical_loss = poller.reconciliation_history.last.dig("changes", "disappeared", 0)
    assert_equal removed, historical_loss
  end

  def test_replacement_at_same_endpoint_is_a_generation_change
    original = fixture_document.fetch("workers").first
    replacement = deep_copy(original).merge("generation_id" => "replacement-generation")
    source = SequenceSource.new(
      snapshot(revision: 7, workers: [original]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [replacement])
    )
    poller = build_poller(source)

    poller.poll_once
    first_identity = poller.current_workers.first.execution_identity
    poller.poll_once
    change = poller.last_reconciliation.fetch("changed").fetch(0)

    assert_equal ["generation"], change.fetch("kinds")
    assert_equal original.fetch("endpoint"), replacement.fetch("endpoint")
    refute_equal first_identity, poller.current_workers.first.execution_identity
  end

  def test_state_endpoint_and_capability_changes_are_explicit
    original = fixture_document.fetch("workers").first
    changed = deep_copy(original)
    changed["endpoint"] = "http://127.0.0.1:11442"
    changed["state"] = "NOT_READY"
    changed["labels"] = %w[inference ollama remote replacement]
    changed["capability_fingerprint"] = capability_fingerprint(changed)
    source = SequenceSource.new(
      snapshot(revision: 7, workers: [original]),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [changed])
    )
    poller = build_poller(source)

    poller.poll_once
    poller.poll_once

    assert_empty poller.ready_workers
    assert_equal %w[endpoint capability state],
                 poller.last_reconciliation.fetch("changed").fetch(0).fetch("kinds")
  end

  def test_revision_rollback_fails_closed
    source = SequenceSource.new(
      snapshot(revision: 8, workers: []),
      snapshot(revision: 7, published_at: "2029-12-31T23:59:30Z", workers: [])
    )
    poller = build_poller(source)
    poller.poll_once

    error = assert_raises(WorkloadOrchestrator::Error) { poller.poll_once }
    assert_includes error.message, "rolled back"
  end

  def test_same_revision_with_changed_bytes_fails_closed
    worker = fixture_document.fetch("workers").first
    changed = deep_copy(worker).merge("generation_id" => "changed-generation")
    source = SequenceSource.new(
      snapshot(revision: 7, workers: [worker]),
      snapshot(revision: 7, workers: [changed])
    )
    poller = build_poller(source)
    poller.poll_once

    error = assert_raises(WorkloadOrchestrator::Error) { poller.poll_once }
    assert_includes error.message, "changed contents"
  end

  def test_expired_snapshot_fails_closed
    source = SequenceSource.new(snapshot(revision: 7, expires_at: NOW.iso8601, workers: []))

    error = assert_raises(WorkloadOrchestrator::Error) { build_poller(source).poll_once }
    assert_includes error.message, "expired"
  end

  def test_snapshot_is_validated_against_time_observed_after_source_returns
    clock_state = { now: Time.iso8601("2030-01-01T00:00:00.999999Z") }
    source = Class.new(SequenceSource) do
      define_method(:latest_snapshot) do
        clock_state[:now] = Time.iso8601("2030-01-01T00:00:01.000001Z")
        super()
      end
    end.new(snapshot(revision: 7, published_at: "2030-01-01T00:00:01Z", workers: []))
    poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: source,
      checkpoint_path: @checkpoint,
      clock: -> { clock_state.fetch(:now) }
    )

    poller.poll_once

    assert_equal Time.iso8601("2030-01-01T00:00:01Z"), poller.registry.published_at
    assert_equal "2030-01-01T00:00:01Z", poller.accepted_checkpoint.fetch("published_at")
    assert_equal "2030-01-01T00:00:01Z", poller.accepted_checkpoint.fetch("accepted_at")
  end

  def test_future_dated_snapshot_still_fails_closed
    source = SequenceSource.new(
      snapshot(revision: 7, published_at: "2030-01-01T00:01:01Z", workers: [])
    )

    error = assert_raises(WorkloadOrchestrator::Error) { build_poller(source).poll_once }

    assert_includes error.message, "future-dated"
    refute File.exist?(@checkpoint)
  end

  def test_resume_retains_registry_identity_revision_and_worker_binding
    worker = fixture_document.fetch("workers").first
    first = build_poller(SequenceSource.new(snapshot(revision: 8, workers: [worker])))
    first.poll_once

    replacement = deep_copy(worker).merge("generation_id" => "replacement-generation")
    resumed = build_poller(SequenceSource.new(
      snapshot(revision: 9, published_at: "2030-01-01T00:00:20Z", workers: [replacement])
    ))
    resumed.poll_once

    change = resumed.last_reconciliation.fetch("changed").fetch(0)
    assert_equal ["generation"], change.fetch("kinds")
    assert_equal worker.fetch("generation_id"), change.dig("previous", "generation_id")

    rollback = build_poller(SequenceSource.new(snapshot(revision: 7, workers: [])))
    error = assert_raises(WorkloadOrchestrator::Error) { rollback.poll_once }
    assert_includes error.message, "rolled back"
  end

  def test_checkpoint_retains_the_validated_immutable_worker_snapshot
    worker = fixture_document.fetch("workers").first
    poller = build_poller(SequenceSource.new(snapshot(revision: 7, workers: [worker])))

    poller.poll_once

    row = poller.accepted_checkpoint.fetch("workers").first
    persisted = row.fetch("worker_snapshot")
    assert_equal worker.fetch("labels"), persisted.fetch("labels")
    assert_equal worker.fetch("capabilities"), persisted.fetch("capabilities")
    assert_equal worker.fetch("generation_id"), persisted.fetch("generation_id")
    assert_equal 7, persisted.fetch("registry_revision")
    assert_equal poller.registry.sha256, persisted.fetch("registry_snapshot_sha256")
  end

  def test_legacy_checkpoint_without_worker_snapshot_remains_resumable
    worker = fixture_document.fetch("workers").first
    bytes = snapshot(revision: 7, workers: [worker])
    build_poller(SequenceSource.new(bytes)).poll_once
    legacy = JSON.parse(File.read(@checkpoint))
    legacy.fetch("workers").each { |row| row.delete("worker_snapshot") }
    legacy.fetch("last_reconciliation").each_value do |rows|
      rows.each { |row| row.delete("worker_snapshot") if row.is_a?(Hash) }
    end
    File.write(@checkpoint, JSON.generate(legacy))

    resumed = build_poller(SequenceSource.new(bytes))
    resumed.poll_once

    assert_equal 7, resumed.accepted_checkpoint.fetch("revision")
    assert resumed.accepted_checkpoint.dig("workers", 0).key?("worker_snapshot")
  end

  def test_resume_rejects_registry_identity_change_and_same_revision_byte_change
    build_poller(SequenceSource.new(snapshot(revision: 8, workers: []))).poll_once

    changed_id = snapshot(revision: 9, published_at: "2030-01-01T00:00:20Z", workers: [])
    changed_id = JSON.parse(changed_id).merge("registry_id" => "different-registry")
    error = assert_raises(WorkloadOrchestrator::Error) do
      build_poller(SequenceSource.new(JSON.generate(changed_id))).poll_once
    end
    assert_includes error.message, "identity changed"

    changed_bytes = snapshot(revision: 8, workers: []) + " "
    error = assert_raises(WorkloadOrchestrator::Error) do
      build_poller(SequenceSource.new(changed_bytes)).poll_once
    end
    assert_includes error.message, "changed contents"
  end

  def test_polling_cadence_is_injectable_and_empty_registry_keeps_loop_alive
    source = SequenceSource.new(snapshot(revision: 7, workers: []))
    waits = []
    stop = false
    sleeper = lambda do |seconds, _condition|
      waits << seconds
      stop = true
    end
    poller = build_poller(source, sleeper: sleeper)

    poller.run(stop: -> { stop })

    assert_equal [WorkloadOrchestrator::WorkerRegistryPoller::DEFAULT_INTERVAL_SECONDS], waits
    assert_equal 1, source.calls
    assert_empty poller.current_workers
  end

  def test_stop_condition_prevents_a_new_poll
    source = SequenceSource.new(snapshot(revision: 7, workers: []))

    build_poller(source).run(stop: -> { true })

    assert_equal 0, source.calls
  end

  private

  def build_poller(source, sleeper: nil)
    WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: source,
      checkpoint_path: @checkpoint,
      clock: -> { NOW },
      sleeper: sleeper
    )
  end

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def snapshot(revision:, workers:, published_at: "2030-01-01T00:00:00Z",
               expires_at: "2030-01-01T00:05:00Z")
    document = fixture_document.merge(
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => expires_at,
      "workers" => workers
    )
    JSON.generate(document)
  end

  def capability_fingerprint(worker)
    capabilities = worker.fetch("capabilities")
    models = capabilities.dig("ollama", "models").map do |model|
      {
        "context_length" => model.fetch("context_length"),
        "digest" => model.fetch("digest"),
        "fully_gpu_resident" => model.fetch("fully_gpu_resident"),
        "model" => model.fetch("model")
      }
    end
    Digest::SHA256.hexdigest(JSON.generate(
                               "gpu_id" => capabilities.fetch("gpu_id"),
                               "labels" => worker.fetch("labels"),
                               "ollama_models" => models
                             ))
  end

  def deep_copy(value)
    JSON.parse(JSON.generate(value))
  end
end
