# frozen_string_literal: true

require_relative "test_helper"

class WorkerSourceTest < Minitest::Test
  include WloTestSupport

  CONTRACT_ROOT = File.expand_path("../contracts/dynamic-worker-registry/v0.1", __dir__)
  EXAMPLE_SOURCE_ROOT = File.expand_path("../examples/worker-sources", __dir__)
  FIXTURE = File.join(CONTRACT_ROOT, "minimal-valid.json")
  INVALID_ROOT = File.join(CONTRACT_ROOT, "invalid")
  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class FixtureSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(bytes)
      super()
      @bytes = bytes
      @calls = 0
    end

    def latest_snapshot
      @calls += 1
      @bytes
    end
  end

  def test_valid_canonical_fixture_becomes_immutable_generic_worker
    registry = load_registry(File.binread(FIXTURE))
    worker = registry.schedulable_workers.fetch(0)

    assert_equal "dynamic-worker-registry/v0.1", registry.contract_version
    assert_equal "rpof-fixture", registry.registry_id
    assert_equal 7, registry.revision
    assert_equal 1, registry.entries.length
    assert_equal registry.entries, registry.schedulable_workers
    assert_equal "worker-1", worker.worker_id
    assert_equal "generation-2029-12-31T23:58:00Z", worker.generation_id
    assert_equal "http://127.0.0.1:11441", worker.endpoint
    assert_equal "READY", worker.state
    assert_equal %w[inference ollama remote], worker.labels
    assert_equal "NVIDIA A40", worker.gpu_id
    assert_equal 131_072, worker.ollama_models.first.fetch("context_length")
    assert worker.ollama_models.first.fetch("fully_gpu_resident")
    assert_equal registry.published_at, worker.published_at
    assert_equal registry.expires_at, worker.expires_at
    assert_equal registry.sha256, worker.snapshot_sha256
    assert registry.frozen?
    assert registry.document.frozen?
    assert registry.document.fetch("workers").first.fetch("capabilities").frozen?
    assert worker.frozen?
    assert worker.labels.frozen?
    assert worker.ollama_models.frozen?
  end

  def test_canonical_invalid_fixtures_fail_closed
    errors = Dir[File.join(INVALID_ROOT, "*.json")].to_h do |path|
      error = assert_raises(WorkloadOrchestrator::Error) do
        load_registry(File.binread(path))
      end
      [File.basename(path), error.message]
    end

    assert_includes errors.fetch("bad-endpoint.json"), "HTTP(S) origin"
    assert_includes errors.fetch("bad-fingerprint.json"), "does not match"
    assert_includes errors.fetch("missing-generation.json"), "missing fields: generation_id"
  end

  def test_only_ready_entries_are_schedulable
    ready = fixture_document.fetch("workers").first
    unavailable = replacement(ready, worker_id: "worker-2", endpoint: "http://127.0.0.1:11442")
    unavailable["state"] = "UNAVAILABLE"
    not_ready = replacement(ready, worker_id: "worker-3", endpoint: "http://127.0.0.1:11443")
    not_ready["state"] = "NOT_READY"
    registry = load_document(fixture_document.merge("workers" => [ready, unavailable, not_ready]))

    assert_equal %w[READY UNAVAILABLE NOT_READY], registry.entries.map(&:state)
    assert_equal ["worker-1"], registry.schedulable_workers.map(&:worker_id)
  end

  def test_expired_and_future_dated_snapshots_are_rejected
    expired = fixture_document.merge("expires_at" => "2030-01-01T00:01:00Z")
    future = fixture_document.merge(
      "published_at" => "2030-01-01T00:02:00Z",
      "expires_at" => "2030-01-01T00:07:00Z"
    )

    assert_error_includes("expired") { load_document(expired) }
    assert_error_includes("future-dated") { load_document(future) }
  end

  def test_duplicate_worker_identity_and_endpoint_are_rejected
    worker = fixture_document.fetch("workers").first
    duplicate_id = replacement(worker, worker_id: worker.fetch("worker_id"),
                                       endpoint: "http://127.0.0.1:11442")
    duplicate_endpoint = replacement(worker, worker_id: "worker-2",
                                             endpoint: "http://127.0.0.1:11441/")

    assert_error_includes("duplicate worker_id") do
      load_document(fixture_document.merge("workers" => [worker, duplicate_id]))
    end
    assert_error_includes("duplicate worker endpoint") do
      load_document(fixture_document.merge("workers" => [worker, duplicate_endpoint]))
    end
  end

  def test_replacement_generation_has_distinct_execution_identity
    first = load_document(fixture_document)
    replacement_document = fixture_document
    replacement_document["revision"] = 8
    replacement_document["published_at"] = "2030-01-01T00:00:30Z"
    replacement_document["expires_at"] = "2030-01-01T00:07:00Z"
    replacement_document["workers"][0]["generation_id"] = "generation-replacement"
    second = load_document(replacement_document, previous: first)

    refute_equal first.schedulable_workers.first.execution_identity,
                 second.schedulable_workers.first.execution_identity
    assert_equal first.schedulable_workers.first.endpoint,
                 second.schedulable_workers.first.endpoint
  end

  def test_capability_matching_inputs_are_preserved_exactly
    worker = load_registry(File.binread(FIXTURE)).schedulable_workers.first
    requirement = {
      "model" => "qualified-model:latest",
      "expected_digest" => "a" * 64,
      "required_context_length" => 131_072,
      "require_fully_gpu_resident" => true,
      "required_gpu_id" => "NVIDIA A40"
    }

    assert worker.compatible?(required_labels: %w[inference remote], ollama_requirement: requirement)
    refute worker.compatible?(required_labels: ["missing"], ollama_requirement: requirement)
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirement.merge("expected_digest" => "b" * 64)
    )
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirement.merge("required_context_length" => 65_536)
    )
  end

  def test_worker_source_is_replaceable_and_static_source_preserves_bytes
    bytes = File.binread(FIXTURE)
    fixture_source = FixtureSource.new(bytes)
    static_source = WorkloadOrchestrator::StaticWorkerSource.new(bytes)

    first = WorkloadOrchestrator::DynamicWorkerRegistry.from_source(fixture_source, now: NOW)
    second = WorkloadOrchestrator::DynamicWorkerRegistry.from_source(static_source, now: NOW)

    assert_equal 1, fixture_source.calls
    assert_equal first.sha256, second.sha256
    assert_same static_source.latest_snapshot, static_source.latest_snapshot
    assert static_source.latest_snapshot.frozen?
  end

  def test_checked_in_worker_source_examples_load
    expected_source_counts = {
      "local-only.yml" => 1,
      "remote-only.yml" => 1,
      "mixed.yml" => 2
    }

    File.stub(:directory?, true) do
      expected_source_counts.each do |filename, expected_count|
        source_set = WorkloadOrchestrator::WorkerSourceConfiguration.load(
          File.join(EXAMPLE_SOURCE_ROOT, filename)
        )

        assert_equal expected_count, source_set.entries.length, filename
      end
    end
  end

  def test_command_source_executes_argv_without_a_shell_and_preserves_stdout_bytes
    code = "require 'json'; STDOUT.write(JSON.generate(ARGV))"
    source = WorkloadOrchestrator::CommandWorkerSource.new(
      RbConfig.ruby, ["-e", code, "two words", "--json", "literal;not-a-shell"]
    )

    assert_equal(
      ["two words", "--json", "literal;not-a-shell"],
      JSON.parse(source.latest_snapshot)
    )
    assert_equal [RbConfig.ruby, "-e", code, "two words", "--json", "literal;not-a-shell"],
                 source.argv
  end

  def test_command_source_reports_nonzero_exit_and_stderr
    source = WorkloadOrchestrator::CommandWorkerSource.new(
      RbConfig.ruby, ["-e", "warn 'registry unavailable'; exit 23"]
    )

    error = assert_raises(WorkloadOrchestrator::Error) { source.latest_snapshot }

    assert_includes error.message, "exit 23"
    assert_includes error.message, "registry unavailable"
  end

  def test_command_source_reports_exec_failure
    source = WorkloadOrchestrator::CommandWorkerSource.new(
      File.join(Dir.tmpdir, "missing-worker-source-#{Process.pid}")
    )

    error = assert_raises(WorkloadOrchestrator::Error) { source.latest_snapshot }

    assert_includes error.message, "cannot execute worker source command"
  end

  def test_contract_shape_state_and_reconciliation_fail_closed
    wrong_version = fixture_document.merge("contract_version" => "dynamic-worker-registry/v0.2")
    provider_field = fixture_document
    provider_field["workers"][0]["pod_id"] = "provider-detail"
    invalid_state = fixture_document
    invalid_state["workers"][0]["state"] = "BOOTING"

    assert_error_includes("contract must be") { load_document(wrong_version) }
    assert_error_includes("unknown fields: pod_id") { load_document(provider_field) }
    assert_error_includes("state must be one of") { load_document(invalid_state) }

    previous = load_document(fixture_document)
    rollback = fixture_document.merge("revision" => 6)
    changed_same_revision = fixture_document
    changed_same_revision["workers"][0]["generation_id"] = "changed"
    stale_publication = fixture_document.merge("revision" => 8)

    assert_error_includes("rolled back") { load_document(rollback, previous: previous) }
    assert_error_includes("changed contents") do
      load_document(changed_same_revision, previous: previous)
    end
    assert_error_includes("did not advance") do
      load_document(stale_publication, previous: previous)
    end
    repeated = load_registry(previous.bytes, previous: previous)
    assert_equal previous.sha256, repeated.sha256
    assert_equal previous.revision, repeated.revision
  end

  def test_existing_static_worker_configuration_remains_unchanged
    Dir.mktmpdir("wlo-static-workers-") do |root|
      plan = WorkloadOrchestrator::Plan.load(
        write_plan(root, jobs: [job("one", code: "exit 0")])
      )
      workers = WorkloadOrchestrator::WorkerSet.load(write_workers(root))

      assert_same plan, workers.validate_plan!(plan)
      assert_equal "local", workers.fetch("local").name
      assert_equal "command", workers.fetch("local").type
    end
  end

  private

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def load_document(document, previous: nil)
    load_registry(JSON.generate(document), previous: previous)
  end

  def load_registry(bytes, previous: nil)
    source = WorkloadOrchestrator::StaticWorkerSource.new(bytes)
    WorkloadOrchestrator::DynamicWorkerRegistry.from_source(
      source, now: NOW, previous: previous
    )
  end

  def replacement(worker, worker_id:, endpoint:)
    JSON.parse(JSON.generate(worker)).merge(
      "worker_id" => worker_id,
      "generation_id" => "#{worker.fetch('generation_id')}-replacement",
      "endpoint" => endpoint
    )
  end

  def assert_error_includes(text, &)
    error = assert_raises(WorkloadOrchestrator::Error, &)
    assert_includes error.message, text
  end
end
