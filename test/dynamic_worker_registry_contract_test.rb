# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "time"

class DynamicWorkerRegistryContractTest < Minitest::Test
  CONTRACT_ROOT = File.expand_path("../contracts/dynamic-worker-registry/v0.1", __dir__)
  FIXTURE = File.join(CONTRACT_ROOT, "minimal-valid.json")
  INVALID_ROOT = File.join(CONTRACT_ROOT, "invalid")
  NOW = Time.iso8601("2030-01-01T00:01:00Z")
  AUTHORITATIVE_SHA256 = {
    "minimal-valid.json" =>
      "58c8f7e79b61bb454948ee045a6f7df89453a7805b54ed438df8d8b8dcb2ba26",
    "invalid/bad-contract-version.json" =>
      "2d2d1de94283ac2694b717b5a21817c4be02292296321660d4616fadf1630f45",
    "invalid/bad-endpoint.json" =>
      "06c53db11ff489ba30f49fa24d444aadd82c5b64f4fffb4ec092b9c7b000f4bd",
    "invalid/bad-fingerprint.json" =>
      "7d9afb6834594fbc85dab591a09ed4bca82735e6856c0060fdc732dcdeedf417",
    "invalid/bad-publication-window.json" =>
      "833b303395e8cd46329d40ac63c52922ded9239f9d352a7c392efdbb98e59b1c",
    "invalid/bad-state.json" =>
      "e90a4ed6e4907595479c231467a723aee4e35c7d872a6fbe507f03c7ea2a6127",
    "invalid/duplicate-worker-id.json" =>
      "fb334f5f996a0e6f16bb4880303b027443cfa16ce9f9f1693e96aced65816dbb",
    "invalid/missing-generation.json" =>
      "c0d0b86037813f05e08df80789d00152d0e086d9fd403aca9b200ab789aa8ddf"
  }.freeze

  def test_fixture_bytes_match_the_authoritative_contract
    actual_paths = [FIXTURE] + Dir[File.join(INVALID_ROOT, "*.json")]
    expected_paths = AUTHORITATIVE_SHA256.keys.map { |path| File.join(CONTRACT_ROOT, path) }

    assert_equal expected_paths.sort, actual_paths.sort
    AUTHORITATIVE_SHA256.each do |path, expected_hash|
      assert_equal expected_hash, Digest::SHA256.file(File.join(CONTRACT_ROOT, path)).hexdigest
    end
  end

  def test_canonical_fixture_identity_publication_and_capabilities
    document = fixture_document
    registry = load_registry(File.binread(FIXTURE))
    worker_record = document.fetch("workers").first
    model = worker_record.dig("capabilities", "ollama", "models").first
    worker = registry.entries.first

    assert_equal %w[contract_version expires_at published_at registry_id revision workers],
                 document.keys.sort
    assert_equal "dynamic-worker-registry/v0.1", registry.contract_version
    assert_equal "rpof-fixture", registry.registry_id
    assert_equal 7, registry.revision
    assert_equal Time.iso8601("2030-01-01T00:00:00Z"), registry.published_at
    assert_equal Time.iso8601("2030-01-01T00:05:00Z"), registry.expires_at
    assert_equal %w[
      capabilities capability_fingerprint endpoint generation_id labels state worker_id
    ], worker_record.keys.sort
    assert_equal %w[gpu_id ollama], worker_record.fetch("capabilities").keys.sort
    assert_equal ["models"], worker_record.dig("capabilities", "ollama").keys
    assert_equal %w[context_length digest fully_gpu_resident model], model.keys.sort
    assert_equal "worker-1", worker.worker_id
    assert_equal "generation-2029-12-31T23:58:00Z", worker.generation_id
    assert_equal "http://127.0.0.1:11441", worker.endpoint
    assert_equal "READY", worker.state
    assert_equal %w[inference ollama remote], worker.labels
    assert_equal "NVIDIA A40", worker.gpu_id
    assert_equal "qualified-model:latest", model.fetch("model")
    assert_equal "a" * 64, model.fetch("digest")
    assert_equal 131_072, model.fetch("context_length")
    assert model.fetch("fully_gpu_resident")
    assert_equal [
      "rpof-fixture",
      "worker-1",
      "generation-2029-12-31T23:58:00Z",
      "http://127.0.0.1:11441",
      "2995693d958654b0074ed25377b7e0a82f06b78c8411f71dcc0b4f9a0a7ea621"
    ], worker.execution_identity
  end

  def test_canonical_invalid_fixtures_fail_closed_through_runtime_validation
    expected_errors = {
      "bad-contract-version.json" => "contract must be",
      "bad-endpoint.json" => "HTTP(S) origin",
      "bad-fingerprint.json" => "does not match",
      "bad-publication-window.json" => "must follow",
      "bad-state.json" => "state must be one of",
      "duplicate-worker-id.json" => "duplicate worker_id",
      "missing-generation.json" => "missing fields: generation_id"
    }

    assert_equal expected_errors.keys.sort, Dir.children(INVALID_ROOT).sort
    expected_errors.each do |name, expected_message|
      error = assert_raises(WorkloadOrchestrator::Error) do
        load_registry(File.binread(File.join(INVALID_ROOT, name)))
      end
      assert_includes error.message, expected_message
    end
  end

  def test_capability_mutations_and_omissions_fail_closed_through_runtime_validation
    mutations = {
      "labels" => ->(worker) { worker.fetch("labels") << "changed" },
      "gpu_id" => ->(worker) { worker.fetch("capabilities")["gpu_id"] = "changed" },
      "model" => ->(worker) { model_for(worker)["model"] = "changed" },
      "digest" => ->(worker) { model_for(worker)["digest"] = "b" * 64 },
      "context_length" => ->(worker) { model_for(worker)["context_length"] = 65_536 },
      "fully_gpu_resident" => ->(worker) { model_for(worker)["fully_gpu_resident"] = false }
    }

    mutations.each_value do |mutate|
      document = fixture_document
      mutate.call(document.fetch("workers").first)
      assert_raises(WorkloadOrchestrator::Error) { load_document(document) }
    end

    %w[model digest context_length fully_gpu_resident].each do |field|
      document = fixture_document
      model_for(document.fetch("workers").first).delete(field)
      assert_raises(WorkloadOrchestrator::Error) { load_document(document) }
    end
  end

  def test_afw_plan_requirements_match_exactly
    worker = load_registry(File.binread(FIXTURE)).schedulable_workers.first
    plan = WorkloadOrchestrator::Plan.load(
      File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
    )
    requirements = plan.pools.first.ollama_requirement

    assert worker.compatible?(required_labels: [], ollama_requirement: requirements)
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirements.merge("model" => "other:model")
    )
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirements.merge("expected_digest" => "b" * 64)
    )
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirements.merge("required_context_length" => 65_536)
    )
    refute worker.compatible?(
      required_labels: [], ollama_requirement: requirements.merge("required_gpu_id" => "other")
    )
    refute worker.compatible?(required_labels: ["missing"], ollama_requirement: requirements)
  end

  def test_worker_generation_and_endpoint_changes_produce_distinct_bindings
    original = load_registry(File.binread(FIXTURE)).entries.first

    %w[worker_id generation_id endpoint].each do |field|
      document = fixture_document
      document.fetch("workers").first[field] = replacement_value(field)
      changed = load_document(document).entries.first
      refute_equal original.execution_identity, changed.execution_identity, field
    end
  end

  private

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def load_document(document)
    load_registry(JSON.generate(document))
  end

  def load_registry(bytes)
    WorkloadOrchestrator::DynamicWorkerRegistry.new(bytes, now: NOW)
  end

  def model_for(worker)
    worker.dig("capabilities", "ollama", "models").first
  end

  def replacement_value(field)
    {
      "worker_id" => "worker-replacement",
      "generation_id" => "generation-replacement",
      "endpoint" => "http://127.0.0.1:11442"
    }.fetch(field)
  end
end
