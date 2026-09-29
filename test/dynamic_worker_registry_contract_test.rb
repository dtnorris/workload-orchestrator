# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "time"
require "uri"

class DynamicWorkerRegistryContractTest < Minitest::Test
  FIXTURE = File.expand_path("fixtures/dynamic-worker-registry-v0.1.json", __dir__)
  INVALID_ROOT = File.expand_path("fixtures/dynamic-worker-registry-v0.1-invalid", __dir__)
  FIXTURE_SHA256 = "58c8f7e79b61bb454948ee045a6f7df89453a7805b54ed438df8d8b8dcb2ba26"

  def test_canonical_fixture_bytes_shape_freshness_and_fingerprint
    bytes = File.binread(FIXTURE)
    document = JSON.parse(bytes)
    worker = document.fetch("workers").first

    assert_equal FIXTURE_SHA256, Digest::SHA256.hexdigest(bytes)
    assert_equal %w[contract_version expires_at published_at registry_id revision workers],
                 document.keys.sort
    assert_equal "dynamic-worker-registry/v0.1", document.fetch("contract_version")
    assert_equal %w[
      capabilities capability_fingerprint endpoint generation_id labels state worker_id
    ], worker.keys.sort
    assert_operator Time.iso8601(document.fetch("expires_at")), :>,
                    Time.iso8601(document.fetch("published_at"))
    assert_equal fingerprint(worker), worker.fetch("capability_fingerprint")
  end

  def test_canonical_invalid_fixtures_cover_required_fail_closed_cases
    missing = invalid_fixture("missing-generation.json").fetch("workers").first
    endpoint = URI.parse(invalid_fixture("bad-endpoint.json").dig("workers", 0, "endpoint"))
    bad_fingerprint = invalid_fixture("bad-fingerprint.json").fetch("workers").first

    refute missing.key?("generation_id")
    refute_nil endpoint.userinfo
    refute_nil endpoint.query
    refute_equal fingerprint(bad_fingerprint), bad_fingerprint.fetch("capability_fingerprint")
  end

  def test_afw_plan_requirements_match_exactly
    worker = fixture.fetch("workers").first
    plan = WorkloadOrchestrator::Plan.load(
      File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
    )
    requirements = plan.pools.first.ollama_requirement

    assert eligible?(worker, requirements:, required_labels: [])
    refute eligible?(worker, requirements: requirements.merge("model" => "other:model"), required_labels: [])
    refute eligible?(worker, requirements: requirements.merge("expected_digest" => "b" * 64), required_labels: [])
    refute eligible?(worker, requirements: requirements.merge("required_context_length" => 65_536),
                     required_labels: [])
    refute eligible?(worker, requirements:, required_labels: ["missing"])
    refute eligible?(worker.merge("state" => "NOT_READY"), requirements:, required_labels: [])
  end

  def test_endpoint_reuse_does_not_reuse_generation_binding
    registry = fixture
    worker = registry.fetch("workers").first
    replacement = worker.merge("generation_id" => "generation-replacement")

    refute_equal binding_identity(registry, worker), binding_identity(registry, replacement)
    assert_equal worker.fetch("endpoint"), replacement.fetch("endpoint")
  end

  private

  def fixture
    JSON.parse(File.read(FIXTURE))
  end

  def invalid_fixture(name)
    JSON.parse(File.read(File.join(INVALID_ROOT, name)))
  end

  def fingerprint(worker)
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

  def eligible?(worker, requirements:, required_labels:)
    return false unless worker["state"] == "READY"
    return false unless (required_labels - worker.fetch("labels")).empty?
    capabilities = worker.fetch("capabilities")
    return false if requirements.key?("required_gpu_id") &&
                    capabilities.fetch("gpu_id") != requirements.fetch("required_gpu_id")

    model = capabilities.dig("ollama", "models").find do |candidate|
      candidate.fetch("model") == requirements.fetch("model") &&
        candidate.fetch("digest") == requirements.fetch("expected_digest")
    end
    return false unless model
    return false if requirements.key?("required_context_length") &&
                    model.fetch("context_length") != requirements.fetch("required_context_length")
    return false if requirements["require_fully_gpu_resident"] &&
                    model.fetch("fully_gpu_resident") != true

    true
  end

  def binding_identity(registry, worker)
    [registry.fetch("registry_id")] +
      %w[worker_id generation_id endpoint capability_fingerprint].map { |key| worker.fetch(key) }
  end
end
