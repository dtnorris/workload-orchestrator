# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "open3"

class OllamaCapabilityRequestTest < Minitest::Test
  CONTRACT_ROOT = File.expand_path("../contracts/ollama-capability-request/v0.1", __dir__)
  FIXTURE = File.join(CONTRACT_ROOT, "canonical-valid.json")
  EXPECTED_NORMALIZED_JSON = <<~JSON.chomp.freeze
    {"ollama":{"model":"qualified-model:latest","expected_digest":"#{'a' * 64}","required_context_length":131072,"require_fully_gpu_resident":true,"required_gpu_id":"NVIDIA A40"}}
  JSON
  EXPECTED_FINGERPRINT = "9121fe00d663bad2e5bd6f2ff4d6b492e66ba71b0843585c547321172dce5ae4"

  def test_canonical_valid_fixture_parses_with_documented_fingerprint
    request = load_fixture

    assert_equal "ollama-capability-request/v0.1", request.contract_version
    assert_equal "qualified-model:latest", request.model
    assert_equal "a" * 64, request.expected_digest
    assert_equal 131_072, request.required_context_length
    assert request.require_fully_gpu_resident
    assert_equal "NVIDIA A40", request.required_gpu_id
    assert_equal EXPECTED_NORMALIZED_JSON, request.normalized_json
    assert_equal EXPECTED_FINGERPRINT, request.fingerprint
    assert_equal Digest::SHA256.hexdigest(request.normalized_json), request.fingerprint
    assert request.normalized_json.encoding == Encoding::UTF_8
    refute request.normalized_json.end_with?("\n")
  end

  def test_equivalent_input_key_order_has_identical_normalization_and_fingerprint
    document = fixture_document
    reordered = {
      "ollama" => document.fetch("ollama").to_a.reverse.to_h,
      "contract_version" => document.fetch("contract_version")
    }
    request = build_request(reordered)

    assert_equal load_fixture.normalized_json, request.normalized_json
    assert_equal load_fixture.fingerprint, request.fingerprint
  end

  def test_every_runtime_capability_field_affects_normalization_and_fingerprint
    original = load_fixture
    mutations = {
      "model" => "other-model:latest",
      "expected_digest" => "b" * 64,
      "required_context_length" => 65_536,
      "require_fully_gpu_resident" => false,
      "required_gpu_id" => "NVIDIA L40S"
    }

    mutations.each do |field, replacement|
      document = fixture_document
      document.fetch("ollama")[field] = replacement
      changed = build_request(document)

      refute_equal original.normalized_json, changed.normalized_json, field
      refute_equal original.fingerprint, changed.fingerprint, field
    end

    document = fixture_document
    document.fetch("ollama").delete("required_gpu_id")
    without_gpu = build_request(document)
    refute_equal original.normalized_json, without_gpu.normalized_json
    refute_equal original.fingerprint, without_gpu.fingerprint
  end

  def test_external_provenance_fields_are_rejected_instead_of_ignored
    %w[
      batch_handle production_batch_id plan_id plan_sha256 model_alias pool_id
      adventure_finder_label provider_id fleet_id campaign_id budget worker_id
      generation_id endpoint
    ].each do |field|
      document = fixture_document.merge(field => "external-provenance")
      error = assert_raises(WorkloadOrchestrator::Error, field) { build_request(document) }
      assert_includes error.message, "unknown fields: #{field}", field
    end
  end

  def test_malformed_runtime_fields_fail_closed
    invalid_values = {
      "model" => ["", " model", "model\n", 1, nil, "m" * 257],
      "expected_digest" => ["a" * 63, "A" * 64, "g" * 64, 1, nil],
      "required_context_length" => [0, -1, 1.0, "131072", true, nil],
      "require_fully_gpu_resident" => [1, 0, "true", nil],
      "required_gpu_id" => ["", " GPU", "GPU\n", 1, nil, "g" * 257]
    }

    invalid_values.each do |field, values|
      values.each do |value|
        document = fixture_document
        document.fetch("ollama")[field] = value
        assert_raises(WorkloadOrchestrator::Error, "#{field}=#{value.inspect}") do
          build_request(document)
        end
      end
    end
  end

  def test_optional_gpu_id_absence_is_valid_and_deterministic
    document = fixture_document
    document.fetch("ollama").delete("required_gpu_id")
    first = build_request(document)
    second = build_request(
      "ollama" => document.fetch("ollama").to_a.reverse.to_h,
      "contract_version" => document.fetch("contract_version")
    )

    assert_nil first.required_gpu_id
    refute first.normalized_request.fetch("ollama").key?("required_gpu_id")
    assert_equal first.normalized_json, second.normalized_json
    assert_equal first.fingerprint, second.fingerprint
    assert_equal "f4c5e1c85bd19070bb7529dd8683796db6727269ec730a58fd89c26c36be2546",
                 first.fingerprint
  end

  def test_contract_version_missing_fields_unknown_fields_and_duplicate_keys_fail_closed
    wrong_version = fixture_document.merge("contract_version" => "ollama-capability-request/v9")
    assert_raises(WorkloadOrchestrator::Error) { build_request(wrong_version) }

    %w[contract_version ollama].each do |field|
      assert_raises(WorkloadOrchestrator::Error, field) do
        build_request(fixture_document.reject { |key, _value| key == field })
      end
    end

    %w[model expected_digest required_context_length require_fully_gpu_resident].each do |field|
      document = fixture_document
      document.fetch("ollama").delete(field)
      assert_raises(WorkloadOrchestrator::Error, field) { build_request(document) }
    end

    document = fixture_document.merge("unknown" => true)
    assert_raises(WorkloadOrchestrator::Error) { build_request(document) }
    document = fixture_document
    document.fetch("ollama")["unknown"] = true
    assert_raises(WorkloadOrchestrator::Error) { build_request(document) }

    duplicate = <<~JSON
      {"contract_version":"ollama-capability-request/v0.1","contract_version":"ollama-capability-request/v0.1","ollama":{}}
    JSON
    assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::OllamaCapabilityRequest.new(duplicate) }
  end

  def test_parser_has_no_adventurefinder_or_worker_publisher_runtime_dependency
    root = File.expand_path("..", __dir__)
    script = <<~'RUBY'
      require "workload_orchestrator/ollama_capability_request"
      require File.join(ARGV.fetch(0), "test/support/sibling_implementation_boundary")
      forbidden = SiblingImplementationBoundary.forbidden_features(
        $LOADED_FEATURES, root: ARGV.fetch(0),
        pattern: /adventure[_-]?finder|local_ollama_workers|runpod_ollama_fleet/i
      )
      abort forbidden.join("\n") unless forbidden.empty?
      puts WorkloadOrchestrator::OllamaCapabilityRequest::CONTRACT_VERSION
    RUBY
    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby, "-I#{File.join(root, 'lib')}", "-e", script, root
    )

    assert status.success?, stderr
    assert_equal "ollama-capability-request/v0.1\n", stdout
    assert_empty stderr
  end

  private

  def load_fixture
    WorkloadOrchestrator::OllamaCapabilityRequest.load(FIXTURE)
  end

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def build_request(document)
    WorkloadOrchestrator::OllamaCapabilityRequest.new(JSON.generate(document))
  end
end
