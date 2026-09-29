# frozen_string_literal: true

require_relative "test_helper"

class CapabilityMatcherTest < Minitest::Test
  include WloTestSupport

  PLAN_FIXTURE = File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
  REGISTRY_FIXTURE = File.expand_path("fixtures/dynamic-worker-registry-v0.1.json", __dir__)
  NOW = Time.iso8601("2030-01-01T00:01:00Z")
  REQUIREMENT = {
    "model" => "qualified-model:latest",
    "expected_digest" => "a" * 64,
    "required_context_length" => 131_072,
    "require_fully_gpu_resident" => true,
    "required_gpu_id" => "NVIDIA A40"
  }.freeze

  def test_exact_match_is_accepted_with_no_mismatch_reasons
    result = match

    assert result.compatible?
    assert_empty result.reasons
  end

  def test_non_ready_states_are_rejected
    not_ready = match(worker: registry_worker(state: "NOT_READY"))
    unavailable = match(worker: registry_worker(state: "UNAVAILABLE"))

    refute not_ready.compatible?
    assert_equal ["worker_not_ready"], not_ready.reasons
    refute unavailable.compatible?
    assert_equal ["worker_not_ready"], unavailable.reasons
  end

  def test_exact_model_digest_and_context_are_required
    wrong_model = match(models: [model_capability("model" => "other-model:latest")])
    wrong_digest = match(models: [model_capability("digest" => "b" * 64)])
    wrong_context = match(models: [model_capability("context_length" => 65_536)])

    assert_equal ["model_mismatch"], wrong_model.reasons
    assert_equal ["digest_mismatch"], wrong_digest.reasons
    assert_equal ["context_length_mismatch"], wrong_context.reasons
  end

  def test_full_gpu_residency_must_be_explicitly_true_when_required
    false_result = match(models: [model_capability("fully_gpu_resident" => false)])
    missing_model = model_capability
    missing_model.delete("fully_gpu_resident")
    missing_result = match(models: [missing_model])

    assert_equal ["full_gpu_residency_required"], false_result.reasons
    assert_equal ["missing_capability"], missing_result.reasons
  end

  def test_gpu_identity_is_exact_only_when_required
    mismatch = match(worker: registry_worker(gpu_id: "NVIDIA L40S"))
    unrestricted = match(
      worker: registry_worker(gpu_id: "NVIDIA L40S"),
      requirement: REQUIREMENT.except("required_gpu_id")
    )

    assert_equal ["gpu_id_mismatch"], mismatch.reasons
    assert unrestricted.compatible?
  end

  def test_required_labels_are_exact_and_extra_labels_are_harmless
    missing = match(worker: registry_worker(labels: %w[inference]), labels: %w[inference remote])
    extra = match(
      worker: registry_worker(labels: %w[inference ollama remote surplus]),
      labels: %w[inference remote]
    )

    assert_equal ["missing_required_label"], missing.reasons
    assert extra.compatible?
  end

  def test_one_model_record_must_satisfy_the_complete_requirement
    complete = model_capability
    other = model_capability("model" => "other-model:latest", "digest" => "b" * 64)
    matching = match(models: [other, complete])

    target_with_wrong_digest = model_capability("digest" => "b" * 64)
    other_with_right_digest = model_capability("model" => "other-model:latest")
    split = match(models: [target_with_wrong_digest, other_with_right_digest])

    assert matching.compatible?
    assert_equal ["digest_mismatch"], split.reasons
  end

  def test_endpoint_and_execution_identity_do_not_change_capability_eligibility
    first = registry_worker(worker_id: "worker-a", endpoint: "http://127.0.0.1:11441")
    second = registry_worker(
      registry_id: "another-registry",
      worker_id: "provider-shaped-name",
      generation_id: "different-generation",
      endpoint: "https://worker.example.test:443",
      fingerprint: "b" * 64
    )

    assert match(worker: first).compatible?
    assert match(worker: second).compatible?
  end

  def test_job_resolves_to_its_existing_pool_requirement
    plan = WorkloadOrchestrator::Plan.load(PLAN_FIXTURE)
    matcher = WorkloadOrchestrator::CapabilityMatcher.new(plan: plan)
    job = plan.jobs.first

    assert_same plan.pool(job.pool_id), matcher.requirement_for(job: job)
    assert matcher.match_job(job: job, worker: registry_worker).compatible?
  end

  def test_compatible_workers_are_sorted_by_complete_execution_identity
    plan = WorkloadOrchestrator::Plan.load(PLAN_FIXTURE)
    matcher = WorkloadOrchestrator::CapabilityMatcher.new(plan: plan)
    workers = [
      registry_worker(registry_id: "b", worker_id: "worker-a"),
      registry_worker(registry_id: "a", worker_id: "worker-z", generation_id: "g-2"),
      registry_worker(registry_id: "a", worker_id: "worker-z", generation_id: "g-1"),
      registry_worker(state: "NOT_READY", worker_id: "worker-ineligible")
    ]

    result = matcher.compatible_workers(job: plan.jobs.first, workers: workers.reverse)

    assert_equal workers.first(3).sort_by(&:execution_identity), result
    assert result.frozen?
  end

  def test_malformed_plan_and_registry_remain_fail_closed
    plan_document = JSON.parse(File.read(PLAN_FIXTURE))
    plan_document.dig("pools", 0, "requirements", "ollama").delete("expected_digest")
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::Plan.new(JSON.generate(plan_document))
    end

    registry_document = JSON.parse(File.read(REGISTRY_FIXTURE))
    registry_document.dig("workers", 0, "capabilities", "ollama", "models", 0)
                     .delete("fully_gpu_resident")
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(registry_document), now: NOW)
    end
  end

  def test_existing_registry_worker_predicate_delegates_to_the_matcher
    worker = registry_worker

    assert worker.compatible?(required_labels: %w[inference remote], ollama_requirement: REQUIREMENT)
    refute worker.compatible?(
      required_labels: %w[inference remote],
      ollama_requirement: REQUIREMENT.merge("expected_digest" => "b" * 64)
    )
  end

  private

  def match(worker: nil, models: nil, labels: %w[inference remote], requirement: REQUIREMENT)
    worker ||= registry_worker(models: models || [model_capability])
    WorkloadOrchestrator::CapabilityMatcher.match(
      worker: worker,
      required_labels: labels,
      ollama_requirement: requirement
    )
  end

  def registry_worker(**overrides)
    values = {
      registry_id: "rpof-fixture",
      worker_id: "worker-1",
      generation_id: "generation-1",
      endpoint: "http://127.0.0.1:11441",
      state: "READY",
      labels: %w[inference ollama remote],
      gpu_id: "NVIDIA A40",
      models: [model_capability],
      fingerprint: "a" * 64
    }.merge(overrides)
    WorkloadOrchestrator::RegistryWorker.new(
      registry: {
        registry_id: values.fetch(:registry_id),
        revision: 7,
        published_at: NOW - 60,
        expires_at: NOW + 240,
        sha256: "f" * 64
      },
      record: {
        "worker_id" => values.fetch(:worker_id),
        "generation_id" => values.fetch(:generation_id),
        "endpoint" => values.fetch(:endpoint),
        "state" => values.fetch(:state),
        "labels" => values.fetch(:labels),
        "capabilities" => {
          "gpu_id" => values.fetch(:gpu_id),
          "ollama" => { "models" => values.fetch(:models) }
        },
        "capability_fingerprint" => values.fetch(:fingerprint)
      }
    )
  end

  def model_capability(overrides = {})
    {
      "model" => "qualified-model:latest",
      "digest" => "a" * 64,
      "context_length" => 131_072,
      "fully_gpu_resident" => true
    }.merge(overrides)
  end
end
