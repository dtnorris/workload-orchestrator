# frozen_string_literal: true

require_relative "test_helper"

# Public plan/registry readers establish exact versus minimum eligibility;
# synthetic observations never contact a runtime or qualify scoring behavior.
class MinimumContextPlanTest < Minitest::Test
  FIXTURE = File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
  REGISTRY = File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)
  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  # Exercise the complete context matrix through Plan and registry ingestion.
  # No inputs/return value; a mismatched eligibility result fails the test.
  def test_versioned_exact_and_minimum_matrix
    %w[0.2 0.3 0.4].each do |version|
      plan = parse_plan(version: version, context: { "required_context_length" => 131_072 })
      assert compatible?(plan, worker(131_072)), version
      refute compatible?(plan, worker(262_144)), version
    end
    [131_072, 262_144].each do |minimum|
      plan = parse_plan(context: { "minimum_context_length" => minimum })
      assert_equal minimum == 131_072, compatible?(plan, worker(131_072))
      assert compatible?(plan, worker(262_144))
      assert plan.priority_scheduling?
      assert_equal WorkloadOrchestrator::Plan::PRIORITY_ONLY_GROUPS, plan.group_semantics
    end
  end

  # Reject malformed contexts, modes, contradictory declarations and duplicates
  # through the public reader. Expected Error classifications are asserted.
  def test_new_contract_rejects_invalid_requirements_and_ambiguous_json
    [0, -1, 1.5, "131072", true, nil, [], {}].each do |context|
      %w[required_context_length minimum_context_length].each do |field|
        error = assert_raises(WorkloadOrchestrator::Error) { parse_plan(context: { field => context }) }
        assert_includes error.message, "positive integer"
      end
    end
    [{ "required_context_length" => 1, "minimum_context_length" => 1 },
     { "minimum_context_length" => 1, "context_mode" => "at_least" },
     { "minimum_context_length" => 1, "unknown" => true }].each do |context|
      assert_raises(WorkloadOrchestrator::Error) { parse_plan(context: context) }
    end
    assert_raises(WorkloadOrchestrator::Error) { parse_plan(version: "9") }
    bytes = JSON.generate(document(context: { "minimum_context_length" => 131_072 }))
    ['"plan_id":', '"model":', '"minimum_context_length":'].each do |key|
      duplicate = bytes.sub(key, "#{key}null,#{key}")
      error = assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.new(duplicate) }
      assert_includes error.message, "duplicate"
    end
  end

  # Legacy plan and standalone parsers refuse minimum declarations, preserving
  # original bytes and the fixed standalone fingerprint; no external IO occurs.
  def test_legacy_contracts_cannot_opt_into_minimum
    %w[0.1 0.2 0.3].each do |version|
      assert_raises(WorkloadOrchestrator::Error) do
        parse_plan(version: version, context: { "minimum_context_length" => 131_072 })
      end
    end
    request = JSON.parse(File.read(File.expand_path(
      "../contracts/ollama-capability-request/v0.1/canonical-valid.json", __dir__
    )))
    exact = WorkloadOrchestrator::OllamaCapabilityRequest.new(JSON.generate(request))
    assert_equal "9121fe00d663bad2e5bd6f2ff4d6b492e66ba71b0843585c547321172dce5ae4", exact.fingerprint
    request.fetch("ollama")["minimum_context_length"] = 131_072
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::OllamaCapabilityRequest.new(JSON.generate(request))
    end
  end

  # Minimum alters only context comparison. Validated observations still fail
  # each independent model/GPU/label/readiness check; expiry fails ingestion.
  def test_minimum_preserves_all_other_eligibility_checks
    plan = parse_plan(context: { "minimum_context_length" => 131_072, "required_gpu_id" => "NVIDIA A40" })
    mutations = {
      "model_mismatch" => ->(row) { row.dig("capabilities", "ollama", "models", 0)["model"] = "other" },
      "digest_mismatch" => ->(row) { row.dig("capabilities", "ollama", "models", 0)["digest"] = "b" * 64 },
      "gpu_id_mismatch" => ->(row) { row["capabilities"]["gpu_id"] = "other" },
      "full_gpu_residency_required" => ->(row) { row.dig("capabilities", "ollama", "models", 0)["fully_gpu_resident"] = false },
      "missing_required_label" => ->(row) { row["labels"] = [] },
      "worker_not_ready" => ->(row) { row["state"] = "NOT_READY" }
    }
    mutations.each do |reason, mutation|
      candidate = worker(262_144, &mutation)
      result = WorkloadOrchestrator::CapabilityMatcher.new(plan: plan).match_job(job: plan.jobs.first, worker: candidate)
      assert_equal [reason], result.reasons
    end
    assert_raises(WorkloadOrchestrator::Error) { worker(262_144, now: NOW + 600) }
    missing = JSON.parse(File.read(REGISTRY))
    missing.dig("workers", 0, "capabilities").delete("gpu_id")
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(missing), now: NOW)
    end
    missing.dig("workers", 0, "capabilities")["gpu_id"] = "NVIDIA A40"
    missing.dig("workers", 0, "capabilities").delete("ollama")
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(missing), now: NOW)
    end
  end

  # Resume/retry keep v0.2/v0.3 exact semantics and byte bindings. Attempts are
  # disposable metadata; replacing a frozen plan with v0.4 must raise Error.
  def test_legacy_resume_and_retry_do_not_relax_or_upgrade_context
    %w[0.2 0.3].each do |version|
      Dir.mktmpdir do |root|
        plan = parse_plan(version: version, context: { "required_context_length" => 131_072 })
        store = WorkloadOrchestrator::ExecutionStore.new(output_dir: File.join(root, "output"), plan: plan, workdir: root)
        store.prepare!
        attempt = store.record_dynamic_running!(job: plan.jobs.first, worker: worker(131_072), environment_keys: [])
        store.record_dynamic_terminal!(attempt: attempt, status: "failed", exit_status: 1)
        store.retry_failed!(reason: "synthetic explicit retry", all: true)
        resumed = WorkloadOrchestrator::Plan.load(File.join(store.output_dir, "plan.json"))
        assert_equal plan.bytes, resumed.bytes
        refute compatible?(resumed, worker(262_144))
        assert compatible?(resumed, worker(131_072))
        changed = parse_plan(context: { "minimum_context_length" => 131_072 })
        incompatible = WorkloadOrchestrator::ExecutionStore.new(output_dir: store.output_dir, plan: changed, workdir: root)
        assert_raises(WorkloadOrchestrator::Error) { incompatible.prepare! }
        assert_equal plan.bytes, File.binread(File.join(store.output_dir, "plan.json"))
      end
    end
  end

  private

  # Return a synthetic AFW-shaped plan document for the selected version/context.
  # Invalid values are deliberately preserved for public-reader negative tests.
  def document(version: "0.4", context: {})
    data = JSON.parse(File.read(FIXTURE))
    data["contract_version"] = "wlo-execution-plan/v#{version}"
    data.dig("pools", 0)["required_labels"] = ["inference"]
    data.dig("pools", 0, "requirements")["ollama"] = {
      "model" => "qualified-model:latest", "expected_digest" => "a" * 64,
      "require_fully_gpu_resident" => true
    }.merge(context)
    data
  end

  # Parse synthetic bytes into a real Plan; invalid inputs raise WLO Error.
  def parse_plan(**options)
    WorkloadOrchestrator::Plan.new(JSON.generate(document(**options)))
  end

  # Return eligibility from the owning matcher for an ingested plan/worker.
  def compatible?(plan, candidate)
    WorkloadOrchestrator::CapabilityMatcher.new(plan: plan).match_job(job: plan.jobs.first, worker: candidate).compatible?
  end

  # Ingest an observed context and optional mutation through the real registry
  # parser, including its canonical fingerprint. Invalid snapshots raise Error.
  def worker(context, now: NOW)
    data = JSON.parse(File.read(REGISTRY))
    row = data.fetch("workers").first
    row.dig("capabilities", "ollama", "models", 0)["context_length"] = context
    yield row if block_given?
    row["capability_fingerprint"] = Digest::SHA256.hexdigest(JSON.generate(
      "gpu_id" => row.dig("capabilities", "gpu_id"), "labels" => row.fetch("labels"),
      "ollama_models" => row.dig("capabilities", "ollama", "models").map do |model|
        model.slice("context_length", "digest", "fully_gpu_resident", "model")
      end
    ))
    WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(data), now: now).entries.first
  end
end
