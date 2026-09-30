# frozen_string_literal: true

require_relative "test_helper"

# Pure result-validation matrices: no filesystem, provider, or workload process.
class RpofContractTest < Minitest::Test
  Contract = WorkloadOrchestrator::RpofContract

  def test_historical_capability_requests_remain_strictly_readable
    all = capability_request.merge("worker_selector" => { "mode" => "all" })
    assert_same all, Contract.capability_request!(all)

    indexed = capability_request
    indexed["requirements"]["required_gpu_id"] = "NVIDIA A40"
    assert_same indexed, Contract.capability_request!(indexed)

    malformed = [
      indexed.merge("extra" => true),
      indexed.reject { |key, _value| key == "fleet_key" },
      indexed.merge("contract_version" => "unknown"),
      indexed.merge("fleet_key" => "bad key"),
      indexed.merge("worker_selector" => []),
      indexed.merge("worker_selector" => { "mode" => "unknown" }),
      indexed.merge("worker_selector" => { "mode" => "indices", "indices" => [1, 1] }),
      indexed.merge("requirements" => indexed.fetch("requirements").merge("models" => [])),
      indexed.merge("requirements" => indexed.fetch("requirements").merge("required_context_length" => 0)),
      indexed.merge("requirements" => indexed.fetch("requirements").merge("require_fully_gpu_resident" => false))
    ]
    malformed.each do |request|
      assert_raises(WorkloadOrchestrator::Error) { Contract.capability_request!(request) }
    end
  end

  def test_historical_dispatch_request_and_job_wrappers_validate_without_execution
    request = dispatch_request
    assert_same request, Contract.dispatch_request!(request)
    job = request.fetch("jobs").first
    before = Marshal.dump(job)
    Contract.job!(job)
    assert_equal before, Marshal.dump(job)
  end

  def test_rejects_wrong_capability_identity_workers_and_readiness
    request = capability_request
    valid = capability_result
    assert_same valid, Contract.capability_result!(valid, request, 0)
    [
      { "fleet_key" => "other" }, { "selected_worker_indices" => [2] },
      { "ready" => "true" }, { "ready" => false }, { "capabilities" => nil }
    ].each do |overrides|
      assert_raises(WorkloadOrchestrator::Error) do
        Contract.capability_result!(valid.merge(overrides), request, 0)
      end
    end
  end

  def test_dispatch_rejects_bad_results
    request = dispatch_request
    valid = dispatch_summary
    assert_same valid, Contract.dispatch_summary!(valid, request, 0)
    [
      { "fleet_id" => "different" }, { "fleet_key" => "different" },
      { "worker_indices" => [2] },
      { "job_count" => 2 }, { "completed_count" => -1 },
      { "jobs" => [{ "job_id" => "wrong", "status" => "completed" }] },
      { "jobs" => [{ "job_id" => "opaque-job", "status" => "failed" }] },
      { "status" => "unknown" }, { "status" => "workload_failed" }
    ].each do |overrides|
      assert_raises(WorkloadOrchestrator::Error) do
        Contract.dispatch_summary!(valid.merge(overrides), request, 0)
      end
    end
  end

  def test_infrastructure_failure_and_drain_preserve_pending_evidence
    %w[infrastructure_failed drained interrupted].each do |status|
      result = dispatch_summary.merge(
        "status" => status, "jobs" => [], "completed_count" => 0,
        "not_started_count" => 1, "not_started_job_ids" => ["opaque-job"]
      )
      before = Marshal.dump(result)
      validated = Contract.dispatch_summary!(result, dispatch_request, 1)
      assert_same result, validated
      assert_equal status, validated["status"]
      assert_equal before, Marshal.dump(validated)
    end
  end

  private

  def capability_request
    {
      "contract_version" => Contract::CAPABILITY_REQUEST,
      "fleet_key" => "fixture", "worker_selector" => { "mode" => "indices", "indices" => [1] },
      "requirements" => {
        "models" => [{ "name" => "fixture:latest", "expected_digest" => "a" * 64 }],
        "required_context_length" => 32_768, "require_fully_gpu_resident" => true
      }
    }
  end

  def capability_result
    {
      "contract_version" => "afio-rpof-capability-check-result/v0.1", "ready" => true,
      "fleet_key" => "fixture", "fleet_id" => "opaque-fleet-id",
      "selected_worker_indices" => [1], "capabilities" => {},
      "diagnostics" => [{ "code" => "fixture", "status" => "PASS", "detail" => "provider detail" }]
    }
  end

  def dispatch_request
    {
      "contract_version" => Contract::DISPATCH_REQUEST,
      "target" => { "fleet_key" => "fixture", "expected_fleet_id" => "opaque-fleet-id", "worker_indices" => [1] },
      "group_by_affinity" => false,
      "jobs" => [{ "job_id" => "opaque-job", "argv" => ["fixture-command"] }]
    }
  end

  def dispatch_summary
    {
      "contract_version" => "afio-rpof-dispatch-summary/v0.1",
      "fleet_key" => "fixture", "fleet_id" => "opaque-fleet-id", "worker_indices" => [1],
      "status" => "completed", "job_count" => 1, "completed_count" => 1, "failed_count" => 0,
      "not_started_count" => 0, "not_started_job_ids" => [],
      "jobs" => [{ "job_id" => "opaque-job", "status" => "completed", "exit_status" => 0,
                   "fixture_stdout" => "ok", "fixture_stderr" => "" }]
    }
  end
end
