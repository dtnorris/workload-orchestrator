# frozen_string_literal: true

require_relative "test_helper"

module PaidBudgetFixtures
  def declaration
    {
      "contract_version" => WorkloadOrchestrator::PaidBudget::VERSION,
      "budget_id" => "fixture-budget", "plan_sha256" => Digest::SHA256.hexdigest("exact plan bytes"),
      "expected_compute_usd" => 1.0, "max_hourly_rate_usd" => 2.0,
      "max_cumulative_compute_usd" => 3.0, "max_runtime_seconds" => 3600.0,
      "guardian_poll_seconds" => 10.0, "orchestrator_heartbeat_timeout_seconds" => 30.0,
      "teardown_reserve_seconds" => 20.0
    }
  end

  def make_budget(data = declaration)
    WorkloadOrchestrator::PaidBudget.new(data, plan_bytes: "exact plan bytes")
  end

  def snapshot(budget = make_budget)
    now = Time.utc(2026, 9, 28, 23)
    {
      "contract_version" => WorkloadOrchestrator::PaidBudget::STATE_VERSION,
      **budget.identity, "limits" => budget.provider_request, "state" => "ARMED", "mutation_allowed" => true,
      "armed_at_utc" => now.iso8601, "deadline_at_utc" => (now + 3600).iso8601,
      "last_guardian_heartbeat_at_utc" => now.iso8601, "last_orchestrator_heartbeat_at_utc" => now.iso8601,
      "accrued_compute_usd" => 0.0, "committed_rate_usd_per_hour" => 0.0,
      "committed_maximum_liability_usd" => 0.0, "remaining_uncommitted_budget_usd" => 3.0
    }
  end

  def guardian
    now = Time.utc(2026, 9, 28, 23)
    { "enabled" => true, "launchd_loaded" => true, "ready" => true, "state" => "ARMED",
      "pid" => Process.pid + 1, "last_error" => nil, "ledger_heartbeat_at_utc" => now.iso8601,
      "provider_probe_at_utc" => now.iso8601 }
  end
end

class PaidBudgetTest < Minitest::Test
  include PaidBudgetFixtures

  def test_finite_bounds_and_unchanged_provider_contract
    budget = make_budget
    assert_equal 60, budget.crash_horizon_seconds
    assert_in_delta 2.0 / 60, budget.bounds.fetch("crash_additional_compute_usd")
    assert_equal 3630, budget.bounds.fetch("max_time_to_absence_seconds")
    assert_equal "afio-production-burst-budget/v0.1", budget.provider_request["contract_version"]
    refute budget.provider_request.key?("expected_compute_usd")
    refute budget.provider_request.key?("max_hourly_rate_usd")
    assert budget.document.frozen?
    assert budget.document["budget_id"].frozen?
  end

  def test_execution_profile_cannot_silently_widen_the_paid_budget
    document = JSON.parse(File.read(File.expand_path("../examples/profiles/rpof.json", __dir__)))
    document["budget"] = { "max_hourly_rate_usd" => 2.0, "max_total_cost_usd" => 3.0, "max_runtime_seconds" => 3600 }
    document["pools"].each { |pool| pool["max_hourly_rate_usd"] = 1.0 }
    profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(document))
    budget = WorkloadOrchestrator::PaidBudget.new(declaration, plan_bytes: "exact plan bytes", execution_profile: profile)
    assert_equal profile.sha256, budget.execution_profile_sha256
    document["budget"]["max_total_cost_usd"] = 4.0
    changed = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(document))
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::PaidBudget.new(declaration, plan_bytes: "exact plan bytes", execution_profile: changed)
    end
    assert_raises(WorkloadOrchestrator::Error) { profile.ensure_runnable! }
  end

  def test_rejects_missing_unknown_nonfinite_and_invalid_limits
    bad = [declaration.merge("unknown" => true), declaration.reject { |key, _| key == "expected_compute_usd" }]
    (WorkloadOrchestrator::PaidBudget::LIMITS + WorkloadOrchestrator::PaidBudget::COSTS).each do |key|
      [nil, 0, -1, "3", Float::INFINITY, Float::NAN].each { |value| bad << declaration.merge(key => value) }
    end
    bad << declaration.merge("orchestrator_heartbeat_timeout_seconds" => 19)
    bad << declaration.merge("max_runtime_seconds" => 60)
    bad << declaration.merge("max_cumulative_compute_usd" => 1.01)
    bad << declaration.merge("plan_sha256" => "b" * 64)
    bad << declaration.merge("budget_id" => "\0")
    bad << declaration.merge("max_hourly_rate_usd" => 1e308, "max_cumulative_compute_usd" => 1e308)
    bad.each { |row| assert_raises(WorkloadOrchestrator::Error) { make_budget(row) } }
  end

  def test_rejects_unready_identity_limits_timestamps_and_liability
    now = Time.utc(2026, 9, 28, 23)
    changes = [
      { "budget_id" => "other" }, { "contract_version" => "unknown" }, { "plan_sha256" => "b" * 64 },
      { "limits" => {} }, { "state" => "CLOSED" }, { "mutation_allowed" => false },
      { "deadline_at_utc" => (now + 3601).iso8601 }, { "deadline_at_utc" => now.iso8601 },
      { "last_guardian_heartbeat_at_utc" => (now - 21).iso8601 },
      { "last_orchestrator_heartbeat_at_utc" => (now - 31).iso8601 },
      { "last_guardian_heartbeat_at_utc" => (now + 1).iso8601 },
      { "committed_rate_usd_per_hour" => 2.1 }, { "committed_maximum_liability_usd" => 3.0 },
      { "accrued_compute_usd" => -1 }, { "armed_at_utc" => "garbage" }
    ]
    budget = make_budget
    budget.validate_snapshot!(snapshot, ready: true, now: now)
    changes.each do |change|
      assert_raises(WorkloadOrchestrator::Error) do
        budget.validate_snapshot!(snapshot.merge(change), ready: true, now: now)
      end
    end
  end

  def test_requires_live_independent_guardian_and_fresh_evidence
    now = Time.utc(2026, 9, 28, 23)
    [{ "enabled" => false }, { "launchd_loaded" => false }, { "pid" => Process.pid },
     { "ready" => false }, { "last_error" => "provider unavailable" }, { "state" => "ERROR_RETRYING" },
     { "ledger_heartbeat_at_utc" => (now - 21).iso8601 }].each do |change|
      assert_raises(WorkloadOrchestrator::Error) do
        make_budget.validate_guardian!(guardian.merge(change), now: now)
      end
    end
  end

  def test_historical_closed_snapshot_and_valid_guardian_remain_inspectable
    now = Time.utc(2026, 9, 28, 23)
    budget = make_budget
    closed = snapshot.merge("state" => "CLOSED", "mutation_allowed" => false)
    assert_same closed, budget.validate_snapshot!(closed, now: now)
    guardian_status = guardian
    assert_same guardian_status, budget.validate_guardian!(guardian_status, now: now)

    error = assert_raises(WorkloadOrchestrator::Error) do
      budget.validate_snapshot!(snapshot.merge("state" => "UNKNOWN"), now: now)
    end
    assert_includes error.message, "invalid provider budget state"
  end
end
