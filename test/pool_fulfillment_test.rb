# frozen_string_literal: true

require_relative "test_helper"

module PoolFixtures
  def logical_document
    { "contract_version" => WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION, "plan_id" => "opaque-work",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 3 },
      "pools" => ["alpha", "beta"].map { |id| { "pool_id" => id,
        "requirements" => { "ollama" => { "model" => "#{id}:exact", "expected_digest" => "a" * 64,
          "required_context_length" => 262_144, "require_fully_gpu_resident" => true } } } },
      "jobs" => ["alpha", "beta"].map { |id| { "job_id" => id, "pool_id" => id, "argv" => ["true"] } } }
  end

  def profile_document
    { "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => ["alpha", "beta"].map { |id| { "pool_id" => id, "backend" => "rpof", "max_concurrency" => 2,
        "min_workers" => 1, "desired_workers" => 2, "max_hourly_rate_usd" => 1.0 } },
      "budget" => { "max_hourly_rate_usd" => 2.0, "max_total_cost_usd" => 3.0, "max_runtime_seconds" => 3600 } }
  end

  def pool_plan(document: logical_document, profile: profile_document, budget_id: "fixture-budget")
    logical = WorkloadOrchestrator::Plan.new(JSON.generate(document))
    placement = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(profile))
    budget = WorkloadOrchestrator::PaidBudget.new(
      { "contract_version" => WorkloadOrchestrator::PaidBudget::VERSION,
        "budget_id" => budget_id, "plan_sha256" => logical.sha256,
        "expected_compute_usd" => 1.0, "max_hourly_rate_usd" => 2.0,
        "max_cumulative_compute_usd" => 3.0, "max_runtime_seconds" => 3600,
        "guardian_poll_seconds" => 10, "orchestrator_heartbeat_timeout_seconds" => 30,
        "teardown_reserve_seconds" => 20 }, plan_bytes: logical.bytes, execution_profile: placement
    )
    WorkloadOrchestrator::ExecutionPoolPlan.new(plan: logical, profile: placement, budget: budget)
  end
end

class ExecutionPoolPlanTest < Minitest::Test
  include PoolFixtures

  def test_exact_domain_identity_and_profile_capacity_are_joined_without_provider_policy_in_plan
    plan = pool_plan
    request = plan.request("alpha")
    assert_equal "alpha:exact", request.dig("requirements", "ollama_model")
    assert_equal "a" * 64, request.dig("requirements", "expected_digest")
    assert_equal 262_144, request.dig("requirements", "required_context_length")
    assert_equal({ "desired_workers" => 2, "minimum_workers" => 2,
                   "max_pool_hourly_usd" => 1.0, "max_total_hourly_usd" => 2.0 }, request["capacity"])
    assert_equal plan.budget.document, request["budget"]
    assert request.frozen?
    assert request["requirements"].frozen?
    assert_equal plan.requests, pool_plan.requests
    refute_equal request["pool_id"], plan.request("beta")["pool_id"]
    refute_equal request["pool_id"], pool_plan(budget_id: "new-budget").request("alpha")["pool_id"]
    assert_equal 2.0, plan.preview.dig("bounds", "max_hourly_rate_usd")
  end

  def test_context_is_explicit_and_aggregate_cannot_be_oversubscribed
    document = logical_document
    document["pools"].first["requirements"]["ollama"].delete("required_context_length")
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(document: document) }
    profile = profile_document
    profile["pools"].first["max_hourly_rate_usd"] = 1.5
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(profile: profile) }
    document = logical_document
    document["pools"].first["requirements"]["ollama"]["required_context_length"] = 0
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(document: document) }
  end

  def test_unsupported_requirements_and_missing_model_are_rejected
    document = logical_document
    document["pools"].first["required_labels"] = ["unprovable"]
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(document: document) }
    document = logical_document
    document["pools"].first.delete("requirements")
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(document: document) }
  end

  def test_hard_gpu_constraints_and_existing_fleet_targets_are_not_silently_dropped
    document = logical_document
    document["pools"].first["requirements"]["ollama"]["required_gpu_id"] = "specific-gpu"
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(document: document) }
    profile = profile_document
    profile["pools"].first["target"] = { "fleet_key" => "other-campaign", "worker_selector" => { "mode" => "all" } }
    assert_raises(WorkloadOrchestrator::Error) { pool_plan(profile: profile) }
  end

  def test_local_pools_are_omitted_and_profile_changes_get_a_different_provider_identity
    profile = profile_document
    profile["pools"][1] = { "pool_id" => "beta", "backend" => "local", "worker_names" => ["local"],
                            "max_concurrency" => 1 }
    assert_equal ["alpha"], pool_plan(profile: profile).requests.keys
    profile = profile_document
    profile["pools"].first["desired_workers"] = 3
    refute_equal pool_plan.request("alpha")["pool_id"], pool_plan(profile: profile).request("alpha")["pool_id"]
  end
end

# Fake only the process boundary. Real client translation, file evidence,
# validators, budget lifecycle, and pool session all run unchanged.
class PoolProviderFixture < WorkloadOrchestrator::RpofCapacityClient
  Status = Struct.new(:exitstatus) do
    def success? = exitstatus.zero?
    def exited? = true
  end
  attr_reader :calls, :snapshot, :wire_requests
  attr_accessor :preflight_status, :initial_workers, :paid_status, :final_workers, :override,
                :ownership, :pending, :fail_after_create, :callback, :teardown_fails, :ready

  def initialize
    super(executable: RbConfig.ruby)
    @calls = []
    @wire_requests = []
    @preflight_status = "planned"
    @initial_workers = 0
    @paid_status = "ready"
    @ownership = true
    @ready = true
  end

  private

  def capture_process(arguments, timeout_seconds:)
    @calls << arguments.dup
    case arguments.first
    when "budget" then budget_response(arguments)
    when "execution-pool-fulfill" then pool_response(arguments)
    when "capability-check" then capability_response(arguments)
    else raise "unexpected operation #{arguments.inspect}"
    end
  end

  def budget_response(arguments)
    operation = arguments[1]
    now = Time.now.utc
    if operation == "arm"
      limits = JSON.parse(File.read(arguments[3]))
      @snapshot = {
        "contract_version" => WorkloadOrchestrator::PaidBudget::STATE_VERSION,
        "budget_id" => limits["budget_id"], "plan_sha256" => limits["plan_sha256"], "limits" => limits,
        "state" => "ARMED", "mutation_allowed" => true, "armed_at_utc" => now.iso8601,
        "deadline_at_utc" => (now + limits.fetch("max_runtime_seconds")).iso8601,
        "last_guardian_heartbeat_at_utc" => now.iso8601, "last_orchestrator_heartbeat_at_utc" => now.iso8601,
        "accrued_compute_usd" => 0.0, "committed_rate_usd_per_hour" => 0.0,
        "committed_maximum_liability_usd" => 0.0, "remaining_uncommitted_budget_usd" => 3.0,
        "owned_resources" => {}, "reservations" => {}
      }
    elsif operation == "guardian-status"
      return [JSON.generate("enabled" => true, "launchd_loaded" => true, "ready" => true, "state" => "ARMED",
                            "pid" => Process.pid + 1, "provider_probe_at_utc" => now.iso8601,
                            "ledger_heartbeat_at_utc" => now.iso8601, "last_error" => nil), "", Status.new(0)]
    elsif operation == "begin-teardown"
      return ["", "teardown unavailable", Status.new(1)] if @teardown_fails
      @snapshot["state"] = "TEARDOWN_REQUIRED"
      @snapshot["mutation_allowed"] = false
    elsif operation == "heartbeat"
      @snapshot["last_orchestrator_heartbeat_at_utc"] = now.iso8601
    end
    [JSON.generate(@snapshot), "", Status.new(0)]
  end

  def pool_response(arguments)
    request = JSON.parse(File.read(arguments[arguments.index("--request") + 1]))
    @wire_requests << request
    dry = arguments.include?("--dry-run")
    status = dry ? @preflight_status : @paid_status
    final = dry ? @initial_workers : (@final_workers || request.dig("capacity", "desired_workers"))
    ready = %w[ready partial_ready].include?(status)
    indices = ready ? (1..final).to_a : []
    handle = "ep-#{request.fetch('pool_id')}-#{request.fetch('plan_sha256')[0, 10]}"
    unless dry
      indices.each do |index|
        @snapshot["owned_resources"]["#{handle}-#{index}"] = {
          "status" => "active", "fleet_key" => handle, "logical_resource_id" => "burst_#{index}",
          "hourly_rate_usd" => 0.1
        } if @ownership
      end
      @snapshot["reservations"]["pending"] = { "status" => "pending" } if @pending
      @snapshot["committed_rate_usd_per_hour"] = @snapshot["owned_resources"].length * 0.1
      @snapshot["committed_maximum_liability_usd"] = @snapshot["committed_rate_usd_per_hour"] / 60.0
      @callback&.call(@snapshot)
      raise WorkloadOrchestrator::Error, "timeout after provider creation" if @fail_after_create
    end
    result = {
      "contract_version" => WIRE_RESULT, "plan_sha256" => request["plan_sha256"], "pool_id" => request["pool_id"],
      "requirements" => request["requirements"], "execution_handle" => handle, "ready" => ready,
      "status" => status, "worker_indices" => indices, "capabilities" => ready ? {} : nil,
      "capacity" => request.fetch("capacity").merge("initial_workers" => @initial_workers, "final_workers" => final)
    }
    result.merge!(@override) if @override && !dry
    path = arguments[arguments.index("--output") + 1]
    File.write(path, JSON.generate(result))
    ["fixture stdout", "fixture stderr", Status.new(ready || status == "planned" ? 0 : 1)]
  end

  def capability_response(arguments)
    request = JSON.parse(File.read(arguments[arguments.index("--request") + 1]))
    result = { "contract_version" => LEGACY_CAPABILITY_RESULT, "ready" => @ready,
               "fleet_key" => request["fleet_key"], "fleet_id" => "fleet-#{request['fleet_key']}",
               "selected_worker_indices" => request.dig("worker_selector", "indices"),
               "diagnostics" => [], "capabilities" => {
                 "gpu_id" => "fixture-gpu", "models" => [{
                   "name" => request.dig("requirements", "models", 0, "name"),
                   "digest" => request.dig("requirements", "models", 0, "expected_digest"),
                   "context_length" => request.dig("requirements", "required_context_length"),
                   "fully_gpu_resident" => true
                 }]
               } }
    File.write(arguments[arguments.index("--output") + 1], JSON.generate(result))
    ["", "", Status.new(@ready ? 0 : 1)]
  end
end

class PoolFulfillmentTest < Minitest::Test
  include PoolFixtures

  def setup
    @root = Dir.mktmpdir("wlo-capacity-")
    @provider = PoolProviderFixture.new
    @plan = pool_plan
    @output = File.join(@root, "capacity")
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def session
    WorkloadOrchestrator::PoolFulfillment.new(pool_plan: @plan, client: @provider, output_dir: @output)
  end

  def run_capacity(&block)
    session.with_capacity(authorize_paid: true, &block)
  end

  def test_fulfills_all_pools_under_one_budget_with_live_targets_and_retained_evidence
    result = run_capacity do |handoffs, lifecycle|
      assert_equal ["alpha", "beta"], handoffs.keys
      handoffs.each_value do |handoff|
        assert_equal @plan.budget.identity, handoff["budget"]
        assert_equal [1, 2], handoff.dig("target", "worker_indices")
        assert_match(/\Afleet-ep-/, handoff.dig("target", "expected_fleet_id"))
        assert_in_delta 0.2, handoff["hourly_rate_usd"]
      end
      assert_equal "ARMED", lifecycle.check!["state"]
      :consumed
    end
    assert_equal :consumed, result
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    operations = @provider.calls.map { |args| args.first == "budget" ? args[1] : args.last }
    assert_equal ["--dry-run", "--dry-run", "arm"], operations.first(3)
    assert_equal 1, operations.count("arm")
    assert_equal 2, operations.count("--yes")
    assert_equal "begin-teardown", operations.last
    assert File.file?(File.join(@output, "capacity.json"))
    assert JSON.parse(File.read(File.join(@output, "session.json")))["teardown_requested"]
    @provider.wire_requests.each do |request|
      assert_equal WorkloadOrchestrator::RpofCapacityClient::WIRE_REQUEST, request["contract_version"]
      assert_equal @plan.budget.provider_request, request["budget"]
      assert_equal 2.0, request.dig("capacity", "max_total_hourly_usd")
    end
  end

  def test_requires_authorization_and_block_before_any_io
    assert_raises(WorkloadOrchestrator::Error) { session.with_capacity { flunk } }
    assert_raises(WorkloadOrchestrator::Error) { session.with_capacity(authorize_paid: true) }
    assert_empty @provider.calls
    refute File.exist?(@output)
  end

  def test_unavailable_and_existing_capacity_fail_before_budget_arm
    @provider.preflight_status = "unavailable"
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    refute @provider.calls.any? { |args| args.first == "budget" }
    @output = File.join(@root, "existing")
    @provider.preflight_status = "planned"
    @provider.initial_workers = 1
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    refute @provider.calls.any? { |args| args.include?("--yes") }
  end

  def test_accepts_minimum_capacity_without_claiming_desired_capacity
    profile = profile_document
    profile["pools"].each { |row| row["max_concurrency"] = 1 }
    @plan = pool_plan(profile: profile)
    @provider.paid_status = "partial_ready"
    @provider.final_workers = 1
    run_capacity do |handoffs, _|
      assert_equal [1], handoffs["alpha"].dig("target", "worker_indices")
      assert_equal "partial_ready", handoffs["alpha"]["status"]
    end
  end

  def test_malformed_or_mismatched_paid_results_stop_and_request_teardown
    [{ "pool_id" => "wrong" }, { "requirements" => {} }, { "ready" => false },
     { "execution_handle" => "other-fleet" }, { "worker_indices" => [2, 3] },
     { "capacity" => {} }].each_with_index do |override, index|
      @output = File.join(@root, "bad-#{index}")
      @provider = PoolProviderFixture.new
      @provider.override = override
      assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
      assert_equal "begin-teardown", @provider.calls.last[1]
      assert_equal 1, @provider.calls.count { |args| args.include?("--yes") }
    end
  end

  def test_no_unowned_or_uncommitted_resources_can_be_handed_off
    @provider.ownership = false
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    @provider = PoolProviderFixture.new
    @provider.pending = true
    @output = File.join(@root, "pending")
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
  end

  def test_fresh_capability_failure_requests_teardown
    @provider.ready = false
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
  end

  def test_unknown_create_outcome_keeps_evidence_and_requests_teardown
    @provider.fail_after_create = true
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal 2, @provider.snapshot["owned_resources"].length
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    assert File.file?(File.join(@output, "intent.json"))
    assert File.file?(File.join(@output, "session.json"))
  end

  def test_callback_exception_is_preserved_and_cleanup_failure_is_reported
    error = assert_raises(ArgumentError) { run_capacity { raise ArgumentError, "consumer failed" } }
    assert_equal "consumer failed", error.message
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    @provider = PoolProviderFixture.new
    @provider.teardown_fails = true
    @output = File.join(@root, "cleanup-failure")
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { :done } }
    refute JSON.parse(File.read(File.join(@output, "session.json")))["teardown_requested"]
  end

  def test_existing_output_is_not_reused
    run_capacity { :done }
    prior_calls = @provider.calls.length
    evidence = File.binread(File.join(@output, "capacity.json"))
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal prior_calls, @provider.calls.length
    assert_equal evidence, File.binread(File.join(@output, "capacity.json"))
  end

  def test_resume_reuses_only_the_original_live_budget_and_capacity
    first = session.with_capacity(authorize_paid: true) do |_handoffs, _lifecycle|
      WorkloadOrchestrator::PoolFulfillment::Outcome.new(value: :paused, retain_capacity: true)
    end
    assert_equal :paused, first
    assert_equal "ARMED", @provider.snapshot["state"]
    paid_calls = @provider.calls.count { |args| args.include?("--yes") }
    arm_calls = @provider.calls.count { |args| args == ["budget", "arm"] }

    resumed = session.with_capacity(authorize_paid: true, resume: true) do |handoffs, _lifecycle|
      assert_equal %w[alpha beta], handoffs.keys
      :completed
    end

    assert_equal :completed, resumed
    assert_equal paid_calls, @provider.calls.count { |args| args.include?("--yes") }
    assert_equal arm_calls, @provider.calls.count { |args| args == ["budget", "arm"] }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    assert File.file?(File.join(@output, "sessions", "session-2.json"))
  end

  def test_resume_never_fulfills_when_original_capacity_evidence_is_missing
    error = assert_raises(WorkloadOrchestrator::Error) do
      session.with_capacity(authorize_paid: true, resume: true) { flunk }
    end
    assert_includes error.message, "re-fulfillment is forbidden"
    assert_empty @provider.calls
  end

  def test_later_pool_failure_tears_down_the_same_parent_budget
    @provider.callback = lambda do |snapshot|
      @provider.fail_after_create = true if snapshot["owned_resources"].length > 2
    end
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal 4, @provider.snapshot["owned_resources"].length
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    paid = @provider.calls.select { |args| args.include?("--yes") }
    assert_equal 2, paid.length
  end

  def test_capacity_client_refuses_an_unbound_client_and_missing_authorization
    lifecycle = WorkloadOrchestrator::PaidBudgetLifecycle.new(
      budget: @plan.budget, client: @provider, binding_path: File.join(@root, "binding.json")
    )
    lifecycle.start!
    other = PoolProviderFixture.new
    assert_raises(WorkloadOrchestrator::Error) do
      other.fulfill_pool(pool_plan: @plan, pool_id: "alpha", lifecycle: lifecycle,
                         output_dir: @output, authorize_paid: true)
    end
    assert_empty other.calls
    assert_raises(WorkloadOrchestrator::Error) do
      @provider.fulfill_pool(pool_plan: @plan, pool_id: "alpha", lifecycle: lifecycle, output_dir: @output)
    end
    refute @provider.calls.any? { |args| args.include?("--yes") }
  ensure
    lifecycle&.finish!(reason: "fixture complete")
  end

  def test_real_process_receives_existing_wire_contract_and_dry_run_flag
    executable = File.join(@root, "rpof ; literal")
    File.write(executable, <<~'PROVIDER'.sub("RUBY_EXECUTABLE", RbConfig.ruby))
      #!RUBY_EXECUTABLE
      require "json"
      abort "wrong operation" unless ARGV.shift == "execution-pool-fulfill"
      abort "must be read only" unless ARGV.pop == "--dry-run"
      options = ARGV.each_slice(2).to_h
      request = JSON.parse(File.read(options.fetch("--request")))
      abort "wrong request version" unless request["contract_version"] == "afio-rpof-execution-pool-fulfill-request/v0.2"
      abort "wrong budget version" unless request.dig("budget", "contract_version") == "afio-production-burst-budget/v0.1"
      result = {
        "contract_version" => "afio-rpof-execution-pool-fulfill-result/v0.1",
        "plan_sha256" => request["plan_sha256"], "pool_id" => request["pool_id"],
        "requirements" => request["requirements"], "ready" => false, "status" => "planned", "worker_indices" => [],
        "execution_handle" => "ep-#{request['pool_id']}-#{request['plan_sha256'][0, 10]}",
        "capacity" => request["capacity"].merge("initial_workers" => 0, "final_workers" => 0)
      }
      File.write(options.fetch("--output"), JSON.generate(result))
      puts "fixture provider output"
    PROVIDER
    File.chmod(0o700, executable)
    client = WorkloadOrchestrator::RpofCapacityClient.new(executable: executable)
    result = client.plan_pool(pool_plan: @plan, pool_id: "alpha", output_dir: @output)
    assert_equal WorkloadOrchestrator::RpofCapacityClient::RESULT_VERSION, result.document["contract_version"]
    assert_includes File.read(File.join(@output, "stdout.log")), "fixture provider output"
    evidence = File.binread(File.join(@output, "result.json"))
    assert_raises(WorkloadOrchestrator::Error) do
      client.plan_pool(pool_plan: @plan, pool_id: "alpha", output_dir: @output)
    end
    assert_equal evidence, File.binread(File.join(@output, "result.json"))
  end

  def test_changed_deadline_and_excessive_rate_fail_before_handoff
    @provider.callback = lambda do |snapshot|
      snapshot["deadline_at_utc"] = (Time.iso8601(snapshot["deadline_at_utc"]) + 1).iso8601
    end
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
    @provider = PoolProviderFixture.new
    @output = File.join(@root, "over-rate")
    @provider.callback = ->(snapshot) { snapshot["committed_rate_usd_per_hour"] = 2.1 }
    assert_raises(WorkloadOrchestrator::Error) { run_capacity { flunk } }
    assert_equal "TEARDOWN_REQUIRED", @provider.snapshot["state"]
  end
end
