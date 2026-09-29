# frozen_string_literal: true

require_relative "test_helper"

class RpofReadinessTest < Minitest::Test
  include WloTestSupport
  WLO = WorkloadOrchestrator

  def setup
    @tmp = Dir.mktmpdir("wlo-readiness-")
    @plan = {
      "contract_version" => WLO::Plan::LOGICAL_CONTRACT_VERSION, "plan_id" => "readiness",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 3 },
      "pools" => [{ "pool_id" => "remote", "requirements" => { "ollama" => requirement } }],
      "jobs" => [job("one", code: "raise 'must not execute'", pool_id: "remote")]
    }
    @profile = {
      "contract_version" => WLO::ExecutionProfile::CONTRACT_VERSION,
      "budget" => { "max_hourly_rate_usd" => 1, "max_total_cost_usd" => 2, "max_runtime_seconds" => 3600 },
      "pools" => [{ "pool_id" => "remote", "backend" => "rpof", "max_concurrency" => 1,
                    "min_workers" => 1, "desired_workers" => 2, "max_hourly_rate_usd" => 1,
                    "target" => { "fleet_key" => "fixture", "worker_selector" => { "mode" => "indices", "indices" => [1] } } }]
    }
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_translates_requirements_exactly_and_preserves_plan_and_profile
    plan = bound_plan
    before = [plan.bytes.dup, plan.execution_profile.bytes.dup]
    requests = []
    checker = WLO::WorkerCheck.new(rpof_client: client(requests))
    result = checker.check_plan!(plan).first
    assert result.ok
    assert_equal "remote", result.pool_id
    assert_equal ["opaque-fleet", [1]], result.provider_result.values_at("fleet_id", "selected_worker_indices")
    assert_equal({
      "contract_version" => WLO::RpofContract::CAPABILITY_REQUEST,
      "fleet_key" => "fixture", "worker_selector" => { "mode" => "indices", "indices" => [1] },
      "requirements" => { "models" => [{ "name" => "fixture:latest", "expected_digest" => DIGEST }],
                          "required_context_length" => 262_144, "require_fully_gpu_resident" => true,
                          "required_gpu_id" => "NVIDIA A40" }
    }, requests.fetch(0))
    assert_equal before, [plan.bytes, plan.execution_profile.bytes]
    assert_includes result.detail, "provider.running=SKIP"
  end

  def test_optional_gpu_constraint_and_all_selector
    @plan["pools"][0]["requirements"]["ollama"].delete("required_gpu_id")
    @profile["pools"][0]["target"]["worker_selector"] = { "mode" => "all" }
    result = response
    result["selected_worker_indices"] = [1, 2]
    result["capabilities"]["gpu_id"] = "mixed"
    requests = []
    assert WLO::WorkerCheck.new(rpof_client: client(requests, result)).check_plan!(bound_plan).first.ok
    refute requests.first["requirements"].key?("required_gpu_id")
    result["selected_worker_indices"] = [1, 2, 3]
    assert_raises(WLO::Error) { WLO::WorkerCheck.new(rpof_client: client([], result)).check_plan!(bound_plan) }
  end

  def test_rejects_invalid_declarations_before_any_provider_call
    original_plan = Marshal.dump(@plan)
    original_profile = Marshal.dump(@profile)
    mutations = [
      -> { @profile["pools"][0].delete("target") },
      -> { @profile["pools"][0]["target"]["worker_selector"]["indices"] = [1, 1] },
      -> { @profile["pools"][0]["target"]["worker_selector"]["indices"] = [0] },
      -> { @profile["pools"][0]["target"]["fleet_key"] = "../other" },
      -> { @profile["pools"][0]["min_workers"] = 2 },
      -> { @plan["pools"][0]["requirements"]["ollama"].delete("expected_digest") },
      -> { @plan["pools"][0]["requirements"]["ollama"].delete("required_context_length") },
      -> { @plan["pools"][0]["requirements"]["ollama"]["required_context_length"] = "262144" },
      -> { @plan["pools"][0]["requirements"]["ollama"]["required_context_length"] = 0 },
      -> { @plan["pools"][0]["requirements"]["ollama"].delete("require_fully_gpu_resident") },
      -> { @plan["pools"][0]["requirements"]["ollama"]["require_fully_gpu_resident"] = false },
      -> { @plan["pools"][0]["requirements"]["ollama"]["required_gpu_id"] = " " },
      -> { @plan["pools"][0]["required_labels"] = ["unverifiable"] }
    ]
    requests = []
    mutations.each do |mutate|
      @plan = Marshal.load(original_plan)
      @profile = Marshal.load(original_profile)
      mutate.call
      assert_raises(WLO::Error) { WLO::WorkerCheck.new(rpof_client: client(requests)).check_plan!(bound_plan) }
    end
    assert_empty requests
  end

  def test_rejects_contradictory_or_missing_capability_evidence
    mutations = [
      ->(r) { r["capabilities"] = {} },
      ->(r) { r["capabilities"]["models"][0]["digest"] = "b" * 64 },
      ->(r) { r["capabilities"]["models"][0]["context_length"] = 8192 },
      ->(r) { r["capabilities"]["models"][0]["fully_gpu_resident"] = false },
      ->(r) { r["capabilities"]["gpu_id"] = "wrong GPU" },
      ->(r) { r["capabilities"]["models"] *= 2 },
      ->(r) { r["selected_worker_indices"] = [2] },
      ->(r) { r["fleet_key"] = "wrong" },
      ->(r) { r["diagnostics"][0]["status"] = "FAIL" },
      ->(r) { r["diagnostics"] = ["malformed"] }
    ]
    mutations.each do |mutate|
      result = response
      mutate.call(result)
      assert_raises(WLO::Error) { WLO::WorkerCheck.new(rpof_client: client([], result)).check_plan!(bound_plan) }
    end
  end

  def test_negative_result_retains_provider_failure_reason
    result = response.merge("ready" => false, "capabilities" => nil,
                            "diagnostics" => [{ "code" => "runtime.provenance", "status" => "FAIL", "detail" => "current pod evidence missing" }])
    error = assert_raises(WLO::Error) do
      WLO::WorkerCheck.new(rpof_client: client([], result)).check_plan!(bound_plan)
    end
    assert_includes error.message, "current pod evidence missing"
  end

  def test_mixed_readiness_checks_fixed_pools_and_retains_execution_gate
    @plan["pools"] << { "pool_id" => "local-pool" }
    @profile["pools"] << { "pool_id" => "local-pool", "backend" => "local", "worker_names" => ["local"], "max_concurrency" => 1 }
    workers = WLO::WorkerSet.load(write_workers(@tmp))
    checker = WLO::WorkerCheck.new(rpof_client: client([]))
    assert_equal %w[remote local-pool], checker.check_plan!(bound_plan, workers).map(&:pool_id)
    error = assert_raises(WLO::Error) { workers.validate_plan!(bound_plan) }
    assert_includes error.message, "RPOF execution is not implemented"
    requests = []
    paid = WLO::WorkerSet.load(write_workers(@tmp, rate: 1))
    assert_raises(WLO::Error) { WLO::WorkerCheck.new(rpof_client: client(requests)).check_plan!(bound_plan, paid) }
    assert_empty requests
  end

  def test_fixed_endpoints_cannot_silently_ignore_new_requirements
    @profile.delete("budget")
    @profile["pools"] = [{ "pool_id" => "remote", "backend" => "fixed_remote", "worker_names" => ["ollama"], "max_concurrency" => 1 }]
    workers = WLO::WorkerSet.load(write_workers(@tmp, include_ollama: true))
    checker = WLO::WorkerCheck.new(fetch_json: ->(*) { flunk "must reject before HTTP" })
    error = assert_raises(WLO::Error) { checker.check_plan!(bound_plan, workers) }
    assert_includes error.message, "fixed endpoints cannot verify"
  end

  def test_cli_uses_real_client_wire_boundary_without_local_workers_or_execution
    executable = File.join(@tmp, "fake-rpof")
    File.write(executable, "#!#{RbConfig.ruby}\n" + <<~'PROVIDER')
      require "json"
      abort "unexpected operation" unless ARGV.shift == "capability-check"
      args = ARGV.each_slice(2).to_h
      request = JSON.parse(File.read(args.fetch("--request")))
      File.write(File.join(__dir__, "observed.json"), JSON.generate(request))
      result = JSON.parse(File.read(File.join(__dir__, "response.json")))
      File.write(args.fetch("--output"), JSON.generate(result))
      exit(result.fetch("ready") ? 0 : 1)
    PROVIDER
    File.chmod(0o700, executable)
    result = response.merge("contract_version" => WLO::RpofClient::LEGACY_CAPABILITY_RESULT)
    File.write(File.join(@tmp, "response.json"), JSON.generate(result))
    plan_path = File.join(@tmp, "plan.json")
    profile_path = File.join(@tmp, "profile.json")
    File.write(plan_path, JSON.generate(@plan))
    File.write(profile_path, JSON.generate(@profile))
    base_args = [plan_path, "--execution-profile", profile_path, "--workers-config", File.join(@tmp, "missing.yml")]
    out, err = StringIO.new, StringIO.new
    code = WLO::CLI.new(["worker-check", *base_args, "--rpof-executable", executable], out: out, err: err).run
    assert_equal 0, code, err.string
    assert_includes out.string, "remote/rpof:fixture: PASS"
    assert_includes out.string, "provider.running=SKIP"
    wire = JSON.parse(File.read(File.join(@tmp, "observed.json")))
    assert_equal WLO::RpofClient::LEGACY_CAPABILITY_REQUEST, wire["contract_version"]
    assert_equal 262_144, wire.dig("requirements", "required_context_length")
    assert_equal DIGEST, wire.dig("requirements", "models", 0, "expected_digest")
    File.delete(File.join(@tmp, "observed.json"))
    %w[run resume].each do |command|
      code = WLO::CLI.new([command, *base_args, "--workdir", @tmp, "--output", File.join(@tmp, "execution")], out: out, err: err).run
      assert_equal 1, code
    end
    refute File.exist?(File.join(@tmp, "observed.json"))
    refute File.exist?(File.join(@tmp, "execution"))
  end

  private

  def requirement
    { "model" => "fixture:latest", "expected_digest" => DIGEST, "required_context_length" => 262_144,
      "require_fully_gpu_resident" => true, "required_gpu_id" => "NVIDIA A40" }
  end

  def bound_plan
    WLO::ExecutionProfile.new(JSON.generate(@profile)).bind(WLO::Plan.new(JSON.generate(@plan)))
  end

  def response
    { "contract_version" => WLO::RpofContract::CAPABILITY_RESULT, "ready" => true,
      "fleet_key" => "fixture", "fleet_id" => "opaque-fleet", "selected_worker_indices" => [1],
      "capabilities" => { "gpu_id" => "NVIDIA A40", "models" => [
        { "name" => "fixture:latest", "digest" => DIGEST, "context_length" => 262_144, "fully_gpu_resident" => true }
      ] },
      "diagnostics" => [{ "code" => "provider.running", "status" => "SKIP", "detail" => "API key unavailable" }] }
  end

  def client(requests, document = response)
    Object.new.tap do |fake|
      fake.define_singleton_method(:capability_check) do |request|
        requests << request
        WLO::RpofClient::Result.new(document: document, exit_status: document.fetch("ready") ? 0 : 1)
      end
    end
  end
end
