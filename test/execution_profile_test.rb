# frozen_string_literal: true

require_relative "test_helper"

class ExecutionProfileTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-profile-")
    @workers = write_workers(@tmp, extra_local: true)
    @plan_path = write_plan(@tmp, jobs: [job("one", code: "puts :ok")])
    @document = JSON.parse(File.read(@plan_path))
    @document["contract_version"] = WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION
    @document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan_path, JSON.pretty_generate(@document))
    @profile_path = File.join(@tmp, "profile.json")
    @profile = {
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                    "worker_names" => ["local"], "max_concurrency" => 1 }]
    }
    write_profile
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_same_plan_resolves_local_fixed_remote_and_rpof_without_changing_intent
    plan = WorkloadOrchestrator::Plan.load(@plan_path)
    original = plan.bytes.dup
    %w[local fixed_remote rpof].each do |backend|
      @profile["pools"][0] = binding_for(backend)
      @profile["budget"] = budget if backend == "rpof"
      bound = profile.bind(plan)
      assert_equal original, bound.bytes
      assert_equal plan.sha256, bound.sha256
      assert_same plan.jobs, bound.jobs
      assert_nil bound.pools.first.ollama_requirement
      assert_equal ["local-pool"], bound.pools.map(&:id)
    end
    assert_nil plan.pools.first.worker_names
    assert_nil plan.pools.first.max_concurrency
  end

  def test_local_and_fixed_remote_profiles_execute_opaque_jobs_with_same_plan_hash
    # These are command-worker fixtures, not real inference endpoints.
    original = File.binread(@plan_path)
    %w[local fixed_remote].each do |backend|
      @profile["pools"][0] = binding_for(backend)
      write_profile
      code, _out, err = cli("run", output: File.join(@tmp, backend))
      assert_equal 0, code, err
      state = JSON.parse(File.read(File.join(@tmp, backend, "execution.json")))
      assert_equal "completed", state["status"]
      assert_equal Digest::SHA256.hexdigest(original), state["plan_sha256"]
      assert_equal profile.sha256, state["execution_profile_sha256"]
      assert_equal File.binread(@profile_path), File.binread(File.join(@tmp, backend, "execution-profile.json"))
      assert_equal "ok\n", File.read(File.join(@tmp, backend, "runs/one/stdout.log"))
    end
    assert_equal original, File.binread(@plan_path)
  end

  def test_fixed_remote_uses_selected_endpoint_and_preserves_exact_digest_gate
    @document["pools"][0]["requirements"] = ollama_pool["requirements"]
    @document["jobs"][0]["argv"] = [RbConfig.ruby, "-e", "puts ENV.fetch('AF_OLLAMA_BASE_URL')"]
    plan = WorkloadOrchestrator::Plan.new(JSON.generate(@document))
    @profile["pools"][0] = binding_for("fixed_remote")
    bound = profile.bind(plan)
    endpoint = "http://remote.example.invalid:11434"
    workers = WorkloadOrchestrator::WorkerSet.new("local2" => {
      "type" => "ollama", "base_url" => endpoint, "hourly_rate_usd" => 0,
      "job_env" => { "AF_OLLAMA_BASE_URL" => endpoint }
    })
    requests = []
    checker = WorkloadOrchestrator::WorkerCheck.new(fetch_json: lambda do |url, path|
      requests << [url, path]
      path == "/api/version" ? { "version" => "fixture" } : {
        "models" => [{ "name" => "fixture-model:latest", "digest" => DIGEST }]
      }
    end)
    runner = WorkloadOrchestrator::Runner.new(
      plan: bound, workers: workers, workdir: @tmp, output_dir: File.join(@tmp, "remote"),
      worker_check: checker, out: StringIO.new
    )
    assert_equal "completed", runner.run
    assert_equal [[endpoint, "/api/version"], [endpoint, "/api/tags"]], requests
    assert_equal "#{endpoint}\n", File.read(File.join(@tmp, "remote/runs/one/stdout.log"))
    wrong_digest = WorkloadOrchestrator::WorkerCheck.new(fetch_json: lambda do |_url, path|
      path == "/api/version" ? {} : { "models" => [{ "name" => "fixture-model", "digest" => "b" * 64 }] }
    end)
    assert_raises(WorkloadOrchestrator::Error) { wrong_digest.check_plan!(bound, workers) }
  end

  def test_profile_cannot_override_requirements_jobs_or_domain_environment
    %w[jobs requirements env].each do |key|
      candidate = Marshal.load(Marshal.dump(@profile))
      candidate["pools"][0][key] = {}
      assert_raises(WorkloadOrchestrator::Error) { profile(candidate) }
    end
    @document["pools"][0]["requirements"] = ollama_pool["requirements"]
    @document["pools"][0]["required_labels"] = ["required"]
    plan = WorkloadOrchestrator::Plan.new(JSON.generate(@document))
    bound = profile.bind(plan)
    assert_same plan.pools.first.ollama_requirement, bound.pools.first.ollama_requirement
    error = assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::WorkerSet.load(@workers).validate_plan!(bound)
    end
    assert_includes error.message, "required labels"
  end

  def test_complete_mapping_and_strict_capacity
    plan = WorkloadOrchestrator::Plan.load(@plan_path)
    @profile["pools"][0]["pool_id"] = "unknown"
    assert_raises(WorkloadOrchestrator::Error) { profile.bind(plan) }
    @profile["pools"] = [binding_for("local"), binding_for("local")]
    assert_raises(WorkloadOrchestrator::Error) { profile }
    @profile["pools"] = []
    assert_raises(WorkloadOrchestrator::Error) { profile }
    [0, -1, 1.5, "1", 2].each do |invalid|
      @profile["pools"] = [binding_for("local").merge("max_concurrency" => invalid)]
      assert_raises(WorkloadOrchestrator::Error) { profile }
    end
    @profile["pools"] = [binding_for("local").merge("worker_names" => %w[local local])]
    assert_raises(WorkloadOrchestrator::Error) { profile }
  end

  def test_rpof_execution_requires_explicit_paid_authorization_before_output_or_worker_access
    @profile["pools"] = [binding_for("rpof")]
    @profile["budget"] = budget
    write_profile
    File.delete(@workers)
    assert_equal 0, cli("validate").first
    code, out, err = cli("plan")
    assert_equal 0, code, err
    assert_includes out, "RPOF-enabled"
    %w[run resume].each do |command|
      code, _out, err = cli(command)
      assert_equal 1, code
      assert_includes err, "requires explicit --authorize-paid-rpof"
      refute File.exist?(File.join(@tmp, "output"))
    end
  end

  def test_rpof_budget_declarations_require_finite_caps_but_never_authorize_spend
    @profile["pools"] = [binding_for("rpof")]
    assert_raises(WorkloadOrchestrator::Error) { profile }
    [0, -1, "unlimited", nil].each do |bad|
      @profile["budget"] = budget.merge("max_total_cost_usd" => bad)
      assert_raises(WorkloadOrchestrator::Error) { profile }
    end
    @profile["budget"] = budget.merge("max_runtime_seconds" => 0)
    assert_raises(WorkloadOrchestrator::Error) { profile }
    @profile["budget"] = budget
    @profile["pools"][0]["min_workers"] = 2
    assert_raises(WorkloadOrchestrator::Error) { profile }
  end

  def test_resume_rejects_changed_profile_worker_binding_and_frozen_copy_before_clearing_pause
    output = File.join(@tmp, "output")
    assert_equal 0, cli("run").first
    FileUtils.mkdir_p(File.join(output, "control"))
    pause = File.join(output, "control/pause")
    File.write(pause, "paused")
    jobs = File.binread(File.join(output, "jobs.json"))

    @profile["pools"][0]["worker_names"] = ["local2"]
    write_profile
    assert_equal 1, cli("resume").first
    assert File.exist?(pause)
    @profile["pools"][0]["worker_names"] = ["local"]
    write_profile

    workers = File.binread(@workers)
    data = YAML.safe_load(workers)
    data["workers"]["local"]["job_env"] = { "CHANGED" => "yes" }
    File.write(@workers, YAML.dump(data))
    code, _out, err = cli("resume")
    assert_equal 1, code
    assert_includes err, "worker binding"
    assert File.exist?(pause)
    File.binwrite(@workers, workers)

    frozen_path = File.join(output, "execution-profile.json")
    frozen = File.binread(frozen_path)
    File.write(frozen_path, "{}")
    assert_equal 1, cli("resume").first
    assert File.exist?(pause)
    assert_equal jobs, File.binread(File.join(output, "jobs.json"))
    File.binwrite(frozen_path, frozen)
    assert_equal 0, cli("resume").first
    refute File.exist?(pause)
    metadata = JSON.parse(File.read(File.join(output, "runs/one/metadata.json")))
    assert_equal 1, metadata["attempt"]
  end

  def test_missing_profile_paid_workers_and_legacy_overlay_are_rejected
    code, _out, err = cli("run", with_profile: false)
    assert_equal 1, code
    assert_includes err, "requires --execution-profile"
    refute File.exist?(File.join(@tmp, "output"))

    write_workers(@tmp, rate: 0.5)
    code, _out, err = cli("run")
    assert_equal 1, code
    assert_includes err, "refuses paid worker"
    refute File.exist?(File.join(@tmp, "output"))

    path = write_plan(@tmp, jobs: [job("old", code: "exit 0")], name: "old.json")
    assert_raises(WorkloadOrchestrator::Error) { profile.bind(WorkloadOrchestrator::Plan.load(path)) }
    @document["pools"][0]["worker_names"] = ["local"]
    assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.new(JSON.generate(@document)) }
  end

  def test_profile_retry_preserves_attempts_and_rejects_placement_drift
    @document["jobs"][0]["argv"] = [RbConfig.ruby, "-e", "exit(File.exist?('repaired') ? 0 : 1)"]
    File.write(@plan_path, JSON.pretty_generate(@document))
    assert_equal 2, cli("run").first
    output = File.join(@tmp, "output")
    state_path = File.join(output, "execution.json")
    before = File.binread(state_path)
    args = ["retry-failed", @plan_path, "--workdir", @tmp, "--output", output,
            "--execution-profile", @profile_path, "--workers-config", @workers,
            "--all", "--reason", "Repaired fixture"]
    retry_call = lambda do
      err = StringIO.new
      code = WorkloadOrchestrator::CLI.new(args, out: StringIO.new, err: err).run
      [code, err.string]
    end
    @profile["pools"][0]["worker_names"] = ["local2"]
    write_profile
    assert_equal 1, retry_call.call.first
    assert_equal before, File.binread(state_path)
    @profile["pools"][0]["worker_names"] = ["local"]
    write_profile
    original_workers = File.binread(@workers)
    data = YAML.safe_load(original_workers)
    data["workers"]["local"]["job_env"] = { "CHANGED" => "yes" }
    File.write(@workers, YAML.dump(data))
    assert_equal 1, retry_call.call.first
    assert_equal before, File.binread(state_path)
    File.binwrite(@workers, original_workers)
    code, err = retry_call.call
    assert_equal 0, code, err
    archive = File.join(output, "attempts/one/attempt-1/metadata.json")
    assert_equal "failed", JSON.parse(File.read(archive))["status"]
    File.write(File.join(@tmp, "repaired"), "yes")
    assert_equal 0, cli("resume").first
    metadata = JSON.parse(File.read(File.join(output, "runs/one/metadata.json")))
    assert_equal "complete", metadata["status"]
    assert_equal 2, metadata["attempt"]
    assert_equal "failed", JSON.parse(File.read(archive))["status"]
  end

  private

  def binding_for(backend)
    row = { "pool_id" => "local-pool", "backend" => backend, "max_concurrency" => 1 }
    if backend == "rpof"
      row.merge("min_workers" => 1, "desired_workers" => 1, "max_hourly_rate_usd" => 1)
    else
      row.merge("worker_names" => [backend == "local" ? "local" : "local2"])
    end
  end

  def budget
    { "max_hourly_rate_usd" => 1, "max_total_cost_usd" => 2, "max_runtime_seconds" => 3600 }
  end

  def profile(document = @profile)
    WorkloadOrchestrator::ExecutionProfile.new(JSON.pretty_generate(document))
  end

  def write_profile
    File.write(@profile_path, JSON.pretty_generate(@profile))
  end

  def cli(command, output: File.join(@tmp, "output"), with_profile: true)
    args = [command, @plan_path]
    args += ["--execution-profile", @profile_path] if with_profile
    args += ["--workers-config", @workers] unless command == "validate"
    args += ["--workdir", @tmp] if %w[plan run resume].include?(command)
    args += ["--output", output] if %w[run resume].include?(command)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(args, out: out, err: err).run
    [code, out.string, err.string]
  end
end
