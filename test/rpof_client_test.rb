# frozen_string_literal: true

require_relative "test_helper"

class RpofClientTest < Minitest::Test
  Contract = WorkloadOrchestrator::RpofContract
  Client = WorkloadOrchestrator::RpofClient

  def setup
    @root = Dir.mktmpdir("wlo rpof ; fixture-")
    @executable = File.join(@root, "rpof ; fixture")
    @control = File.join(@root, "control.json")
    File.write(@control, JSON.generate({}))
    File.write(@executable, "#!#{RbConfig.ruby}\n" + provider_fixture)
    File.chmod(0o700, @executable)
    @client = Client.new(executable: @executable)
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_capability_uses_exact_digest_wire_contract_and_normalizes_only_version
    request = capability_request
    original = Marshal.dump(request)
    result = @client.capability_check(request)
    recorded = JSON.parse(File.read(File.join(@root, "request.json")))

    assert_equal "afio-rpof-capability-check-request/v0.2", recorded.delete("contract_version")
    assert_equal request.reject { |key, _| key == "contract_version" }, recorded
    assert_equal Contract::CAPABILITY_RESULT, result.document["contract_version"]
    assert_equal "opaque-fleet-id", result.document["fleet_id"]
    assert_equal "provider detail", result.document["diagnostics"].first["detail"]
    assert_equal original, Marshal.dump(request)
    assert_equal 0, result.exit_status
  end

  def test_not_ready_is_a_valid_negative_result
    configure("exit" => 1, "overrides" => { "ready" => false, "fleet_id" => nil, "capabilities" => nil })
    result = @client.capability_check(capability_request)
    assert_equal false, result.document["ready"]
    assert_equal 1, result.exit_status
  end

  def test_rejects_bad_request_before_spawning
    requests = [nil, [], {}, capability_request.merge("contract_version" => "wlo-rpof-capability-check-request/v9")]
    no_digest = capability_request
    no_digest["requirements"]["models"].first.delete("expected_digest")
    requests << no_digest
    requests << capability_request.merge("budget" => {})
    requests.each { |request| assert_raises(WorkloadOrchestrator::Error) { @client.capability_check(request) } }
    refute File.exist?(File.join(@root, "request.json"))
  end

  def test_rejects_unexpected_exit_even_with_valid_result
    [2, 7, 130].each do |code|
      configure("exit" => code)
      error = assert_raises(WorkloadOrchestrator::Error) { @client.capability_check(capability_request) }
      assert_includes error.message, "exit #{code}"
    end
  end

  def test_rejects_signaled_provider
    configure("signal" => true)
    error = assert_raises(WorkloadOrchestrator::Error) { @client.capability_check(capability_request) }
    assert_includes error.message, "signal"
  end

  def test_rejects_missing_empty_invalid_and_non_object_results
    [{ "missing" => true }, { "raw" => "" }, { "raw" => "{" }, { "raw" => "[]" }, { "raw" => "null" }].each do |config|
      configure(config)
      assert_raises(WorkloadOrchestrator::Error) { @client.capability_check(capability_request) }
    end
  end

  def test_rejects_wrong_version_identity_workers_and_readiness
    [
      { "contract_version" => "wlo-rpof-capability-check-result/v0.1" },
      { "contract_version" => "afio-rpof-capability-check-result/v99" },
      { "fleet_key" => "other" }, { "selected_worker_indices" => [2] },
      { "ready" => "true" }, { "ready" => false }, { "capabilities" => nil }
    ].each do |overrides|
      configure("overrides" => overrides)
      assert_raises(WorkloadOrchestrator::Error) { @client.capability_check(capability_request) }
    end
  end

  def test_dispatch_executes_opaque_argv_and_env_without_shell_interpretation
    request = dispatch_request
    literal = "$(touch injected); * ' quoted"
    request["jobs"].first["argv"] = [RbConfig.ruby, "-e", 'print ENV.fetch("FIXTURE") + ARGV.fetch(0)', literal]
    request["jobs"].first["env"] = { "FIXTURE" => "prefix:" }
    request["jobs"].first["affinity"] = "opaque-group"
    request["group_by_affinity"] = true
    before = Marshal.dump(request)
    output = File.join(@root, "evidence ; literal")
    result = @client.dispatch(request: request, workdir: @root, output_dir: output)

    assert_equal "prefix:#{literal}", result.document["jobs"].first["fixture_stdout"]
    assert_equal Contract::DISPATCH_SUMMARY, result.document["contract_version"]
    assert_equal before, Marshal.dump(request)
    refute File.exist?(File.join(@root, "injected"))
    wire = JSON.parse(File.read(File.join(@root, "request.json")))
    assert_equal "afio-rpof-dispatch-request/v0.1", wire.delete("contract_version")
    assert_equal request.reject { |key, _| key == "contract_version" }, wire
    persisted = JSON.parse(File.read(File.join(output, "summary.json")))
    assert_equal "afio-rpof-dispatch-summary/v0.1", persisted["contract_version"]
    assert_includes result.stdout, "provider stdout"
    assert_includes result.stderr, "provider stderr"
  end

  def test_workload_failure_preserves_result_and_exit_status
    request = dispatch_request
    request["jobs"].first["argv"] = [RbConfig.ruby, "-e", "exit 3"]
    result = dispatch(request)
    assert_equal 1, result.exit_status
    assert_equal "workload_failed", result.document["status"]
    assert_equal 3, result.document["jobs"].first["exit_status"]
  end

  def test_infrastructure_failure_and_drain_preserve_pending_evidence
    %w[infrastructure_failed drained interrupted].each do |status|
      configure("exit" => 1, "overrides" => {
        "status" => status, "jobs" => [], "completed_count" => 0,
        "not_started_count" => 1, "not_started_job_ids" => ["opaque-job"]
      })
      assert_equal status, dispatch.document["status"]
    end
  end

  def test_existing_output_is_rejected_without_overwriting_evidence_or_spawning
    output = File.join(@root, "old")
    Dir.mkdir(output)
    File.write(File.join(output, "summary.json"), "prior evidence")
    assert_raises(WorkloadOrchestrator::Error) do
      @client.dispatch(request: dispatch_request, workdir: @root, output_dir: output)
    end
    assert_equal "prior evidence", File.read(File.join(output, "summary.json"))
    refute File.exist?(File.join(@root, "request.json"))
  end

  def test_dispatch_rejects_bad_results
    [
      { "fleet_id" => "different" }, { "fleet_key" => "different" },
      { "worker_indices" => [2] },
      { "job_count" => 2 }, { "completed_count" => -1 },
      { "jobs" => [{ "job_id" => "wrong", "status" => "completed" }] },
      { "jobs" => [{ "job_id" => "opaque-job", "status" => "failed" }] },
      { "status" => "unknown" }, { "status" => "workload_failed" }
    ].each do |overrides|
      configure("overrides" => overrides)
      assert_raises(WorkloadOrchestrator::Error) { dispatch }
    end
  end

  def test_dispatch_rejects_unsupported_or_ambiguous_jobs_before_spawning
    invalid_jobs = [
      [{ "job_id" => "one", "argv" => ["true"], "env" => { "UNSET" => nil } }],
      [{ "job_id" => "one", "argv" => ["true\0"] }],
      [{ "job_id" => "one", "argv" => [""] }],
      [{ "job_id" => "one", "argv" => ["true"], "pool_id" => "not-a-wire-field" }],
      [{ "job_id" => "one", "argv" => ["true"] }] * 2
    ]
    invalid_jobs.each do |jobs|
      assert_raises(WorkloadOrchestrator::Error) { dispatch(dispatch_request.merge("jobs" => jobs)) }
    end
    refute File.exist?(File.join(@root, "request.json"))
  end

  def test_client_has_no_paid_resource_operations
    %i[arm_budget fulfill_execution_pool scale_fleet terminal_shutdown admit_dispatch_worker].each do |method|
      refute_respond_to @client, method
    end
  end

  def test_executable_must_be_explicit_and_executable
    [nil, "rpof", "/missing/rpof", @root, @control].each do |path|
      assert_raises(WorkloadOrchestrator::Error) { Client.new(executable: path) }
    end
  end

  private

  def configure(values)
    File.write(@control, JSON.generate(values))
  end

  def dispatch(request = dispatch_request)
    @attempt = (@attempt || 0) + 1
    @client.dispatch(request: request, workdir: @root, output_dir: File.join(@root, "attempt-#{@attempt}"))
  end

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

  def dispatch_request
    {
      "contract_version" => Contract::DISPATCH_REQUEST,
      "target" => { "fleet_key" => "fixture", "expected_fleet_id" => "opaque-fleet-id", "worker_indices" => [1] },
      "group_by_affinity" => false,
      "jobs" => [{ "job_id" => "opaque-job", "argv" => [RbConfig.ruby, "-e", "print 'ok'"] }]
    }
  end

  # An actual external process with no AFW or provider Ruby dependencies. It
  # runs harmless local fixture jobs, never contacts a provider or creates pods.
  def provider_fixture
    <<~'RUBY'
      require "json"
      require "open3"
      control = JSON.parse(File.read(File.join(__dir__, "control.json")))
      operation = ARGV.shift
      options = ARGV.each_slice(2).to_h
      request = JSON.parse(File.read(options.fetch("--request")))
      File.write(File.join(__dir__, "request.json"), JSON.generate(request))
      Process.kill("TERM", Process.pid) if control["signal"]
      code = 0
      if operation == "capability-check"
        abort "wrong wire version" unless request["contract_version"] == "afio-rpof-capability-check-request/v0.2"
        result = {
          "contract_version" => "afio-rpof-capability-check-result/v0.1", "ready" => true,
          "fleet_key" => request.fetch("fleet_key"), "fleet_id" => "opaque-fleet-id",
          "selected_worker_indices" => [1], "capabilities" => {},
          "diagnostics" => [{ "code" => "fixture", "status" => "PASS", "detail" => "provider detail" }]
        }
        path = options.fetch("--output")
      elsif operation == "dispatch"
        abort "wrong wire version" unless request["contract_version"] == "afio-rpof-dispatch-request/v0.1"
        jobs = request.fetch("jobs").map do |job|
          argv = job.fetch("argv")
          stdout, stderr, status = Open3.capture3(job.fetch("env", {}), [argv.first, argv.first], *argv.drop(1),
                                                chdir: options.fetch("--workdir"))
          { "job_id" => job.fetch("job_id"), "status" => status.success? ? "completed" : "failed",
            "exit_status" => status.exitstatus, "fixture_stdout" => stdout, "fixture_stderr" => stderr }
        end
        failed = jobs.count { |job| job["status"] == "failed" }
        code = failed.zero? ? 0 : 1
        result = {
          "contract_version" => "afio-rpof-dispatch-summary/v0.1",
          "fleet_key" => request.fetch("target").fetch("fleet_key"),
          "fleet_id" => request.fetch("target").fetch("expected_fleet_id"),
          "worker_indices" => request.fetch("target").fetch("worker_indices"),
          "status" => failed.zero? ? "completed" : "workload_failed", "job_count" => jobs.length,
          "completed_count" => jobs.length - failed, "failed_count" => failed,
          "not_started_count" => 0, "not_started_job_ids" => [], "jobs" => jobs
        }
        path = File.join(options.fetch("--output"), "summary.json")
      else
        abort "unexpected operation"
      end
      result.merge!(control.fetch("overrides", {}))
      File.write(path, control.fetch("raw") { JSON.generate(result) }) unless control["missing"]
      puts "provider stdout"
      warn "provider stderr"
      exit control.fetch("exit", code)
    RUBY
  end
end
