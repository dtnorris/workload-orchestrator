# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "open3"
require "stringio"

class DynamicCliTest < Minitest::Test
  include WloTestSupport

  ENDPOINT = "http://127.0.0.1:19441"
  MODEL = "qualified-model:latest"
  DIGEST = "a" * 64

  def setup
    @tmp = Dir.mktmpdir("wlo-dynamic-cli-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    @source_state = File.join(@tmp, "source-state")
    @manager_pids = []
    FileUtils.mkdir_p([@workdir, @source_state])
    @plan = write_dynamic_plan
    @source = write_source_executable
    write_source_mode("arrive")
  end

  def teardown
    @manager_pids.each { |pid| terminate_process(pid) }
    FileUtils.rm_rf(@tmp)
  end

  def test_unbound_dynamic_plan_is_inspectable_without_profile_workers_or_source
    code, out, error = run_cli("plan", @plan, "--workdir", @workdir)

    assert_equal 0, code, error
    assert_includes out, "Scheduling: work-conserving priority"
    assert_includes out, "Dynamic placement: unresolved / registry-selected at runtime"
    assert_includes out, "placement=registry-selected"
    assert_includes out, "ollama=#{MODEL}"
    assert_includes out, "Zero-cost gate: PASS"
  end

  def test_run_requires_dynamic_source_before_creating_execution_state
    code, _out, error = run_cli(
      "run", @plan, "--workdir", @workdir, "--output", @output
    )

    assert_equal 1, code
    assert_includes error, "--worker-source-command FILE"
    refute File.exist?(@output)
  end

  def test_command_source_repolls_then_runs_locally_with_exact_endpoint
    code, _out, error = run_cli(*dynamic_args("run"))

    assert_equal 0, code, error
    assert_operator source_calls, :>=, 2
    assert_equal "#{ENDPOINT}\n", File.read(File.join(@output, "runs", "job-1", "stdout.log"))
    metadata = JSON.parse(File.read(File.join(@output, "runs", "job-1", "metadata.json")))
    assert_equal ENDPOINT, metadata.dig("worker_execution_identity", "endpoint")
  end

  def test_source_failure_and_malformed_registry_wait_without_legacy_fallback
    {
      "fail" => ["exit 23", "registry deliberately unavailable"],
      "malformed" => ["invalid dynamic worker registry JSON", nil]
    }.each do |mode, (expected, extra)|
      output = File.join(@tmp, "output-#{mode}")
      write_source_mode(mode)
      legacy_before = loaded_legacy_features

      code, out, error = run_cli(*dynamic_args("start", output: output))
      assert_equal 0, code, error
      pid = Integer(out.match(/PID (\d+)/)[1])
      @manager_pids << pid
      health_path = File.join(output, "dynamic-workers", "health.json")
      wait_until do
        File.file?(health_path) && JSON.parse(File.read(health_path)).fetch("last_poll_result") == "failure"
      end
      status_code, status_out, status_error = run_cli("status", @plan, "--output", output, "--json")
      document = JSON.parse(status_out)
      health = document.fetch("worker_sources").fetch(0)

      assert_equal 0, status_code, status_error
      assert_equal "required", health.fetch("policy")
      assert_equal "unavailable", health.fetch("state")
      assert health.fetch("blocking")
      assert_includes health.fetch("failure_reason"), expected
      assert_includes health.fetch("failure_reason"), extra if extra
      assert_equal "REQUIRED_WORKER_SOURCE_UNAVAILABLE",
                   document.dig("pool_status", 0, "reason")
      refute JSON.parse(File.read(File.join(output, "execution.json"))).key?("dispatch_halt")
      assert_equal legacy_before, loaded_legacy_features

      assert_equal 0, run_cli("pause", "--output", output).first
      wait_until { manager_status(output) == "paused" }
      wait_until { !process_alive?(pid) }
      @manager_pids.delete(pid)
    end
  end

  def test_worker_check_directs_dynamic_operators_to_registry_command
    code, _out, error = run_cli("worker-check", @plan)

    assert_equal 1, code
    assert_includes error, "configured publisher's own tooling"
    refute File.exist?(@output)
  end

  def test_detached_start_keeps_polling_after_parent_exit_and_resume_reuses_checkpoint
    write_source_mode("zero")
    code, out, error = run_cli(*dynamic_args("start"))
    assert_equal 0, code, error
    pid = Integer(out.match(/PID (\d+)/)[1])
    @manager_pids << pid

    wait_until { source_calls >= 2 }
    assert process_alive?(pid), "detached manager stopped before capacity appeared"
    checkpoint_before_pause = JSON.parse(
      File.read(File.join(@output, "dynamic-workers", "checkpoint.json"))
    )

    assert_equal 0, run_cli("pause", "--output", @output).first
    wait_until { manager_status == "paused" }
    wait_until { !process_alive?(pid) }
    @manager_pids.delete(pid)

    write_source_mode("ready")
    code, _out, error = run_cli(*dynamic_args("resume"))

    assert_equal 0, code, error
    checkpoint_after_resume = JSON.parse(
      File.read(File.join(@output, "dynamic-workers", "checkpoint.json"))
    )
    assert_operator checkpoint_after_resume.fetch("revision"), :>, checkpoint_before_pause.fetch("revision")
    assert_equal checkpoint_before_pause.fetch("registry_id"), checkpoint_after_resume.fetch("registry_id")
    assert_equal "#{ENDPOINT}\n", File.read(File.join(@output, "runs", "job-1", "stdout.log"))
    assert_equal 1, JSON.parse(File.read(File.join(@output, "runs", "job-1", "metadata.json"))).fetch("attempt")
  end

  private

  def write_dynamic_plan
    path = File.join(@tmp, "dynamic-plan.json")
    document = {
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "dynamic-cli-fixture",
      "failure_policy" => {
        "max_consecutive_failures" => 2,
        "max_total_failures" => 3
      },
      "pools" => [{
        "pool_id" => "dynamic-pool",
        "required_labels" => ["inference"],
        "requirements" => {
          "ollama" => {
            "model" => MODEL,
            "expected_digest" => DIGEST,
            "required_context_length" => 131_072,
            "require_fully_gpu_resident" => true
          }
        }
      }],
      "jobs" => [{
        "job_id" => "job-1",
        "pool_id" => "dynamic-pool",
        "argv" => [RbConfig.ruby, "-e", "puts ENV.fetch('WLO_WORKER_ENDPOINT')"],
        "env" => {},
        "depends_on_job_ids" => []
      }]
    }
    File.write(path, "#{JSON.pretty_generate(document)}\n")
    path
  end

  def write_source_executable
    path = File.join(@tmp, "worker-source")
    File.write(path, <<~RUBY)
      #!#{RbConfig.ruby}
      require "digest"
      require "json"
      require "time"

      root = ARGV.fetch(0)
      abort "missing --json argv" unless ARGV.fetch(1) == "--json"
      count_path = File.join(root, "calls")
      count = File.file?(count_path) ? Integer(File.read(count_path)) + 1 : 1
      File.write(count_path, count.to_s)
      mode = File.read(File.join(root, "mode")).strip
      if mode == "fail"
        warn "registry deliberately unavailable"
        exit 23
      end
      if mode == "malformed"
        STDOUT.write("{not-json")
        exit
      end

      labels = ["inference", "remote"]
      models = [{
        "model" => #{MODEL.inspect},
        "digest" => #{DIGEST.inspect},
        "context_length" => 131_072,
        "fully_gpu_resident" => true
      }]
      capabilities = { "gpu_id" => "NVIDIA A40", "ollama" => { "models" => models } }
      fingerprint_models = models.map do |model|
        {
          "context_length" => model.fetch("context_length"),
          "digest" => model.fetch("digest"),
          "fully_gpu_resident" => model.fetch("fully_gpu_resident"),
          "model" => model.fetch("model")
        }
      end
      fingerprint = Digest::SHA256.hexdigest(JSON.generate(
        "gpu_id" => capabilities.fetch("gpu_id"),
        "labels" => labels,
        "ollama_models" => fingerprint_models
      ))
      worker = {
        "worker_id" => "worker-1",
        "generation_id" => "generation-1",
        "endpoint" => #{ENDPOINT.inspect},
        "state" => "READY",
        "labels" => labels,
        "capabilities" => capabilities,
        "capability_fingerprint" => fingerprint
      }
      workers = %w[ready].include?(mode) || (mode == "arrive" && count >= 2) ? [worker] : []
      now = Time.now.utc
      STDOUT.write(JSON.generate(
        "contract_version" => "dynamic-worker-registry/v0.1",
        "registry_id" => "dynamic-cli-registry",
        "revision" => count,
        "published_at" => (now - 120 + count).iso8601(0),
        "expires_at" => (now + 300).iso8601(0),
        "workers" => workers
      ))
    RUBY
    FileUtils.chmod(0o755, path)
    path
  end

  def dynamic_args(command, output: @output)
    [
      command, @plan, "--workdir", @workdir, "--output", output,
      "--worker-source-command", @source,
      "--worker-source-arg", @source_state,
      "--worker-source-arg=--json"
    ]
  end

  def run_cli(*argv)
    out = StringIO.new
    error = StringIO.new
    code = WorkloadOrchestrator::CLI.new(
      argv, out: out, err: error, worker_poll_interval: 0.01
    ).run
    [code, out.string, error.string]
  end

  def write_source_mode(mode)
    File.write(File.join(@source_state, "mode"), mode)
  end

  def source_calls
    path = File.join(@source_state, "calls")
    File.file?(path) ? Integer(File.read(path)) : 0
  end

  def loaded_legacy_features
    names = %w[
      legacy_rpof_runner rpof_capacity_client pool_fulfillment rpof_client
      rpof_contract rpof_contract_values rpof_dispatch_validation rpof_readiness
    ]
    $LOADED_FEATURES.select { |path| names.any? { |name| path.end_with?("/#{name}.rb") } }
  end

  def manager_status(output = @output)
    path = File.join(output, "manager.json")
    return unless File.file?(path)

    pointer = JSON.parse(File.read(path))
    JSON.parse(File.read(pointer.fetch("record_path"))).fetch("status")
  end

  def wait_until(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "timed out waiting for dynamic CLI state" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end

  def process_alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  def terminate_process(pid)
    return unless process_alive?(pid)

    Process.kill("TERM", pid)
    Process.wait(pid)
  rescue Errno::ECHILD, Errno::ESRCH
    nil
  end
end
