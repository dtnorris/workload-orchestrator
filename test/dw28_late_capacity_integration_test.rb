# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "time"

class Dw28LateCapacityIntegrationTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:00:25Z")
  PUBLISHED_AT = [
    Time.iso8601("2030-01-01T00:00:00Z"),
    Time.iso8601("2030-01-01T00:00:10Z"),
    Time.iso8601("2030-01-01T00:00:20Z")
  ].freeze
  REQUIRED_MODEL = "model-a:latest"
  INCOMPATIBLE_MODEL = "model-b:latest"
  RPOF_ROOT = File.expand_path(ENV.fetch("RPOF_REPO_ROOT", "../../runpod-ollama-fleet"), __dir__)
  RPOF_REGISTRY_PATH = File.join(RPOF_ROOT, "lib/runpod_ollama_fleet/dynamic_worker_registry.rb")

  require RPOF_REGISTRY_PATH if File.file?(RPOF_REGISTRY_PATH)

  class FakeState
    attr_reader :phase

    def initialize(root, fleets)
      @root = root
      @fleets = fleets
      @phase = 0
    end

    def advance!
      raise "all registry phases are already published" if phase >= 2

      @phase += 1
    end

    def current
      fleet = @fleets[phase]
      fleet && Marshal.load(Marshal.dump(fleet))
    end

    def artifact_dir(fleet_id, name)
      File.join(@root, fleet_id, name)
    end

    def registry_identity(index:, observed_pod_id:)
      worker = current.fetch("workers").find { |candidate| candidate.fetch("index") == index }
      raise "publisher observed the wrong pod" unless worker.fetch("pod_id") == observed_pod_id

      worker.slice("worker_id", "generation_id")
    end
  end

  class FakeProcess
    def alive?(_pid) = true
    def matches?(_pid, _identity) = true
  end

  class FakeHealth
    Result = Struct.new(:healthy, :version, :detail, keyword_init: true)

    def check(_endpoint)
      Result.new(healthy: true, version: "fixture", detail: nil)
    end
  end

  class RpofPublishingSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls, :published_revisions

    def initialize(publisher:, state:)
      super()
      @publisher = publisher
      @state = state
      @calls = 0
      @snapshots = {}
      @published_revisions = []
    end

    def latest_snapshot
      @calls += 1
      @snapshots[@state.phase] ||= serialize_snapshot
    end

    def advance!
      @state.advance!
    end

    private

    # This is the exact serialization used by RPOF's `workers --json` command.
    def serialize_snapshot
      snapshot = @publisher.snapshot
      @published_revisions << snapshot.fetch("revision")
      "#{JSON.generate(snapshot)}\n"
    end
  end

  def setup
    skip "set RPOF_REPO_ROOT to a runpod-ollama-fleet checkout" unless File.file?(RPOF_REGISTRY_PATH)

    @tmp = Dir.mktmpdir("wlo-dw28-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    @rpof_state_root = File.join(@tmp, "rpof-publisher")
    FileUtils.mkdir_p(@workdir)
    @fleets = [nil, fleet(INCOMPATIBLE_MODEL, 11_442), fleet(REQUIRED_MODEL, 11_441)]
    @rpof_state = FakeState.new(File.join(@tmp, "rpof-state"), @fleets)
    @fleets.compact.each { |fleet| write_capability_evidence(fleet) }
  end

  def teardown
    FileUtils.remove_entry(@tmp) if @tmp && File.exist?(@tmp)
  end

  def test_capacity_published_after_start_launches_pending_job_without_resume
    plan = execution_plan
    job = plan.jobs.fetch(0)
    publisher = rpof_registry_class.new(
      state_root: @rpof_state_root,
      repo_root: RPOF_ROOT,
      clock: -> { PUBLISHED_AT.fetch(@rpof_state.phase) },
      ttl_seconds: 300,
      process_adapter: FakeProcess.new,
      health_checker: FakeHealth.new,
      fleet_sources: [{ "fleet_key" => "dw28", "state" => @rpof_state }],
      id_generator: -> { "rpof-dw28" }
    )
    source = RpofPublishingSource.new(publisher: publisher, state: @rpof_state)
    execution_started = Queue.new
    release_execution = Queue.new
    command_observation = Queue.new
    command_calls = 0
    executor = lambda do |environment, *argv, chdir:|
      command_calls += 1
      command_observation << observe_durable_launch(environment, argv, chdir, job)
      execution_started << true
      release_execution.pop
      command_result(stdout: "fake score complete\n")
    end
    original_execution = nil
    runner = nil
    sleep_calls = 0
    sleeper = lambda do |_seconds, stop|
      sleep_calls += 1
      case sleep_calls
      when 1
        original_execution = assert_waiting_phase(runner, job, revision: 1, expected_model: nil)
        source.advance!
      when 2
        assert_waiting_phase(runner, job, revision: 2, expected_model: INCOMPATIBLE_MODEL)
        source.advance!
      else
        execution_started.pop
        release_execution << true
        runner.send(:dynamic_poll_sleep, 1.0, stop)
      end
    end
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_registry_sleeper: sleeper,
      worker_poll_interval: 1.0,
      command_executor: executor
    )

    assert_equal "completed", runner.run

    observed = command_observation.pop
    assert_durable_launch(observed, publisher, plan, job)
    assert_single_completed_execution(runner, original_execution, plan, job)
    assert_equal 1, command_calls
    assert_equal [1, 2, 3], source.published_revisions
    assert_operator source.calls, :>=, 3
  end

  private

  def rpof_registry_class
    RunpodOllamaFleet::DynamicWorkerRegistry
  end

  def execution_plan
    WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "dw28-late-capacity",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 2 },
      "pools" => [{
        "pool_id" => "model-a-pool",
        "required_labels" => ["inference"],
        "requirements" => {
          "ollama" => {
            "model" => REQUIRED_MODEL,
            "expected_digest" => model_digest(REQUIRED_MODEL),
            "required_context_length" => 131_072,
            "require_fully_gpu_resident" => true,
            "required_gpu_id" => "NVIDIA A40"
          }
        }
      }],
      "jobs" => [{
        "job_id" => "score-later",
        "pool_id" => "model-a-pool",
        "argv" => ["fake-local-scorer", "score-later"]
      }]
    ))
  end

  def fleet(model, port)
    slug = model.delete(":")
    worker_id = "rpof-#{model}-worker"
    generation_id = "rpof-#{model}-generation-1"
    {
      "fleet_id" => "dw28-#{slug}",
      "status" => "active",
      "created_at_utc" => "2029-12-31T23:59:00Z",
      "gpu" => { "id" => "NVIDIA A40" },
      "workers" => [{
        "index" => 1,
        "name" => "#{slug}-worker",
        "pod_id" => "#{slug}-pod",
        "worker_id" => worker_id,
        "generation_id" => generation_id,
        "generation" => 1,
        "created_at_utc" => "2029-12-31T23:59:00Z",
        "status" => "active",
        "gpu_id" => "NVIDIA A40",
        "local_ollama_url" => "http://127.0.0.1:#{port}",
        "model" => model
      }]
    }
  end

  def write_capability_evidence(fleet)
    worker = fleet.fetch("workers").fetch(0)
    write_bootstrap(fleet, worker)
    root = @rpof_state.artifact_dir(fleet.fetch("fleet_id"), "tunnels")
    FileUtils.mkdir_p(root)
    File.write(File.join(root, "tunnels.json"), JSON.pretty_generate(
      "fleet_id" => fleet.fetch("fleet_id"),
      "workers" => [{
        "index" => worker.fetch("index"),
        "pod_id" => worker.fetch("pod_id"),
        "worker_id" => worker.fetch("worker_id"),
        "generation_id" => worker.fetch("generation_id"),
        "pid" => 12_345,
        "endpoint" => worker.fetch("local_ollama_url"),
        "process_identity" => {
          "forward" => "fixture",
          "ssh_port" => 22_001,
          "target" => "root@fixture"
        }
      }]
    ) + "\n")
  end

  def write_bootstrap(fleet, worker)
    run_id = "dw28-bootstrap"
    root = @rpof_state.artifact_dir(fleet.fetch("fleet_id"), "bootstrap")
    FileUtils.mkdir_p(File.join(root, run_id))
    File.write(File.join(root, "current"), "#{run_id}\n")
    model = worker.fetch("model")
    File.write(File.join(root, run_id, "bootstrap.json"), JSON.pretty_generate(
      "schema_version" => 2,
      "bootstrap_run_id" => run_id,
      "fleet_id" => fleet.fetch("fleet_id"),
      "status" => "passed",
      "models" => [model],
      "expected_digests" => { model => model_digest(model) },
      "context" => 131_072,
      "workers" => [{
        "index" => worker.fetch("index"),
        "pod_id" => worker.fetch("pod_id"),
        "worker_id" => worker.fetch("worker_id"),
        "generation_id" => worker.fetch("generation_id"),
        "status" => "passed",
        "provenance_error" => nil,
        "provenance" => {
          "gpu" => { "name" => "NVIDIA A40" },
          "models" => {
            model => {
              "digest" => model_digest(model),
              "context_length" => 131_072,
              "size_bytes" => 20_000,
              "size_vram_bytes" => 20_000,
              "fully_gpu_resident" => true
            }
          }
        }
      }]
    ) + "\n")
  end

  def model_digest(model)
    Digest::SHA256.hexdigest(model)
  end

  def assert_waiting_phase(runner, job, revision:, expected_model:)
    assert_equal "running", runner.store.status
    assert_equal({ "pending" => 1 }, runner.store.counts)
    assert_nil runner.store.metadata_for(job)
    assert_empty Dir[File.join(@output, "runs", "*")]
    assert_empty Dir[File.join(@output, "claims", "*")]
    checkpoint = runner.worker_registry_poller.accepted_checkpoint
    assert_equal revision, checkpoint.fetch("revision")
    if expected_model
      assert_equal expected_model,
                   checkpoint.dig("workers", 0, "worker_snapshot", "capabilities", "ollama", "models", 0, "model")
    else
      assert_empty checkpoint.fetch("workers")
    end
    JSON.parse(File.read(File.join(@output, "execution.json")))
  end

  def observe_durable_launch(environment, argv, chdir, job)
    metadata = JSON.parse(File.read(File.join(@output, "runs", job.id, "metadata.json")))
    checkpoint = JSON.parse(File.read(File.join(@output, "dynamic-workers", "checkpoint.json")))
    claim_path = File.join(@output, "claims", "#{Digest::SHA256.hexdigest(job.id)}.lock")
    {
      "environment" => environment.dup,
      "argv" => argv,
      "chdir" => chdir,
      "metadata" => metadata,
      "checkpoint" => checkpoint,
      "claim" => JSON.parse(File.read(claim_path)),
      "execution" => JSON.parse(File.read(File.join(@output, "execution.json")))
    }
  end

  def assert_durable_launch(observed, publisher, plan, job)
    metadata = observed.fetch("metadata")
    checkpoint = observed.fetch("checkpoint")
    identity = metadata.fetch("worker_execution_identity")
    snapshot = metadata.fetch("worker_snapshot")
    expected_identity = {
      "registry_id" => "rpof-dw28",
      "worker_id" => "rpof-#{REQUIRED_MODEL}-worker",
      "generation_id" => "rpof-#{REQUIRED_MODEL}-generation-1",
      "endpoint" => "http://127.0.0.1:11441",
      "capability_fingerprint" => snapshot.fetch("capability_fingerprint")
    }

    assert_equal job.argv, observed.fetch("argv")
    assert_equal @workdir, observed.fetch("chdir")
    assert_equal "running", metadata.fetch("status")
    assert_equal 1, metadata.fetch("attempt")
    assert_equal expected_identity, identity
    assert_equal 3, metadata.dig("worker_registry_binding", "registry_revision")
    assert_equal checkpoint.fetch("snapshot_sha256"),
                 metadata.dig("worker_registry_binding", "registry_snapshot_sha256")
    assert_equal REQUIRED_MODEL, snapshot.dig("capabilities", "ollama", "models", 0, "model")
    assert_equal rpof_registry_class.capability_fingerprint(snapshot),
                 snapshot.fetch("capability_fingerprint")
    assert_equal "claimed", observed.dig("claim", "state")
    assert_equal expected_identity.fetch("endpoint"), observed.dig("environment", "AF_OLLAMA_BASE_URL")
    assert_equal "running", observed.dig("execution", "status")
    assert_equal plan.id, observed.dig("execution", "plan_id")
    refute metadata.key?("remote_request")
    refute metadata.key?("provider_job_id")
    refute runner_has_legacy_dispatch?(publisher)
  end

  def runner_has_legacy_dispatch?(publisher)
    publisher.respond_to?(:dispatch) || Dir.exist?(File.join(@output, "runs", "score-later", "provider-attempt-1"))
  end

  def assert_single_completed_execution(runner, original, plan, job)
    current = JSON.parse(File.read(File.join(@output, "execution.json")))
    metadata = runner.store.metadata_for(job)
    checkpoint = runner.worker_registry_poller.accepted_checkpoint

    assert_equal "completed", current.fetch("status")
    assert_equal({ "complete" => 1 }, runner.store.counts)
    assert_equal "complete", metadata.fetch("status")
    assert_equal 1, metadata.fetch("attempt")
    assert_equal original.fetch("created_at"), current.fetch("created_at")
    assert_equal original.fetch("plan_id"), current.fetch("plan_id")
    assert_equal plan.sha256, current.fetch("plan_sha256")
    assert_equal 3, checkpoint.fetch("revision")
    assert_equal [2, 3], checkpoint.fetch("reconciliation_history").map { |row| row.fetch("revision") }
    assert_equal 1, Dir[File.join(@tmp, "**", "execution.json")].length
    assert_equal "fake score complete\n", File.read(File.join(@output, "runs", job.id, "stdout.log"))
  end
end
