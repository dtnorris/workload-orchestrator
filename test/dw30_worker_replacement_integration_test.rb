# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "timeout"

class Dw30WorkerReplacementIntegrationTest < Minitest::Test
  include WloTestSupport

  MODEL = "replacement-model:latest"
  MODEL_DIGEST = Digest::SHA256.hexdigest(MODEL)
  WLO_NOW = Time.iso8601("2030-01-01T00:00:30Z")
  RPOF_ROOT = File.expand_path(ENV.fetch("RPOF_REPO_ROOT", "../../runpod-ollama-fleet"), __dir__)
  RPOF_REGISTRY_PATH = File.join(RPOF_ROOT, "lib/runpod_ollama_fleet/dynamic_worker_registry.rb")

  require RPOF_REGISTRY_PATH if File.file?(RPOF_REGISTRY_PATH)

  FleetWorker = Struct.new(:index, :pod_id, :name, :host, :ssh_port, :hourly_rate, keyword_init: true)

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

    def initialize(publisher:)
      super()
      @publisher = publisher
      @phase = 0
      @calls = 0
      @snapshots = {}
      @published_revisions = []
    end

    def latest_snapshot
      @calls += 1
      @snapshots[@phase] ||= serialize_snapshot
    end

    def publish_replacement!
      raise "replacement registry revision is already active" unless @phase.zero?

      @phase = 1
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

    @tmp = Dir.mktmpdir("wlo-dw30-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    @provider_now = Time.iso8601("2030-01-01T00:00:00Z")
    @registry_now = @provider_now
    FileUtils.mkdir_p(@workdir)
    @fleet_state = LocalModelEvaluation::RunpodFleetState.new(
      root: File.join(@tmp, "rpof-fleets"),
      clock: -> { @provider_now }
    )
    @fleet_state.activate(
      workers: [fleet_worker("pod-generation-1")],
      cloud: "SECURE",
      gpu_id: "NVIDIA A40",
      image: "fixture/image"
    )
    write_capability_evidence
    @poll_ready = Queue.new
    @continue_polling = Queue.new
    @old_command_started = Queue.new
    @release_old_command = Queue.new
    @loss_recorded = Queue.new
  end

  def teardown
    @continue_polling&.push(true)
    @release_old_command&.push(true)
    @runner_thread&.join(0.1)
    @runner_thread&.kill if @runner_thread&.alive?
    FileUtils.remove_entry(@tmp) if @tmp && File.exist?(@tmp)
  end

  def test_replacement_generation_cannot_inherit_or_complete_the_bound_attempt
    plan = execution_plan
    job = plan.jobs.fetch(0)
    publisher = rpof_registry_class.new(
      state_root: File.join(@tmp, "rpof-publisher"),
      repo_root: RPOF_ROOT,
      clock: -> { @registry_now },
      ttl_seconds: 300,
      process_adapter: FakeProcess.new,
      health_checker: FakeHealth.new,
      fleet_sources: [{ "fleet_key" => "dw30", "state" => @fleet_state }],
      id_generator: -> { "rpof-dw30" }
    )
    source = RpofPublishingSource.new(publisher: publisher)
    launches = Queue.new
    launch_count = 0
    retrying = false
    executor = lambda do |environment, *argv, chdir:|
      launch_count += 1
      launches << observe_launch(environment, argv, chdir, job)
      if launch_count == 1
        @old_command_started << true
        queue_pop(@release_old_command)
        command_result(stdout: "late generation-1 success\n")
      else
        command_result(stdout: "generation-2 success\n")
      end
    end
    runner = nil
    sleeper = lambda do |_seconds, stop|
      if retrying
        runner.send(:dynamic_poll_sleep, 1.0, stop)
      else
        @poll_ready << true
        queue_pop(@continue_polling)
      end
    end
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { WLO_NOW },
      worker_registry_sleeper: sleeper,
      worker_poll_interval: 1.0,
      command_executor: executor
    )
    observe_losses(runner.store)

    initial_execution = run_until_first_launch(runner)
    first_launch = queue_pop(launches)
    original_identity = first_launch.dig("metadata", "worker_execution_identity")
    original_snapshot = first_launch.dig("metadata", "worker_snapshot")
    assert_first_launch(first_launch, plan, job)

    replacement_identity = replace_rpof_generation
    source.publish_replacement!
    @continue_polling << true
    assert_equal :recorded_in_doubt, queue_pop(@loss_recorded)
    loss_metadata = read_metadata(job)
    assert_loss_state(runner, loss_metadata, original_identity, original_snapshot, replacement_identity)

    @release_old_command << true
    assert_equal "infrastructure_failed", finish_runner_thread
    after_late_result = read_metadata(job)
    assert_late_result_is_evidence_only(runner, after_late_result, original_identity, original_snapshot)
    assert_equal 1, launch_count

    before_retry = JSON.parse(JSON.generate(after_late_result))
    assert_equal [job.id], runner.store.retry_failed!(reason: "operator verified retry is safe", all: true)
    retrying = true
    assert_equal "completed", runner.run(resume: true)
    second_launch = queue_pop(launches)
    assert_retry_result(
      runner, second_launch, before_retry, replacement_identity,
      original_identity, original_snapshot, initial_execution, plan, job
    )
    assert_equal 2, launch_count
    assert_equal [1, 2], source.published_revisions
    assert_operator source.calls, :>=, 3
  end

  private

  def rpof_registry_class
    RunpodOllamaFleet::DynamicWorkerRegistry
  end

  def fleet_worker(pod_id)
    FleetWorker.new(
      index: 1,
      pod_id: pod_id,
      name: "dw30-worker",
      host: "198.51.100.11",
      ssh_port: 22_001,
      hourly_rate: 0.0
    )
  end

  def execution_plan
    WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "dw30-worker-replacement",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 2 },
      "pools" => [{
        "pool_id" => "replacement-pool",
        "required_labels" => ["inference"],
        "requirements" => {
          "ollama" => {
            "model" => MODEL,
            "expected_digest" => MODEL_DIGEST,
            "required_context_length" => 131_072,
            "require_fully_gpu_resident" => true,
            "required_gpu_id" => "NVIDIA A40"
          }
        }
      }],
      "jobs" => [{
        "job_id" => "replacement-job",
        "pool_id" => "replacement-pool",
        "argv" => ["fake-local-scorer", "replacement-job"]
      }]
    ))
  end

  def write_capability_evidence
    fleet = @fleet_state.current
    worker = fleet.fetch("workers").fetch(0)
    write_bootstrap(fleet, worker)
    root = @fleet_state.artifact_dir(fleet.fetch("fleet_id"), "tunnels")
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
    run_id = "dw30-bootstrap"
    root = @fleet_state.artifact_dir(fleet.fetch("fleet_id"), "bootstrap")
    FileUtils.mkdir_p(File.join(root, run_id))
    File.write(File.join(root, "current"), "#{run_id}\n")
    File.write(File.join(root, run_id, "bootstrap.json"), JSON.pretty_generate(
      "schema_version" => 2,
      "bootstrap_run_id" => run_id,
      "fleet_id" => fleet.fetch("fleet_id"),
      "status" => "passed",
      "models" => [MODEL],
      "expected_digests" => { MODEL => MODEL_DIGEST },
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
            MODEL => {
              "digest" => MODEL_DIGEST,
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

  def observe_losses(store)
    signal = @loss_recorded
    observer = Module.new do
      define_method(:record_dynamic_worker_loss!) do |**attributes|
        result = super(**attributes)
        signal << result
        result
      end
    end
    store.singleton_class.prepend(observer)
  end

  def run_until_first_launch(runner)
    result = Queue.new
    @runner_thread = Thread.new do
      result << [:ok, runner.run]
    rescue StandardError => e
      result << [:error, e]
    end
    @runner_result = result
    queue_pop(@old_command_started)
    queue_pop(@poll_ready)
    JSON.parse(File.read(File.join(@output, "execution.json")))
  end

  def finish_runner_thread
    kind, value = queue_pop(@runner_result)
    raise value if kind == :error

    @runner_thread.join
    value
  end

  def replace_rpof_generation
    before = @fleet_state.current.fetch("workers").fetch(0)
    endpoint = before.fetch("local_ollama_url")
    identity = @fleet_state.registry_identity(index: 1, observed_pod_id: before.fetch("pod_id"))
    @fleet_state.begin_replacement(1)
    @fleet_state.mark_replacement_destroyed(1)
    @provider_now += 10
    @registry_now += 10
    @fleet_state.complete_replacement(
      worker: fleet_worker("pod-generation-2"),
      created_at_utc: @provider_now.iso8601
    )
    write_capability_evidence
    replacement = @fleet_state.current.fetch("workers").fetch(0)
    replacement_identity = @fleet_state.registry_identity(
      index: 1,
      observed_pod_id: replacement.fetch("pod_id")
    )
    assert_equal identity.fetch("worker_id"), replacement_identity.fetch("worker_id")
    refute_equal identity.fetch("generation_id"), replacement_identity.fetch("generation_id")
    assert_equal endpoint, replacement.fetch("local_ollama_url")
    replacement_identity
  end

  def observe_launch(environment, argv, chdir, job)
    claim_path = File.join(@output, "claims", "#{Digest::SHA256.hexdigest(job.id)}.lock")
    {
      "environment" => environment.dup,
      "argv" => argv,
      "chdir" => chdir,
      "metadata" => read_metadata(job),
      "claim" => JSON.parse(File.read(claim_path)),
      "execution" => JSON.parse(File.read(File.join(@output, "execution.json")))
    }
  end

  def assert_first_launch(observed, plan, job)
    metadata = observed.fetch("metadata")
    identity = metadata.fetch("worker_execution_identity")
    snapshot = metadata.fetch("worker_snapshot")

    assert_equal job.argv, observed.fetch("argv")
    assert_equal @workdir, observed.fetch("chdir")
    assert_equal "running", metadata.fetch("status")
    assert_equal 1, metadata.fetch("attempt")
    assert_equal 1, metadata.dig("worker_registry_binding", "registry_revision")
    rpof_identity = @fleet_state.registry_identity(
      index: 1,
      observed_pod_id: @fleet_state.current.dig("workers", 0, "pod_id")
    )
    assert_equal rpof_identity, identity.slice("worker_id", "generation_id")
    assert_equal identity.fetch("generation_id"), snapshot.fetch("generation_id")
    assert_equal identity.fetch("capability_fingerprint"), snapshot.fetch("capability_fingerprint")
    assert_equal identity.fetch("endpoint"), observed.dig("environment", "WLO_WORKER_ENDPOINT")
    assert_equal "claimed", observed.dig("claim", "state")
    assert_equal "running", observed.dig("execution", "status")
    assert_equal plan.id, observed.dig("execution", "plan_id")
    refute metadata.key?("remote_request")
    refute metadata.key?("provider_job_id")
  end

  def assert_loss_state(runner, metadata, original_identity, original_snapshot, replacement_identity)
    checkpoint = runner.worker_registry_poller.accepted_checkpoint
    evidence = metadata.fetch("evidence")
    observed_replacement = evidence.fetch("observed_replacement_identity")

    assert_equal 2, checkpoint.fetch("revision")
    assert_equal [1, 2], checkpoint.fetch("reconciliation_history").map { |row| row.fetch("revision") }
    assert_equal ["generation"],
                 checkpoint.dig("reconciliation_history", 1, "changes", "changed", 0, "kinds")
    assert_equal "failed", metadata.fetch("status")
    assert_equal "non_operational", metadata.fetch("failure_class")
    assert_equal "dynamic_worker_loss_in_doubt", evidence.fetch("kind")
    assert_equal "worker_generation_replaced", evidence.fetch("reason")
    assert_equal false, evidence.fetch("outcome_known")
    assert_equal original_identity, metadata.fetch("worker_execution_identity")
    assert_equal original_snapshot, metadata.fetch("worker_snapshot")
    assert_equal original_identity, evidence.fetch("worker_execution_identity")
    assert_equal replacement_identity,
                 observed_replacement.slice("worker_id", "generation_id")
    assert_equal original_identity.fetch("endpoint"), observed_replacement.fetch("endpoint")
    refute_equal original_identity.fetch("generation_id"), observed_replacement.fetch("generation_id")
    assert_equal "infrastructure_failed", runner.store.status
    assert_equal "dynamic_worker_loss", runner.store.dispatch_halt.fetch("kind")
    assert_equal 1, runner.store.dispatch_halt.fetch("attempt")
    refute metadata.key?("late_evidence")
  end

  def assert_late_result_is_evidence_only(runner, metadata, original_identity, original_snapshot)
    late = metadata.fetch("late_evidence").fetch(0)

    assert_equal "failed", metadata.fetch("status")
    assert_equal "dynamic_worker_loss_in_doubt", metadata.dig("evidence", "kind")
    assert_equal "worker_generation_replaced", metadata.dig("evidence", "reason")
    assert_equal original_identity, metadata.fetch("worker_execution_identity")
    assert_equal original_snapshot, metadata.fetch("worker_snapshot")
    assert_equal "late_dynamic_attempt_terminal", late.fetch("kind")
    assert_equal "complete", late.fetch("status")
    assert_equal 0, late.fetch("exit_status")
    assert_equal "infrastructure_failed", runner.store.status
    assert runner.store.dispatch_halted?
  end

  def assert_retry_result(runner, observed, attempt_one, replacement_identity,
                          original_identity, original_snapshot, initial_execution, plan, job)
    metadata = runner.store.metadata_for(job)
    archive_path = File.join(@output, "attempts", job.id, "attempt-1", "metadata.json")
    archive = JSON.parse(File.read(archive_path))
    current_execution = JSON.parse(File.read(File.join(@output, "execution.json")))
    expected_replacement = observed.dig("metadata", "worker_execution_identity")

    assert_equal attempt_one, archive
    assert_equal original_identity, archive.fetch("worker_execution_identity")
    assert_equal original_snapshot, archive.fetch("worker_snapshot")
    assert_equal "dynamic_worker_loss_in_doubt", archive.dig("evidence", "kind")
    assert_equal "late_dynamic_attempt_terminal", archive.dig("late_evidence", 0, "kind")
    assert_equal 2, observed.dig("metadata", "attempt")
    assert_equal "running", observed.dig("metadata", "status")
    assert_equal 2, observed.dig("metadata", "worker_registry_binding", "registry_revision")
    assert_equal replacement_identity,
                 expected_replacement.slice("worker_id", "generation_id")
    refute_equal original_identity.fetch("generation_id"), expected_replacement.fetch("generation_id")
    assert_equal expected_replacement.fetch("endpoint"), observed.dig("environment", "WLO_WORKER_ENDPOINT")
    assert_equal "complete", metadata.fetch("status")
    assert_equal 2, metadata.fetch("attempt")
    assert_equal expected_replacement, metadata.fetch("worker_execution_identity")
    assert_equal "completed", current_execution.fetch("status")
    assert_equal initial_execution.fetch("created_at"), current_execution.fetch("created_at")
    assert_equal plan.id, current_execution.fetch("plan_id")
    assert_equal plan.sha256, current_execution.fetch("plan_sha256")
    assert_equal 1, current_execution.fetch("retry_history").length
    assert_equal 1, current_execution.dig("retry_history", 0, "jobs", 0, "attempt")
    assert_equal 1, current_execution.fetch("dispatch_halt_history").length
    assert_equal 1, Dir[File.join(@tmp, "**", "execution.json")].length
    assert_equal "generation-2 success\n", File.read(File.join(@output, "runs", job.id, "stdout.log"))
    refute Dir.exist?(File.join(@output, "runs", job.id, "provider-attempt-2"))
    refute Dir.exist?(File.join(@output, "attempts", job.id, "attempt-1", "provider-attempt-1"))
    refute metadata.key?("remote_request")
    refute metadata.key?("provider_job_id")
    refute runner.respond_to?(:remote_request, true)
  end

  def read_metadata(job)
    JSON.parse(File.read(File.join(@output, "runs", job.id, "metadata.json")))
  end

  def queue_pop(queue)
    Timeout.timeout(2) { queue.pop }
  rescue Timeout::Error
    flunk "timed out waiting for deterministic DW-30 fixture gate"
  end
end
