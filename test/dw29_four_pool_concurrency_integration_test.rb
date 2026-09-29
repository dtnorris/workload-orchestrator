# frozen_string_literal: true

require_relative "test_helper"
require "digest"
require "timeout"

class Dw29FourPoolConcurrencyIntegrationTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:00:05Z")
  PUBLISHED_AT = Time.iso8601("2030-01-01T00:00:00Z")
  GPU_ID = "NVIDIA A40"
  CONTEXT_LENGTH = 131_072
  RPOF_ROOT = File.expand_path(ENV.fetch("RPOF_REPO_ROOT", "../../runpod-ollama-fleet"), __dir__)
  RPOF_REGISTRY_PATH = File.join(RPOF_ROOT, "lib/runpod_ollama_fleet/dynamic_worker_registry.rb")
  TOPOLOGY = {
    "model-a" => [11_441, 11_442, 11_443],
    "model-b" => [11_444],
    "model-c" => [11_445],
    "model-d" => [11_446]
  }.freeze

  require RPOF_REGISTRY_PATH if File.file?(RPOF_REGISTRY_PATH)

  class FakeFleetState
    def initialize(root:, fleet:, identities:)
      @root = root
      @fleet = fleet
      @identities = identities
    end

    def current
      Marshal.load(Marshal.dump(@fleet))
    end

    def artifact_dir(fleet_id, name)
      raise "publisher requested the wrong fleet" unless fleet_id == @fleet.fetch("fleet_id")

      File.join(@root, fleet_id, name)
    end

    def registry_identity(index:, observed_pod_id:)
      worker = @fleet.fetch("workers").find { |candidate| candidate.fetch("index") == index }
      raise "publisher observed the wrong pod" unless worker.fetch("pod_id") == observed_pod_id

      @identities.fetch(index)
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
    attr_reader :document

    def initialize(publisher)
      super()
      @publisher = publisher
    end

    def latest_snapshot
      @bytes ||= begin
        @document = @publisher.snapshot
        "#{JSON.generate(document)}\n"
      end
    end
  end

  def setup
    skip "set RPOF_REPO_ROOT to a runpod-ollama-fleet checkout" unless File.file?(RPOF_REGISTRY_PATH)

    @tmp = Dir.mktmpdir("wlo-dw29-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp) if @tmp && File.exist?(@tmp)
  end

  def test_all_four_pools_make_concurrent_progress_in_one_execution
    plan = execution_plan
    states = TOPOLOGY.map { |pool_id, ports| fleet_state(pool_id, ports) }
    publisher = RunpodOllamaFleet::DynamicWorkerRegistry.new(
      state_root: File.join(@tmp, "rpof-publisher"),
      repo_root: RPOF_ROOT,
      clock: -> { PUBLISHED_AT },
      ttl_seconds: 300,
      process_adapter: FakeProcess.new,
      health_checker: FakeHealth.new,
      fleet_sources: states.map { |pool_id, state| { "fleet_key" => pool_id, "state" => state } },
      id_generator: -> { "rpof-dw29" }
    )
    source = RpofPublishingSource.new(publisher)
    started = Queue.new
    release = Queue.new
    observations = Queue.new
    executor = lambda do |environment, *argv, chdir:|
      job_id = argv.last
      observations << durable_observation(job_id, environment, argv, chdir)
      started << job_id
      release.pop
      command_result(stdout: "completed #{job_id}\n")
    end
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_poll_interval: 0.001,
      command_executor: executor
    )
    run_thread = Thread.new { runner.run }

    begin
      started_jobs = Array.new(6) { Timeout.timeout(5) { started.pop } }
      assert_equal plan.jobs.map(&:id).sort, started_jobs.sort
      assert_running_wave(runner, plan, source.document)
      wave = Array.new(6) { observations.pop }.to_h { |row| [row.fetch("job_id"), row] }
      assert_exact_assignments(wave, plan, source.document)
    ensure
      12.times { release << true }
    end

    assert_equal "completed", Timeout.timeout(5) { run_thread.value }
    assert_equal({ "complete" => 6 }, runner.store.counts)
    assert_equal "completed", execution_report(plan).fetch("status")
    assert_equal 1, Dir[File.join(@tmp, "**", "execution.json")].length
    plan.jobs.each do |job|
      metadata = runner.store.metadata_for(job)
      assert_equal "complete", metadata.fetch("status")
      assert_equal 0, metadata.fetch("exit_status")
      refute metadata.key?("remote_request")
      refute metadata.key?("provider_job_id")
      refute Dir.exist?(File.join(@output, "runs", job.id, "provider-attempt-1"))
    end
    refute runner.respond_to?(:remote_request, true)
  ensure
    if run_thread&.alive?
      12.times { release << true } if release
      run_thread.join(1)
      run_thread.kill if run_thread.alive?
    end
  end

  private

  def fleet_state(pool_id, ports)
    model = "#{pool_id}:latest"
    slug = pool_id.delete("-")
    workers = ports.each_with_index.map do |port, offset|
      index = offset + 1
      {
        "index" => index,
        "name" => "#{pool_id}-worker-#{index}",
        "pod_id" => "#{pool_id}-pod-#{index}",
        "generation" => 1,
        "created_at_utc" => "2029-12-31T23:59:00Z",
        "status" => "active",
        "gpu_id" => GPU_ID,
        "local_ollama_url" => "http://127.0.0.1:#{port}",
        "model" => model
      }
    end
    fleet = {
      "fleet_id" => "dw29-#{pool_id}",
      "status" => "active",
      "created_at_utc" => "2029-12-31T23:59:00Z",
      "gpu" => { "id" => GPU_ID },
      "workers" => workers
    }
    identities = workers.to_h do |worker|
      index = worker.fetch("index")
      [index, {
        "worker_id" => "rpof-#{slug}-worker-#{index}",
        "generation_id" => "rpof-#{slug}-generation-#{index}-1"
      }]
    end
    state = FakeFleetState.new(
      root: File.join(@tmp, "rpof-state"), fleet: fleet, identities: identities
    )
    write_capability_evidence(state, fleet, model)
    [pool_id, state]
  end

  def write_capability_evidence(state, fleet, model)
    run_id = "dw29-bootstrap"
    bootstrap_root = state.artifact_dir(fleet.fetch("fleet_id"), "bootstrap")
    FileUtils.mkdir_p(File.join(bootstrap_root, run_id))
    File.write(File.join(bootstrap_root, "current"), "#{run_id}\n")
    File.write(File.join(bootstrap_root, run_id, "bootstrap.json"), JSON.pretty_generate(
      "schema_version" => 2,
      "bootstrap_run_id" => run_id,
      "fleet_id" => fleet.fetch("fleet_id"),
      "status" => "passed",
      "models" => [model],
      "expected_digests" => { model => model_digest(model) },
      "context" => CONTEXT_LENGTH,
      "workers" => fleet.fetch("workers").map { |worker| bootstrap_worker(worker, model) }
    ) + "\n")
    tunnels_root = state.artifact_dir(fleet.fetch("fleet_id"), "tunnels")
    FileUtils.mkdir_p(tunnels_root)
    File.write(File.join(tunnels_root, "tunnels.json"), JSON.pretty_generate(
      "fleet_id" => fleet.fetch("fleet_id"),
      "workers" => fleet.fetch("workers").map { |worker| tunnel_worker(worker) }
    ) + "\n")
  end

  def bootstrap_worker(worker, model)
    {
      "index" => worker.fetch("index"),
      "pod_id" => worker.fetch("pod_id"),
      "status" => "passed",
      "provenance_error" => nil,
      "provenance" => {
        "gpu" => { "name" => GPU_ID },
        "models" => {
          model => {
            "digest" => model_digest(model),
            "context_length" => CONTEXT_LENGTH,
            "size_bytes" => 20_000,
            "size_vram_bytes" => 20_000,
            "fully_gpu_resident" => true
          }
        }
      }
    }
  end

  def tunnel_worker(worker)
    {
      "index" => worker.fetch("index"),
      "pod_id" => worker.fetch("pod_id"),
      "pid" => 12_000 + worker.fetch("index"),
      "endpoint" => worker.fetch("local_ollama_url"),
      "process_identity" => {
        "forward" => "fixture",
        "ssh_port" => 22_000 + worker.fetch("index"),
        "target" => "root@fixture"
      }
    }
  end

  def execution_plan
    WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "dw29-four-pool-concurrency",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 2 },
      "pools" => TOPOLOGY.keys.map { |pool_id| pool(pool_id) },
      "jobs" => [
        job("a1", "model-a"), job("a2", "model-a"), job("a3", "model-a"),
        job("b1", "model-b"), job("c1", "model-c"), job("d1", "model-d")
      ]
    ))
  end

  def pool(pool_id)
    model = "#{pool_id}:latest"
    {
      "pool_id" => pool_id,
      "required_labels" => ["inference"],
      "requirements" => {
        "ollama" => {
          "model" => model,
          "expected_digest" => model_digest(model),
          "required_context_length" => CONTEXT_LENGTH,
          "require_fully_gpu_resident" => true,
          "required_gpu_id" => GPU_ID
        }
      }
    }
  end

  def job(id, pool_id)
    {
      "job_id" => id,
      "pool_id" => pool_id,
      "depends_on_job_ids" => [],
      "argv" => ["fake-local-scorer", id]
    }
  end

  def model_digest(model)
    Digest::SHA256.hexdigest(model)
  end

  def durable_observation(job_id, environment, argv, chdir)
    {
      "job_id" => job_id,
      "environment" => environment.dup,
      "argv" => argv,
      "chdir" => chdir,
      "metadata" => JSON.parse(File.read(File.join(@output, "runs", job_id, "metadata.json")))
    }
  end

  def assert_running_wave(runner, plan, published)
    assert_equal({ "running" => 6 }, runner.store.counts)
    assert_equal %w[execution.json jobs.json plan.json],
                 Dir.children(@output).grep(/\.json\z/).sort
    assert_equal 1, Dir[File.join(@tmp, "**", "execution.json")].length
    assert_same runner.store, runner.dynamic_scheduler.store
    report = execution_report(plan)
    assert_equal "running", report.fetch("status")
    assert_equal({ "complete" => 0, "failed" => 0, "running" => 6, "pending" => 0 },
                 report.fetch("counts"))
    assert_equal 0.0, report.fetch("progress_percent")
    assert_equal 6, report.fetch("jobs").count { |row| row.fetch("status") == "running" }
    live = WorkloadOrchestrator::LiveExecutionReport.new(plan: plan, output: @output).document(
      workers: runner.worker_registry_poller.ready_workers
    )
    assert_equal 6, live.fetch("busy_workers")
    assert_equal 0, live.fetch("idle_workers")
    assert_equal TOPOLOGY.keys.sort, live.fetch("workers").flat_map { |row| row.fetch("pools") }.uniq.sort
    assert_equal published.fetch("workers").map { |row| row.fetch("worker_id") }.sort,
                 live.fetch("workers").map { |row| row.fetch("worker_id") }.sort
  end

  def assert_exact_assignments(wave, plan, published)
    published_by_id = published.fetch("workers").to_h { |row| [row.fetch("worker_id"), row] }
    expected = {
      "a1" => "rpof-modela-worker-1",
      "a2" => "rpof-modela-worker-2",
      "a3" => "rpof-modela-worker-3",
      "b1" => "rpof-modelb-worker-1",
      "c1" => "rpof-modelc-worker-1",
      "d1" => "rpof-modeld-worker-1"
    }
    assert_equal expected, wave.transform_values { |row| row.dig("metadata", "worker_execution_identity", "worker_id") }
    assert_equal({ "model-a" => 3, "model-b" => 1, "model-c" => 1, "model-d" => 1 },
                 wave.values.map { |row| row.dig("metadata", "pool_id") }.tally)
    wave.each do |job_id, observed|
      job = plan.jobs.find { |candidate| candidate.id == job_id }
      metadata = observed.fetch("metadata")
      identity = metadata.fetch("worker_execution_identity")
      worker = published_by_id.fetch(identity.fetch("worker_id"))
      snapshot = metadata.fetch("worker_snapshot")
      model = "#{job.pool_id}:latest"

      assert_equal 1, metadata.fetch("attempt")
      assert_equal "running", metadata.fetch("status")
      assert_equal @workdir, observed.fetch("chdir")
      assert_equal job.argv, observed.fetch("argv")
      assert_equal model, snapshot.dig("capabilities", "ollama", "models", 0, "model")
      assert_equal model_digest(model), snapshot.dig("capabilities", "ollama", "models", 0, "digest")
      assert_equal worker.fetch("generation_id"), identity.fetch("generation_id")
      assert_equal worker.fetch("endpoint"), identity.fetch("endpoint")
      assert_equal worker.fetch("endpoint"), observed.dig("environment", "AF_OLLAMA_BASE_URL")
      assert_equal 1, metadata.dig("worker_registry_binding", "registry_revision")
      assert metadata.key?("worker_snapshot")
      refute metadata.key?("remote_request")
      refute metadata.key?("provider_job_id")
    end
    assert_equal 3,
                 wave.values.select { |row| row.dig("metadata", "pool_id") == "model-a" }
                     .map { |row| row.dig("metadata", "worker_execution_identity", "worker_id") }.uniq.length
  end

  def execution_report(plan)
    WorkloadOrchestrator::ExecutionReport.new(plan: plan, output: @output).document
  end
end
