# frozen_string_literal: true

require_relative "test_helper"

class ExecutionPoolStatusTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class Source < WorkloadOrchestrator::WorkerSource
    def initialize(bytes)
      super()
      @bytes = bytes
    end

    def latest_snapshot
      @bytes
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-pool-status-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_no_accepted_snapshot
    plan, = prepare_execution

    assert_equal "NO_ACCEPTED_REGISTRY_SNAPSHOT", status(plan).fetch("reason")
  end

  def test_valid_empty_snapshot
    plan, = prepare_execution
    accept_snapshot([])

    assert_equal "NO_COMPATIBLE_READY_WORKERS", status(plan).fetch("reason")
  end

  def test_not_ready_worker_is_distinct
    plan, = prepare_execution
    accept_snapshot([worker("worker-a", "model-a", state: "NOT_READY")])

    row = status(plan)
    assert_equal "WORKERS_NOT_READY", row.fetch("reason")
    assert_equal 1, row.dig("workers", "not_ready")
    assert_equal 0, row.dig("workers", "compatible_ready")
  end

  def test_ready_incompatible_worker_is_distinct
    plan, = prepare_execution
    accept_snapshot([worker("worker-b", "model-b")])

    row = status(plan)
    assert_equal "READY_WORKERS_INCOMPATIBLE", row.fetch("reason")
    assert_equal 1, row.dig("workers", "ready_incompatible")
  end

  def test_compatible_idle_worker_is_ready_to_dispatch
    plan, = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])

    row = status(plan)
    assert_equal "READY_TO_DISPATCH", row.fetch("reason")
    assert_equal({
                   "compatible_ready" => 1, "busy" => 0, "idle" => 1,
                   "ready_incompatible" => 0, "not_ready" => 0, "unavailable" => 0
                 }, row.fetch("workers"))
  end

  def test_compatible_worker_busy_in_another_pool
    pools = [pool("pool-a", "model-a"), pool("pool-b", "model-a")]
    jobs = [job("running-a", "pool-a"), job("pending-b", "pool-b")]
    plan, store = prepare_execution(pools:, jobs:)
    poller = accept_snapshot([worker("worker-a", "model-a")])
    store.record_dynamic_running!(job: plan.jobs.first, worker: poller.current_workers.first,
                                  environment_keys: [])

    rows = report(plan).fetch("pool_status").to_h { |row| [row.fetch("pool_id"), row] }
    assert_equal "RUNNING", rows.fetch("pool-a").fetch("reason")
    assert_equal "ALL_COMPATIBLE_WORKERS_BUSY", rows.fetch("pool-b").fetch("reason")
    assert_equal 1, rows.fetch("pool-b").dig("workers", "busy")
    assert_equal 0, rows.fetch("pool-b").dig("workers", "idle")
  end

  def test_pause_precedes_idle_capacity
    plan, store = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])
    store.pause!

    assert_equal "PAUSED", status(plan).fetch("reason")
  end

  def test_breaker_precedes_idle_capacity
    plan, = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])
    update_execution do |state|
      state.fetch("circuit_breaker").merge!("tripped" => true, "reason" => "failure limit reached")
    end

    assert_equal "CIRCUIT_BREAKER", status(plan).fetch("reason")
  end

  def test_dispatch_halt_precedes_idle_capacity
    plan, store = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])
    store.record_dispatch_halt!(kind: "dynamic_worker_loss", error: "worker disappeared")

    assert_equal "DISPATCH_HALTED", status(plan).fetch("reason")
  end

  def test_registry_failure_is_distinct_from_generic_dispatch_halt
    plan, store = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])
    store.record_dispatch_halt!(kind: "worker_registry", error: "snapshot expired")

    row = status(plan)
    assert_equal "REGISTRY_INVALID_OR_STALE", row.fetch("reason")
  end

  def test_expired_checkpoint_fails_closed_without_a_halt_record
    plan, = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")], expires_at: "2030-01-01T00:00:30Z")

    row = status(plan)
    assert_equal "REGISTRY_INVALID_OR_STALE", row.fetch("reason")
    assert_includes row.fetch("detail"), "expired"
  end

  def test_generation_replacement_is_idle_not_busy_for_old_attempt
    plan, store = prepare_execution(jobs: [job("running", "pool-a"), job("pending", "pool-a")])
    original = accept_snapshot([worker("worker-a", "model-a", generation: "generation-1")])
    store.record_dynamic_running!(job: plan.jobs.first, worker: original.current_workers.first,
                                  environment_keys: [])
    accept_snapshot(
      [worker("worker-a", "model-a", generation: "generation-2")],
      revision: 2, published_at: "2030-01-01T00:00:20Z"
    )

    row = status(plan)
    assert_equal "RUNNING", row.fetch("reason")
    assert_equal 0, row.dig("workers", "busy")
    assert_equal 1, row.dig("workers", "idle")
  end

  def test_no_runnable_work_is_not_reported_as_capacity_starvation
    pools = [pool("pool-a", "model-a"), pool("pool-b", "model-b")]
    jobs = [job("first", "pool-a"), job("dependent", "pool-b", depends_on: ["first"])]
    plan, = prepare_execution(pools:, jobs:)
    accept_snapshot([worker("worker-b", "model-b")])

    row = report(plan).fetch("pool_status").find { |candidate| candidate.fetch("pool_id") == "pool-b" }
    assert_equal "NO_RUNNABLE_WORK", row.fetch("reason")
  end

  def test_completed_pool_stays_complete_with_live_capacity
    plan, store = prepare_execution
    poller = accept_snapshot([worker("worker-a", "model-a")])
    attempt = store.record_dynamic_running!(job: plan.jobs.first, worker: poller.current_workers.first,
                                            environment_keys: [])
    store.record_dynamic_terminal!(attempt:, status: "complete", exit_status: 0)
    store.finish!

    assert_equal "COMPLETE", status(plan).fetch("reason")
  end

  def test_human_report_is_compact_and_status_is_read_only
    plan, = prepare_execution
    accept_snapshot([worker("worker-a", "model-a")])
    evidence = evidence_digests
    out = StringIO.new

    WorkloadOrchestrator::ExecutionReport.new(
      plan:, output: @output, clock: -> { NOW }
    ).print(out)

    assert_equal evidence, evidence_digests
    assert_includes out.string, "Pool"
    assert_includes out.string, "Jobs C/R/F/P"
    assert_match(%r{pool-a\s+0/0/0/1\s+1\s+0\s+1\s+READY_TO_DISPATCH}, out.string)
  end

  private

  def prepare_execution(pools: [pool("pool-a", "model-a")], jobs: [job("pending", "pool-a")])
    path = File.join(@tmp, "plan.json")
    File.write(path, JSON.generate(
                       "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                       "plan_id" => "pool-status-fixture",
                       "failure_policy" => {
                         "max_consecutive_failures" => 10,
                         "max_total_failures" => 10
                       },
                       "pools" => pools,
                       "jobs" => jobs
                     ))
    plan = WorkloadOrchestrator::Plan.load(path)
    store = WorkloadOrchestrator::ExecutionStore.new(
      output_dir: @output, plan:, workdir: @workdir
    )
    store.prepare!
    store.start!
    [plan, store]
  end

  def pool(id, model)
    {
      "pool_id" => id,
      "required_labels" => ["inference"],
      "requirements" => {
        "ollama" => {
          "model" => model,
          "expected_digest" => Digest::SHA256.hexdigest(model),
          "required_context_length" => 131_072,
          "require_fully_gpu_resident" => true,
          "required_gpu_id" => "NVIDIA A40"
        }
      }
    }
  end

  def job(id, pool_id, depends_on: [])
    {
      "job_id" => id,
      "pool_id" => pool_id,
      "depends_on_job_ids" => depends_on,
      "argv" => ["fixture-command", id]
    }
  end

  def worker(worker_id, model, state: "READY", generation: "generation-1")
    row = {
      "worker_id" => worker_id,
      "generation_id" => generation,
      "endpoint" => "http://127.0.0.1:#{20_000 + (Digest::SHA256.hexdigest(worker_id)[0, 4].to_i(16) % 20_000)}",
      "state" => state,
      "labels" => ["inference"],
      "capabilities" => {
        "gpu_id" => "NVIDIA A40",
        "ollama" => {
          "models" => [{
            "model" => model,
            "digest" => Digest::SHA256.hexdigest(model),
            "context_length" => 131_072,
            "fully_gpu_resident" => true
          }]
        }
      }
    }
    row["capability_fingerprint"] = capability_fingerprint(row)
    row
  end

  def capability_fingerprint(worker)
    capabilities = worker.fetch("capabilities")
    models = capabilities.dig("ollama", "models").map do |model|
      model.slice("context_length", "digest", "fully_gpu_resident", "model")
    end
    Digest::SHA256.hexdigest(JSON.generate(
                               "gpu_id" => capabilities.fetch("gpu_id"),
                               "labels" => worker.fetch("labels"),
                               "ollama_models" => models
                             ))
  end

  def accept_snapshot(workers, revision: 1, published_at: "2030-01-01T00:00:00Z",
                      expires_at: "2030-01-01T00:05:00Z")
    source = Source.new(JSON.generate(
                          "contract_version" => WorkloadOrchestrator::DynamicWorkerRegistry::CONTRACT_VERSION,
                          "registry_id" => "pool-status-registry",
                          "revision" => revision,
                          "published_at" => published_at,
                          "expires_at" => expires_at,
                          "workers" => workers
                        ))
    poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source:,
      checkpoint_path: File.join(@output, "dynamic-workers", "checkpoint.json"),
      clock: -> { Time.iso8601(published_at) }
    )
    poller.poll_once
  end

  def report(plan)
    WorkloadOrchestrator::ExecutionReport.new(
      plan:, output: @output, clock: -> { NOW }
    ).document
  end

  def status(plan)
    report(plan).fetch("pool_status").first
  end

  def update_execution
    path = File.join(@output, "execution.json")
    state = JSON.parse(File.read(path))
    yield state
    File.write(path, JSON.generate(state))
  end

  def evidence_digests
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path, Digest::SHA256.file(path).hexdigest] }
  end
end
