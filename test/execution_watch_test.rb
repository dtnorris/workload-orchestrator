# frozen_string_literal: true

require_relative "test_helper"

class ExecutionWatchTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    def initialize(*snapshots)
      super()
      @snapshots = snapshots
      @calls = 0
    end

    def latest_snapshot
      snapshot = @snapshots.fetch([@calls, @snapshots.length - 1].min)
      @calls += 1
      snapshot
    end
  end

  class TtyOutput < StringIO
    def tty?
      true
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-watch-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_consolidated_snapshot_and_refresh_are_read_only
    plan = write_dynamic_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [
        dynamic_job("complete-a", "model-a", group: "adventure-a"),
        dynamic_job("failed-b", "model-b", group: "adventure-b"),
        dynamic_job("running-a", "model-a", group: "adventure-a"),
        dynamic_job("pending-b", "model-b", group: "adventure-b")
      ]
    )
    records = [
      worker_record("worker-a", "model-a"),
      worker_record("worker-b", "model-b"),
      worker_record("worker-c", "model-a", state: "NOT_READY")
    ]
    poller = poller_for(snapshot(revision: 1, workers: records))
    store = prepared_store(plan)
    poller.poll_once
    complete = store.record_dynamic_running!(job: plan.jobs[0], worker: poller.current_workers[0],
                                              environment_keys: [])
    store.record_dynamic_terminal!(attempt: complete, status: "complete", exit_status: 0)
    failed = store.record_dynamic_running!(job: plan.jobs[1], worker: poller.current_workers[1],
                                            environment_keys: [])
    store.record_dynamic_terminal!(attempt: failed, status: "failed", exit_status: 1)
    store.record_dynamic_running!(job: plan.jobs[2], worker: poller.current_workers[0], environment_keys: [])
    paths = evidence_paths("runs/running-a/metadata.json")
    before = digests(paths)
    out = StringIO.new
    watcher = watch(plan, out: out, sleeper: ->(*) { raise Interrupt })

    assert_equal 0, watcher.run
    assert_equal before, digests(paths)
    document = watcher.snapshot
    assert_equal({ "complete" => 1, "failed" => 1, "running" => 1, "pending" => 1 },
                 document.fetch("counts"))
    assert_equal "running-a", document.fetch("running_attempts").first.fetch("job_id")
    assert_equal "adventure-a", document.fetch("running_attempts").first.fetch("group_id")
    assert_equal "model-a", document.fetch("running_attempts").first.fetch("pool_id")
    assert_equal ["model-a"], document.fetch("running_attempts").first.fetch("models")
    assert_equal({ "READY" => 2, "NOT_READY" => 1, "UNAVAILABLE" => 0,
                   "busy" => 1, "idle_ready" => 1 }, document.dig("worker_registry", "counts"))
    assert_equal %w[busy idle unavailable],
                 document.dig("worker_registry", "workers").map { |row| row.fetch("availability") }
    assert_includes out.string, "Progress: 1 complete / 1 running / 1 pending / 1 failed"
    assert_includes out.string, "worker=worker-a"
    assert_includes out.string, "models=model-a"
    refute_includes out.string, "\e["
  end

  def test_generation_replacement_is_not_the_running_attempt_worker
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("running", "model-a")])
    original = worker_record("worker-a", "model-a", generation: "generation-1")
    replacement = worker_record("worker-a", "model-a", generation: "generation-2")
    poller = poller_for(
      snapshot(revision: 1, workers: [original]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z", workers: [replacement])
    )
    store = prepared_store(plan)
    poller.poll_once
    store.record_dynamic_running!(job: plan.jobs.first, worker: poller.current_workers.first,
                                  environment_keys: [])
    poller.poll_once

    document = watch(plan).snapshot
    attempt = document.fetch("running_attempts").first
    current = document.dig("worker_registry", "workers", 0)
    assert_equal "generation-1", attempt.fetch("generation_id")
    refute attempt.fetch("current_worker")
    assert_equal "generation-2", current.fetch("generation_id")
    assert_equal "idle", current.fetch("availability")
    assert_equal 0, document.dig("worker_registry", "counts", "busy")
  end

  def test_waiting_requires_compatible_capacity_and_reflects_checkpoint_update
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    incompatible = worker_record("worker-b", "model-b")
    compatible = worker_record("worker-a", "model-a")
    poller = poller_for(
      snapshot(revision: 1, workers: [incompatible]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z",
               workers: [incompatible, compatible])
    )
    prepared_store(plan)
    poller.poll_once

    waiting = watch(plan).snapshot
    assert waiting.fetch("waiting_for_capacity")
    assert_equal "Waiting: ready workers incompatible", waiting.fetch("display_state")

    poller.poll_once
    available = watch(plan).snapshot
    refute available.fetch("waiting_for_capacity")
    assert_equal "capacity_available", available.fetch("capacity_evaluation")
    assert_equal "Ready to dispatch", available.fetch("display_state")
  end

  def test_worker_loss_halt_is_distinct_from_waiting
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("lost", "model-a")])
    worker = worker_record("worker-a", "model-a")
    poller = poller_for(
      snapshot(revision: 1, workers: [worker]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    store = prepared_store(plan)
    poller.poll_once
    store.record_dynamic_running!(job: plan.jobs.first, worker: poller.current_workers.first,
                                  environment_keys: [])
    poller.poll_once
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: store).reconcile!(poller)

    document = watch(plan).snapshot
    assert_includes document.fetch("display_state"), "Dispatch halted (dynamic_worker_loss)"
    refute document.fetch("waiting_for_capacity")
    assert_equal "failed", document.fetch("jobs").first.fetch("status")
  end

  def test_pause_takes_precedence_without_mutation
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    store = prepared_store(plan)
    store.pause!
    before = digests(evidence_paths)

    document = watch(plan).snapshot

    assert_equal "Paused", document.fetch("display_state")
    assert_equal before, digests(evidence_paths)
  end

  def test_completed_execution_renders_once_and_cli_exits
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("complete", "model-a")])
    poller = poller_for(snapshot(revision: 1, workers: [worker_record("worker-a", "model-a")]))
    store = prepared_store(plan)
    poller.poll_once
    attempt = store.record_dynamic_running!(job: plan.jobs.first, worker: poller.current_workers.first,
                                            environment_keys: [])
    store.record_dynamic_terminal!(attempt: attempt, status: "complete", exit_status: 0)
    store.finish!(resource_cleanup_pending: false)
    sleeps = 0
    out = StringIO.new

    assert_equal 0, watch(plan, out: out, sleeper: ->(*) { sleeps += 1 }).run
    assert_equal 0, sleeps
    assert_includes out.string, "State: Completed"
    code, cli_out, err = run_cli("watch", @plan_path, "--output", @output, "--interval", "0.25")
    assert_equal 0, code, err
    assert_includes cli_out, "State: Completed"
  end

  def test_ctrl_c_stops_only_the_watcher
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    prepared_store(plan)
    before = read_json("execution.json")

    assert_equal 0, watch(plan, sleeper: ->(*) { raise Interrupt }).run
    assert_equal before, read_json("execution.json")
    refute read_json("execution.json").key?("interruption")
  end

  def test_tty_refresh_uses_cursor_control_only_after_first_snapshot
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    prepared_store(plan)
    out = TtyOutput.new
    refreshes = 0
    sleeper = lambda do |*_args|
      refreshes += 1
      raise Interrupt if refreshes == 2
    end

    assert_equal 0, watch(plan, out: out, sleeper: sleeper).run
    assert_equal 1, out.string.scan("\e[2J\e[H").length
    refute_includes out.string, "---"
  end

  def test_missing_checkpoint_is_reported_without_fabricated_workers
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    prepared_store(plan)

    document = watch(plan).snapshot

    refute document.dig("worker_registry", "available")
    assert_empty document.dig("worker_registry", "workers")
    assert_equal "Waiting: no accepted registry snapshot", document.fetch("display_state")
  end

  def test_refresh_skips_a_transient_cross_file_state
    plan = write_dynamic_plan(pools: [pool("model-a")], jobs: [dynamic_job("pending", "model-a")])
    prepared_store(plan)
    path = File.join(@output, "jobs.json")
    stable = File.read(path)
    inconsistent = JSON.parse(stable)
    inconsistent.fetch("jobs").first.merge!("status" => "running", "attempt" => 1)
    File.write(path, JSON.generate(inconsistent))
    sleeps = 0
    sleeper = lambda do |*_args|
      sleeps += 1
      if sleeps == 1
        File.write(path, stable)
      else
        raise Interrupt
      end
    end
    out = StringIO.new

    assert_equal 0, watch(plan, out: out, sleeper: sleeper).run
    assert_equal 2, sleeps
    assert_includes out.string, "Batch: watch-fixture"
    assert_includes out.string, "Progress: 0 complete / 0 running / 1 pending / 0 failed"
  end

  private

  def write_dynamic_plan(pools:, jobs:)
    @plan_path = File.join(@tmp, "plan.json")
    File.write(@plan_path, JSON.generate(
                             "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                             "plan_id" => "watch-fixture",
                             "failure_policy" => {
                               "max_consecutive_failures" => 10,
                               "max_total_failures" => 10
                             },
                             "pools" => pools,
                             "jobs" => jobs
                           ))
    WorkloadOrchestrator::Plan.load(@plan_path)
  end

  def pool(model)
    {
      "pool_id" => model,
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

  def dynamic_job(id, pool_id, group: nil)
    row = {
      "job_id" => id,
      "pool_id" => pool_id,
      "depends_on_job_ids" => [],
      "argv" => ["fixture-command", id]
    }
    row["group_id"] = group if group
    row
  end

  def worker_record(worker_id, model, generation: "generation-1", state: "READY")
    worker = {
      "worker_id" => worker_id,
      "generation_id" => generation,
      "endpoint" => "http://127.0.0.1:#{20_000 + Digest::SHA256.hexdigest(worker_id)[0, 4].to_i(16) % 20_000}",
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
    worker["capability_fingerprint"] = capability_fingerprint(worker)
    worker
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

  def snapshot(revision:, workers:, published_at: "2030-01-01T00:00:00Z")
    JSON.generate(
      "contract_version" => WorkloadOrchestrator::DynamicWorkerRegistry::CONTRACT_VERSION,
      "registry_id" => "watch-registry",
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => "2030-01-01T00:05:00Z",
      "workers" => workers
    )
  end

  def poller_for(*snapshots)
    WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: SequenceSource.new(*snapshots),
      checkpoint_path: File.join(@output, "dynamic-workers", "checkpoint.json"),
      clock: -> { NOW }
    )
  end

  def prepared_store(plan)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: plan, workdir: @workdir)
    store.prepare!
    store.start!
    store
  end

  def watch(plan, out: StringIO.new, sleeper: ->(*) { raise "unexpected sleep" })
    WorkloadOrchestrator::ExecutionWatch.new(
      plan: plan, output: @output, out: out, sleeper: sleeper, clock: -> { NOW }
    )
  end

  def evidence_paths(*extra)
    %w[execution.json jobs.json dynamic-workers/checkpoint.json].map { |name| File.join(@output, name) } +
      extra.map { |name| File.join(@output, name) }
  end

  def digests(paths)
    paths.to_h { |path| [path, File.file?(path) ? Digest::SHA256.file(path).hexdigest : nil] }
  end

  def read_json(relative)
    JSON.parse(File.read(File.join(@output, relative)))
  end

  def run_cli(*argv)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(argv, out: out, err: err).run
    [code, out.string, err.string]
  end
end
