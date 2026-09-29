# frozen_string_literal: true

require_relative "test_helper"

class Dw13ConsolidatedExecutionTest < Minitest::Test
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

  def setup
    @tmp = Dir.mktmpdir("wlo-dw13-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_mixed_heterogeneous_state_has_one_store_breaker_and_status_surface
    plan = write_dynamic_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [
        dynamic_job("complete-a", "model-a", group: "adventure-a"),
        dynamic_job("failed-b", "model-b", group: "adventure-b"),
        dynamic_job("running-a", "model-a", group: "adventure-a"),
        dynamic_job("pending-b", "model-b", group: "adventure-b")
      ]
    )
    workers = registry_workers(
      worker_record("worker-a-1", "model-a"),
      worker_record("worker-a-2", "model-a"),
      worker_record("worker-b-1", "model-b")
    )
    store = execution_store(plan)
    store.prepare!
    store.start!

    complete = store.record_dynamic_running!(job: plan.jobs[0], worker: workers[0], environment_keys: [])
    store.record_dynamic_terminal!(attempt: complete, status: "complete", exit_status: 0)
    failed = store.record_dynamic_running!(job: plan.jobs[1], worker: workers[2], environment_keys: [])
    store.record_dynamic_terminal!(attempt: failed, status: "failed", exit_status: 1)
    store.record_dynamic_running!(job: plan.jobs[2], worker: workers[1], environment_keys: [])

    report = status_document(plan)
    assert_equal({ "complete" => 1, "failed" => 1, "running" => 1, "pending" => 1 },
                 report.fetch("counts"))
    assert_equal 4, report.fetch("total")
    assert_equal plan.id, report.fetch("plan_id")
    assert_equal plan.sha256, report.fetch("plan_sha256")
    assert_equal 1, report.dig("circuit_breaker", "total_failures")
    assert_equal %w[complete-a failed-b pending-b running-a],
                 report.fetch("jobs").map { |row| row.fetch("job_id") }.sort
    worker_ids = report.fetch("jobs").filter_map do |row|
      row.dig("worker_execution_identity", "worker_id")
    end
    assert_equal %w[worker-a-1 worker-a-2 worker-b-1], worker_ids.sort

    json = cli_status(plan, "status")
    assert_equal report, JSON.parse(json)
    human = cli_status(plan, "summary")
    assert_includes human, "Progress: [2/4] terminal (50.0%)"
    assert_includes human, "Jobs: complete=1 failed=1 running=1 pending=1"
    assert_equal %w[execution.json jobs.json plan.json],
                 Dir.children(@output).grep(/\.json\z/).sort
  end

  def test_worker_replacement_retry_remains_attempt_history_in_the_same_execution
    plan = write_dynamic_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [dynamic_job("replaced", "model-a"), dynamic_job("unrelated", "model-b")]
    )
    original = worker_record("worker-a", "model-a", generation: "generation-1")
    replacement = worker_record("worker-a", "model-a", generation: "generation-2")
    other = worker_record("worker-b", "model-b")
    poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: SequenceSource.new(
        snapshot(revision: 1, workers: [original, other]),
        snapshot(revision: 2, workers: [replacement, other], published_at: "2030-01-01T00:00:20Z")
      ),
      checkpoint_path: File.join(@output, "dynamic-workers", "checkpoint.json"),
      clock: -> { NOW }
    )
    store = execution_store(plan)
    store.prepare!
    store.start!
    poller.poll_once
    original_worker, other_worker = poller.current_workers
    first_attempt = store.record_dynamic_running!(
      job: plan.jobs[0], worker: original_worker, environment_keys: []
    )
    other_attempt = store.record_dynamic_running!(
      job: plan.jobs[1], worker: other_worker, environment_keys: []
    )
    store.record_dynamic_terminal!(attempt: other_attempt, status: "complete", exit_status: 0)

    poller.poll_once
    reconciler = WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: store)
    assert_equal "worker_generation_replaced", reconciler.reconcile!(poller).first.fetch("reason")
    store.retry_failed!(reason: "replacement generation is ready", all: true)
    store.clear_pause!
    retry_attempt = store.record_dynamic_running!(
      job: plan.jobs[0], worker: poller.current_workers.first, environment_keys: []
    )
    store.record_dynamic_terminal!(attempt: retry_attempt, status: "complete", exit_status: 0)
    assert_equal "completed", store.finish!(resource_cleanup_pending: false)

    archived = read_json("attempts/replaced/attempt-1/metadata.json")
    current = read_json("runs/replaced/metadata.json")
    execution = read_json("execution.json")
    assert_equal 1, first_attempt.attempt_id
    assert_equal 2, retry_attempt.attempt_id
    assert_equal "generation-1", archived.dig("worker_execution_identity", "generation_id")
    assert_equal "dynamic_worker_loss_in_doubt", archived.dig("evidence", "kind")
    assert_equal "generation-2", current.dig("worker_execution_identity", "generation_id")
    assert_equal "complete", current.fetch("status")
    assert_equal [{ "job_id" => "replaced", "attempt" => 1,
                    "archive" => "attempts/replaced/attempt-1" }],
                 execution.fetch("retry_history").first.fetch("jobs")
    assert_equal({ "complete" => 2, "failed" => 0, "running" => 0, "pending" => 0 },
                 status_document(plan).fetch("counts"))
    assert_equal plan.id, execution.fetch("plan_id")
  end

  def test_pause_and_resume_cover_all_worker_classes_in_one_execution
    plan = write_dynamic_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [
        dynamic_job("a-1", "model-a"), dynamic_job("b-1", "model-b"),
        dynamic_job("a-2", "model-a"), dynamic_job("b-2", "model-b")
      ]
    )
    registry = snapshot(
      revision: 1,
      workers: [worker_record("worker-a", "model-a"), worker_record("worker-b", "model-b")]
    )
    mutex = Mutex.new
    condition = ConditionVariable.new
    started = []
    release = false
    first_runner = nil
    executor = lambda do |_environment, *argv, **_options|
      mutex.synchronize do
        started << argv.last
        condition.broadcast
        condition.wait(mutex) until release
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize do
        wait_for(condition, mutex) { started.length == 2 }
        first_runner.store.pause!
        release = true
        condition.broadcast
      end
    end
    first_runner = runner(plan, SequenceSource.new(registry), executor, sleeper)

    assert_equal "paused", first_runner.run
    assert_equal %w[a-1 b-1], started.sort
    assert_equal({ "complete" => 2, "pending" => 2 }, compact_counts(first_runner.store.counts))
    created_at = read_json("execution.json").fetch("created_at")
    assert_equal 2, Dir.children(File.join(@output, "claims")).length

    resumed_calls = []
    resumed = nil
    resumed_executor = lambda do |_environment, *argv, **_options|
      resumed_calls << argv.last
      command_result
    end
    resumed_sleeper = lambda do |*_args|
      Thread.pass
      sleep(0.001) if resumed.store.counts["running"].positive?
    end
    resumed = runner(plan, SequenceSource.new(registry), resumed_executor, resumed_sleeper)

    assert_equal "completed", resumed.run(resume: true)
    assert_equal %w[a-2 b-2], resumed_calls.sort
    assert_equal created_at, read_json("execution.json").fetch("created_at")
    assert_equal({ "complete" => 4, "failed" => 0, "running" => 0, "pending" => 0 },
                 status_document(plan).fetch("counts"))
    assert_equal 4, Dir.children(File.join(@output, "claims")).length
    refute File.exist?(File.join(@output, "control", "pause"))
  end

  private

  def write_dynamic_plan(pools:, jobs:)
    @plan_path = File.join(@tmp, "plan.json")
    File.write(@plan_path, JSON.generate(
                             "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                             "plan_id" => "dw13-consolidated-execution",
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

  def worker_record(worker_id, model, generation: "generation-1")
    worker = {
      "worker_id" => worker_id,
      "generation_id" => generation,
      "endpoint" => "http://127.0.0.1:#{20_000 + Digest::SHA256.hexdigest(worker_id)[0, 4].to_i(16) % 20_000}",
      "state" => "READY",
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
      "registry_id" => "dw13-registry",
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => "2030-01-01T00:05:00Z",
      "workers" => workers
    )
  end

  def registry_workers(*records)
    WorkloadOrchestrator::DynamicWorkerRegistry.new(
      snapshot(revision: 1, workers: records), now: NOW
    ).schedulable_workers
  end

  def execution_store(plan)
    WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: plan, workdir: @workdir)
  end

  def runner(plan, source, executor, sleeper)
    WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_registry_sleeper: sleeper,
      worker_poll_interval: 0.001,
      command_executor: executor
    )
  end

  def status_document(plan)
    WorkloadOrchestrator::ExecutionReport.new(plan: plan, output: @output).document
  end

  def cli_status(_plan, command)
    out = StringIO.new
    err = StringIO.new
    code = WorkloadOrchestrator::CLI.new(
      [command, @plan_path, "--output", @output], out: out, err: err, root: @tmp
    ).run
    assert_equal 0, code, err.string
    out.string
  end

  def read_json(relative)
    JSON.parse(File.read(File.join(@output, relative)))
  end

  def compact_counts(counts)
    counts.reject { |_status, count| count.zero? }
  end

  def wait_for(condition, mutex, timeout: 2.0)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
      raise "timed out waiting for concurrent fixture" unless remaining.positive?

      condition.wait(mutex, remaining)
    end
  end
end
