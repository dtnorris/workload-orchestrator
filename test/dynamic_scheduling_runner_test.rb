# frozen_string_literal: true

require_relative "test_helper"

class DynamicSchedulingRunnerTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(*snapshots)
      super()
      @snapshots = snapshots
      @calls = 0
    end

    def latest_snapshot
      snapshot = @snapshots.fetch([calls, @snapshots.length - 1].min)
      @calls += 1
      snapshot
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-dynamic-scheduling-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_six_workers_across_four_model_profiles_run_concurrently_with_durable_bindings
    plan = build_plan(
      pools: [pool("qwen35"), pool("qwen27"), pool("gemma"), pool("gptoss")],
      jobs: [
        job("qwen35-1", "qwen35"), job("qwen35-2", "qwen35"), job("qwen35-3", "qwen35"),
        job("qwen27-1", "qwen27"), job("gemma-1", "gemma"), job("gptoss-1", "gptoss")
      ]
    )
    records = [
      worker_record("qwen35-a", "qwen35"), worker_record("qwen35-b", "qwen35"),
      worker_record("qwen35-c", "qwen35"), worker_record("qwen27-a", "qwen27"),
      worker_record("gemma-a", "gemma"), worker_record("gptoss-a", "gptoss")
    ]
    source = SequenceSource.new(snapshot(revision: 1, workers: records))
    mutex = Mutex.new
    condition = ConditionVariable.new
    active = 0
    started = 0
    maximum_active = 0
    completed = 0
    claims_seen = {}
    claim_states_seen = {}
    executor = lambda do |_environment, *argv, chdir:|
      raise "unexpected workdir" unless chdir == @workdir

      job_id = argv.last
      metadata = metadata_for(job_id)
      claim_path = File.join(@output, "claims", "#{Digest::SHA256.hexdigest(job_id)}.lock")
      mutex.synchronize do
        claims_seen[job_id] = metadata.fetch("worker_execution_identity")
        claim_states_seen[job_id] = JSON.parse(File.read(claim_path)).fetch("state")
        active += 1
        started += 1
        maximum_active = [maximum_active, active].max
        condition.broadcast if started == 6
        wait_for(condition, mutex) { started == 6 }
        active -= 1
        completed += 1
        condition.broadcast
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize { wait_for(condition, mutex) { completed == 6 } }
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "completed", runner.run
    assert_equal 6, maximum_active
    assert_equal plan.jobs.map(&:id).sort, claims_seen.keys.sort
    assert(claims_seen.values.all? { |identity| identity.is_a?(Hash) && identity.length == 5 })
    assert_equal ["claimed"], claim_states_seen.values.uniq
    job_rows = JSON.parse(File.read(File.join(@output, "jobs.json"))).fetch("jobs")
    assert_equal 6, job_rows.length
    assert(job_rows.all? { |row| row.fetch("worker_execution_identity").length == 5 })
    assert_assignments_match_pools(plan, records)
  end

  def test_zero_worker_startup_assigns_when_worker_appears_later
    plan = build_plan(pools: [pool("qwen")], jobs: [job("later", "qwen")])
    source = SequenceSource.new(
      snapshot(revision: 1, workers: []),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z",
               workers: [worker_record("qwen-worker", "qwen")])
    )
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result
    end
    sleeper = lambda do |*_args|
      Thread.pass
      metadata = runner&.then { |current| current.store.metadata_for(plan.jobs.first) }
      sleep(0.001) if metadata&.fetch("status", nil) == "running"
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "completed", runner.run
    assert_operator source.calls, :>=, 2
    assert_equal ["later"], calls
  end

  def test_incompatible_ready_worker_keeps_execution_waiting_until_paused
    plan = build_plan(pools: [pool("qwen35")], jobs: [job("qwen-job", "qwen35")])
    source = SequenceSource.new(snapshot(
                                  revision: 1,
                                  workers: [worker_record("gemma-worker", "gemma")]
                                ))
    waits = []
    observed_statuses = []
    runner = nil
    sleeper = lambda do |seconds, _stop|
      waits << seconds
      observed_statuses << runner.store.status
      runner.store.pause! if waits.length == 2
    end
    executor = ->(*) { raise "incompatible work must not execute" }
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "paused", runner.run
    assert_equal 2, source.calls
    assert_equal [0.001, 0.001], waits
    assert_equal ["running", "running"], observed_statuses
    assert_equal({ "pending" => 1 }, runner.store.counts)
    assert_nil runner.store.metadata_for(plan.jobs.first)
  end

  def test_partial_capacity_finishes_eligible_work_and_waits_for_later_worker
    plan = build_plan(
      pools: [pool("qwen"), pool("gemma")],
      jobs: [job("qwen-job", "qwen"), job("gemma-job", "gemma")]
    )
    qwen = worker_record("qwen-worker", "qwen")
    gemma = worker_record("gemma-worker", "gemma")
    source = SequenceSource.new(
      snapshot(revision: 1, workers: [qwen]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z", workers: [qwen]),
      snapshot(revision: 3, published_at: "2030-01-01T00:00:40Z", workers: [qwen, gemma])
    )
    calls = []
    runner = nil
    observations = []
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result
    end
    sleeper = lambda do |*_args|
      Thread.pass
      sleep(0.001) while runner.store.counts["running"].positive?
      observations << runner.store.counts
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "completed", runner.run
    assert_equal 3, source.calls
    assert_equal %w[qwen-job gemma-job], calls
    assert_equal({ "complete" => 1, "pending" => 1 }, observations.first)
    assert_includes observations, { "complete" => 2 }
  end

  def test_resume_reuses_execution_and_checkpoint_then_schedules_later_capacity
    plan = build_plan(pools: [pool("qwen")], jobs: [job("later", "qwen")])
    first = nil
    first = build_runner(
      plan,
      SequenceSource.new(snapshot(revision: 1, workers: [])),
      executor: ->(*) { raise "work must not execute before capacity exists" },
      sleeper: ->(*) { first.store.pause! }
    )
    assert_equal "paused", first.run
    original = JSON.parse(File.read(File.join(@output, "execution.json")))

    calls = []
    resumed = nil
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result
    end
    sleeper = lambda do |*_args|
      Thread.pass
      sleep(0.001) if resumed.store.counts["running"].positive?
    end
    resumed = build_runner(
      plan,
      SequenceSource.new(snapshot(
                           revision: 2,
                           published_at: "2030-01-01T00:00:20Z",
                           workers: [worker_record("qwen-worker", "qwen")]
                         )),
      executor: executor,
      sleeper: sleeper
    )

    assert_equal "completed", resumed.run(resume: true)
    current = JSON.parse(File.read(File.join(@output, "execution.json")))
    assert_equal ["later"], calls
    assert_equal original.fetch("created_at"), current.fetch("created_at")
    assert_equal original.fetch("plan_sha256"), current.fetch("plan_sha256")
    assert_equal 1, resumed.store.metadata_for(plan.jobs.first).fetch("attempt")
    assert_equal 2, JSON.parse(File.read(File.join(@output, "dynamic-workers/checkpoint.json"))).fetch("revision")
  end

  def test_attempt_completion_wakes_default_poll_sleep_before_the_interval
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("first", "model"), job("second", "model")]
    )
    source = SequenceSource.new(snapshot(
                                  revision: 1,
                                  workers: [worker_record("worker", "model")]
                                ))
    calls = []
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result
    end
    runner = build_runner(
      plan, source, executor: executor, sleeper: nil, poll_interval: 2.0
    )
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    assert_equal "completed", runner.run

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    assert_equal %w[first second], calls
    assert_operator elapsed, :<, 1.0
  end

  def test_activity_completed_before_idle_wait_skips_the_interval
    plan = build_plan(pools: [pool("model")], jobs: [job("job", "model")])
    runner = build_runner(
      plan,
      SequenceSource.new(snapshot(revision: 1, workers: [])),
      executor: ->(*) { command_result },
      sleeper: nil,
      poll_interval: 60.0
    )
    runner.send(:signal_dynamic_activity)
    stop_calls = 0
    started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    runner.send(:dynamic_poll_sleep, 60.0, -> { stop_calls += 1; false })

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    assert_equal 0, stop_calls
    assert_operator elapsed, :<, 0.1
  end

  def test_dynamic_executor_error_is_terminal_and_wakes_polling
    plan = build_plan(pools: [pool("model")], jobs: [job("job", "model")])
    source = SequenceSource.new(snapshot(
                                  revision: 1,
                                  workers: [worker_record("worker", "model")]
                                ))
    runner = nil
    sleeper = lambda do |*_args|
      Thread.pass
      sleep(0.001) if runner.store.counts["running"].positive?
    end
    runner = build_runner(
      plan, source,
      executor: ->(*) { raise IOError, "simulated executor failure" },
      sleeper: sleeper
    )

    assert_equal "workload_failed", runner.run
    metadata = runner.store.metadata_for(plan.jobs.first)
    assert_equal "failed", metadata.fetch("status")
    assert_equal "simulated executor failure", metadata.fetch("error")
    assert_includes File.read(File.join(@output, "runs/job/stderr.log")), "IOError"
  end

  def test_new_worker_receives_work_while_existing_worker_remains_busy
    plan = build_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [job("job-a", "model-a"), job("job-b", "model-b")]
    )
    worker_a = worker_record("worker-a", "model-a")
    worker_b = worker_record("worker-b", "model-b")
    source = SequenceSource.new(
      snapshot(revision: 1, workers: [worker_a]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z", workers: [worker_a, worker_b])
    )
    mutex = Mutex.new
    condition = ConditionVariable.new
    active = []
    started = []
    maximum_active = 0
    executor = lambda do |_environment, *argv, **_options|
      job_id = argv.last
      mutex.synchronize do
        active << job_id
        started << job_id
        maximum_active = [maximum_active, active.length].max
        condition.broadcast
        wait_for(condition, mutex) { started.length == 2 }
        active.delete(job_id)
        condition.broadcast
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize do
        condition.wait(mutex, 0.01) unless active.length == 2
      end
    end

    assert_equal "completed", build_runner(plan, source, executor: executor, sleeper: sleeper).run
    assert_equal 2, maximum_active
    assert_operator source.calls, :>=, 2
  end

  def test_priority_groups_overlap_and_dependencies_unlock_on_later_decisions
    plan = build_plan(
      pools: [pool("model-a"), pool("model-b")],
      jobs: [
        job("group-a-1", "model-a", group: "group-a"),
        job("group-b-1", "model-b", group: "group-b"),
        job("group-a-2", "model-a", group: "group-a", depends_on: ["group-a-1"])
      ]
    )
    source = SequenceSource.new(snapshot(
                                  revision: 1,
                                  workers: [worker_record("worker-a", "model-a"),
                                            worker_record("worker-b", "model-b")]
                                ))
    mutex = Mutex.new
    condition = ConditionVariable.new
    active = []
    started = []
    first_wave = []
    completed = []
    executor = lambda do |_environment, *argv, **_options|
      job_id = argv.last
      mutex.synchronize do
        active << job_id
        started << job_id
        first_wave << job_id if completed.empty?
        condition.broadcast
        wait_for(condition, mutex) { started.length >= 2 } if %w[group-a-1 group-b-1].include?(job_id)
        active.delete(job_id)
        completed << job_id
        condition.broadcast
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize { condition.wait(mutex, 0.01) }
    end

    assert_equal "completed", build_runner(plan, source, executor: executor, sleeper: sleeper).run
    assert_equal %w[group-a-1 group-b-1], first_wave.uniq.sort
    assert_operator completed.index("group-a-2"), :>, completed.index("group-a-1")
  end

  def test_pause_stops_new_assignments_and_drains_the_claimed_attempt
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("first", "model"), job("second", "model")]
    )
    source = SequenceSource.new(snapshot(revision: 1, workers: [worker_record("worker", "model")]))
    mutex = Mutex.new
    condition = ConditionVariable.new
    started = false
    release = false
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      mutex.synchronize do
        calls << argv.last
        started = true
        condition.broadcast
        wait_for(condition, mutex) { release }
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize do
        wait_for(condition, mutex) { started }
        runner.store.pause!
        release = true
        condition.broadcast
      end
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "paused", runner.run
    assert_equal ["first"], calls
    assert_nil runner.store.metadata_for(plan.jobs.last)
  end

  def test_breaker_stops_new_assignments
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("first", "model"), job("second", "model")],
      max_consecutive_failures: 1,
      max_total_failures: 1
    )
    source = SequenceSource.new(snapshot(revision: 1, workers: [worker_record("worker", "model")]))
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result(exit_status: 1)
    end
    sleeper = lambda do |*_args|
      sleep(0.001) while runner.store.metadata_for(plan.jobs.first)&.fetch("status") == "running"
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "circuit_broken", runner.run
    assert_equal ["first"], calls
    assert runner.store.circuit_tripped?
  end

  def test_interruption_stops_new_assignments_and_preserves_the_claim
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("first", "model"), job("second", "model")]
    )
    source = SequenceSource.new(snapshot(revision: 1, workers: [worker_record("worker", "model")]))
    mutex = Mutex.new
    condition = ConditionVariable.new
    started = false
    release = false
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      mutex.synchronize do
        calls << argv.last
        started = true
        condition.broadcast
        wait_for(condition, mutex) { release }
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize do
        wait_for(condition, mutex) { started }
        runner.instance_variable_set(:@interrupt_signal, "TERM")
        release = true
        condition.broadcast
      end
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "interrupted", runner.run
    assert_equal ["first"], calls
    assert_nil runner.store.metadata_for(plan.jobs.last)
    assert_equal "TERM", JSON.parse(File.read(File.join(@output, "execution.json"))).dig("interruption", "signal")
  end

  def test_worker_loss_halts_dispatch_and_late_completion_cannot_overwrite_in_doubt_state
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("first", "model"), job("second", "model")]
    )
    worker = worker_record("worker", "model")
    source = SequenceSource.new(
      snapshot(revision: 1, workers: [worker]),
      snapshot(revision: 2, published_at: "2030-01-01T00:00:20Z", workers: [])
    )
    mutex = Mutex.new
    condition = ConditionVariable.new
    started = false
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      mutex.synchronize do
        calls << argv.last
        started = true
        condition.broadcast
      end
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      until runner.store.dispatch_halted?
        raise "timed out waiting for worker-loss halt" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        Thread.pass
      end
      command_result
    end
    sleeper = lambda do |*_args|
      mutex.synchronize { wait_for(condition, mutex) { started } }
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)

    assert_equal "infrastructure_failed", runner.run
    assert_equal ["first"], calls
    metadata = runner.store.metadata_for(plan.jobs.first)
    assert_equal "failed", metadata.fetch("status")
    assert_equal "worker_disappeared", metadata.dig("evidence", "reason")
    assert_equal "complete", metadata.dig("late_evidence", 0, "status")
    assert_nil runner.store.metadata_for(plan.jobs.last)
  end

  def test_resume_preserves_running_binding_without_redispatch
    plan = build_plan(
      pools: [pool("model")],
      jobs: [job("running", "model"), job("pending", "model")]
    )
    record = worker_record("worker", "model")
    bytes = snapshot(revision: 1, workers: [record])
    source = SequenceSource.new(bytes)
    worker = WorkloadOrchestrator::DynamicWorkerRegistry.new(bytes, now: NOW).schedulable_workers.first
    calls = []
    runner = nil
    executor = lambda do |_environment, *argv, **_options|
      calls << argv.last
      command_result
    end
    sleeper = lambda do |*_args|
      runner.store.pause!
    end
    runner = build_runner(plan, source, executor: executor, sleeper: sleeper)
    runner.store.prepare!
    runner.store.record_dynamic_running!(job: plan.jobs.first, worker: worker, environment_keys: [])

    assert_equal "paused", runner.run(resume: true)
    assert_empty calls
    assert_equal "running", runner.store.metadata_for(plan.jobs.first).fetch("status")
    assert_equal identity_hash(worker),
                 runner.store.metadata_for(plan.jobs.first).fetch("worker_execution_identity")
  end

  private

  def build_runner(plan, source, executor:, sleeper:, poll_interval: 0.001)
    WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_registry_sleeper: sleeper,
      worker_poll_interval: poll_interval,
      command_executor: executor
    )
  end

  def build_plan(pools:, jobs:, max_consecutive_failures: 10, max_total_failures: 10)
    WorkloadOrchestrator::Plan.new(JSON.generate(
                                     "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                                     "plan_id" => "dynamic-runner-fixture",
                                     "failure_policy" => {
                                       "max_consecutive_failures" => max_consecutive_failures,
                                       "max_total_failures" => max_total_failures
                                     },
                                     "pools" => pools,
                                     "jobs" => jobs
                                   ))
  end

  def pool(model_name)
    {
      "pool_id" => model_name,
      "required_labels" => ["inference"],
      "requirements" => {
        "ollama" => {
          "model" => model_name,
          "expected_digest" => Digest::SHA256.hexdigest(model_name),
          "required_context_length" => 131_072,
          "require_fully_gpu_resident" => true,
          "required_gpu_id" => "NVIDIA A40"
        }
      }
    }
  end

  def job(id, pool_id, group: nil, depends_on: [])
    row = {
      "job_id" => id,
      "pool_id" => pool_id,
      "depends_on_job_ids" => depends_on,
      "argv" => ["fixture-command", id]
    }
    row["group_id"] = group if group
    row
  end

  def worker_record(worker_id, model_name, generation_id: "generation-1")
    worker = {
      "worker_id" => worker_id,
      "generation_id" => generation_id,
      "endpoint" => "http://127.0.0.1:#{20_000 + (Digest::SHA256.hexdigest(worker_id)[0, 4].to_i(16) % 20_000)}",
      "state" => "READY",
      "labels" => ["inference"],
      "capabilities" => {
        "gpu_id" => "NVIDIA A40",
        "ollama" => {
          "models" => [{
            "model" => model_name,
            "digest" => Digest::SHA256.hexdigest(model_name),
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
      {
        "context_length" => model.fetch("context_length"),
        "digest" => model.fetch("digest"),
        "fully_gpu_resident" => model.fetch("fully_gpu_resident"),
        "model" => model.fetch("model")
      }
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
      "registry_id" => "dynamic-runner-registry",
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => "2030-01-01T00:05:00Z",
      "workers" => workers
    )
  end

  def metadata_for(job_id)
    JSON.parse(File.read(File.join(@output, "runs", job_id, "metadata.json")))
  end

  def assert_assignments_match_pools(plan, records)
    workers = records.to_h { |worker| [worker.fetch("worker_id"), worker] }
    plan.jobs.each do |job|
      metadata = metadata_for(job.id)
      worker = workers.fetch(metadata.fetch("worker"))
      assert_equal job.pool_id, worker.dig("capabilities", "ollama", "models", 0, "model")
      assert_equal({
        "registry_id" => "dynamic-runner-registry",
        "worker_id" => worker.fetch("worker_id"),
        "generation_id" => worker.fetch("generation_id"),
        "endpoint" => worker.fetch("endpoint"),
        "capability_fingerprint" => worker.fetch("capability_fingerprint")
      }, metadata.fetch("worker_execution_identity"))
    end
  end

  def identity_hash(worker)
    WorkloadOrchestrator::DynamicWorkerBinding.from_worker(worker).execution_identity
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
