# frozen_string_literal: true

require_relative "test_helper"

class LiveExecutionDisplayTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")
  FIXTURE = File.expand_path("fixtures/dynamic-worker-registry-v0.1.json", __dir__)

  class Source < WorkloadOrchestrator::WorkerSource
    def initialize(&callback)
      super()
      @callback = callback
    end

    def latest_snapshot
      @callback.call
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-live-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @out = StringIO.new
    @record = JSON.parse(File.read(FIXTURE)).fetch("workers").first
    @plan = build_plan
    @store = WorkloadOrchestrator::ExecutionStore.new(plan: @plan, output_dir: @output, workdir: @workdir)
    @store.prepare!
    @store.start!
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_one_aggregate_and_active_annotations_across_pools
    first = registry_worker(@record)
    other = registry_worker(@record.merge("worker_id" => "other", "generation_id" => "other-generation",
                                          "endpoint" => "http://127.0.0.1:11499"))
    claim(@plan.jobs[0], first)
    claim(@plan.jobs[1], other)
    display = build_display
    display.accept_workers([first, other])
    before = WorkloadOrchestrator::ExecutionReport.new(plan: @plan, output: @output).document
    display.refresh

    assert_includes @out.string, "EXECUTION live-fixture"
    assert_equal 1, @out.string.scan(/^EXECUTION /).length
    assert_includes @out.string, "0 complete / 2 running / 1 pending / 0 failed"
    assert_includes @out.string, "RUNNING first group=group-a pool=pool-a worker=#{first.worker_id}"
    assert_includes @out.string, "RUNNING second group=group-b pool=pool-b worker=other"
    assert_includes @out.string, "Workers: 2 READY / 2 busy / 0 idle"
    assert_equal before, WorkloadOrchestrator::ExecutionReport.new(plan: @plan, output: @output).document
    refute_match(/[\e\r]/, @out.string)
  end

  def test_repeated_registry_revisions_without_availability_changes_are_quiet
    display = build_display
    display.accept_workers([])
    display.refresh
    before = @out.string.dup
    4.times { display.refresh }
    assert_equal before, @out.string
    assert_includes before, "WAIT Waiting for compatible capacity: 3 pending, 0 eligible idle READY workers"
    assert_equal "running", @store.status
    refute_match(/^(DONE|FAIL|IN_DOUBT)/, before)
    worker = registry_worker(@record)
    display.accept_workers([worker])
    display.refresh
    assert_includes @out.string, "ACTIVE compatible capacity available"
    stable = @out.string.dup
    display.accept_workers([registry_worker(@record, revision: 2)])
    display.refresh
    assert_equal stable, @out.string
  end

  def test_lifecycle_events_use_durable_attempt_state
    worker = registry_worker(@record)
    display = build_display
    display.accept_workers([worker])
    display.refresh
    first = claim(@plan.jobs[0], worker)
    display.refresh
    @store.record_dynamic_terminal!(attempt: first, status: "complete", exit_status: 0)
    display.refresh
    second = claim(@plan.jobs[1], worker)
    display.refresh
    @store.record_dynamic_terminal!(attempt: second, status: "failed", exit_status: 1)
    display.refresh

    assert_equal 2, @out.string.scan(/^START /).length
    assert_equal 1, @out.string.scan(/^DONE /).length
    assert_equal 1, @out.string.scan(/^FAIL /).length
    assert_includes @out.string, "1 complete / 0 running / 1 pending / 1 failed"
    stable = @out.string.dup
    display.refresh
    assert_equal stable, @out.string
  end

  def test_pool_ceiling_and_dependencies_do_not_report_idle_workers_as_eligible
    row = JSON.parse(@plan.bytes)
    row.fetch("jobs")[2]["depends_on_job_ids"] = ["second"]
    plan = WorkloadOrchestrator::Plan.new(JSON.generate(row))
    limited = plan.pools.map { |pool| pool.dup.tap { |copy| copy.max_concurrency = 1 }.freeze }
    plan.define_singleton_method(:pool) { |id| limited.find { |pool| pool.id == id } }
    output = File.join(@tmp, "limited")
    store = WorkloadOrchestrator::ExecutionStore.new(plan: plan, output_dir: output, workdir: @workdir)
    store.prepare!
    store.start!
    worker = registry_worker(@record)
    idle = registry_worker(@record.merge("worker_id" => "idle", "endpoint" => "http://127.0.0.1:11499"))
    store.record_dynamic_running!(job: plan.jobs[0], worker: worker, environment_keys: [])
    # pool-b requires a label absent from both workers.
    report = WorkloadOrchestrator::LiveExecutionReport.new(plan: plan, output: output)
    state = report.document(workers: [worker, idle])

    assert_equal 1, state.fetch("idle_workers")
    assert_equal 0, state.fetch("eligible_idle_workers")
    assert_match(/^WAIT /, state.fetch("condition"))
  end

  def test_controls_are_distinct_from_waiting
    display = build_display
    display.accept_workers([])
    display.refresh
    @store.pause!
    display.refresh
    assert_includes @out.string, "PAUSED execution retained"
    @store.clear_pause!
    @store.record_dispatch_halt!(kind: "worker_registry", error: "invalid snapshot")
    display.refresh
    assert_includes @out.string, "HALT dispatch halted kind=worker_registry reason=invalid snapshot"
    assert_equal 1, @out.string.scan(/^WAIT /).length
  end

  def test_runner_waits_quietly_then_streams_start_and_done_with_partial_capacity
    waits = 0
    runner = nil
    states = []
    source = Source.new { snapshot(waits < 2 ? [] : [@record], revision: waits + 1) }
    sleeper = lambda do |*_args|
      states << runner.store.status
      waits += 1
      if waits >= 3
        # pool-b remains pending; stop after the two compatible jobs complete.
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
        until runner.store.counts["running"].zero?
          raise "timeout waiting for terminal jobs" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

          Thread.pass
        end
        runner.store.pause! if runner.store.counts["complete"] == 2
      end
    end
    runner = build_runner(source: source, executor: ->(*) { command_result }, sleeper: sleeper)
    assert_equal "paused", runner.run

    assert(states.all? { |status| status == "running" })
    assert_includes @out.string, "WAIT Waiting for compatible capacity: 3 pending"
    assert_equal 2, @out.string.scan(/^START /).length
    assert_equal 2, @out.string.scan(/^DONE /).length
    assert_includes @out.string, "2 complete / 0 running / 1 pending / 0 failed"
    assert_includes @out.string, "PAUSE"
    assert_equal "pending", runner.store.metadata_for(@plan.jobs[1])&.fetch("status") || "pending"
  end

  def test_runner_pause_resume_preserves_identity_and_progress
    runner = nil
    source = Source.new { snapshot([@record]) }
    executor = lambda do |*_args|
      runner.store.pause!
      command_result
    end
    runner = build_runner(source: source, executor: executor)
    assert_equal "paused", runner.run
    completed = runner.store.counts["complete"]
    ready_b = @record.merge("labels" => @record.fetch("labels") + ["pool-b"])
    ready_b["capability_fingerprint"] = fingerprint(ready_b)
    resumed = build_runner(source: Source.new { snapshot([ready_b], revision: 2) }, executor: ->(*) { command_result })
    assert_equal "completed", resumed.run(resume: true)

    assert_operator completed, :>, 0
    assert_includes @out.string, "RESUME live-fixture"
    assert_includes @out.string, "3 complete / 0 running / 0 pending / 0 failed"
    assert_equal 3, @out.string.scan(/^DONE /).length
  end

  def test_generation_loss_streams_in_doubt_and_halt_not_completion
    polls = 0
    runner = nil
    first_started = Queue.new
    source = Source.new do
      polls += 1
      snapshot(polls == 1 ? [@record] : [], revision: polls)
    end
    executor = lambda do |*_args|
      first_started << true
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      until runner.store.dispatch_halted?
        raise "timeout waiting for halt" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        Thread.pass
      end
      command_result
    end
    sleeper = ->(*) { first_started.pop if polls == 1 }
    runner = build_runner(source: source, executor: executor, sleeper: sleeper)
    assert_equal "infrastructure_failed", runner.run

    assert_includes @out.string, "IN_DOUBT first group=group-a pool=pool-a worker=#{@record.fetch('worker_id')}"
    assert_includes @out.string, "generation=#{@record.fetch('generation_id')}"
    assert_includes @out.string, "HALT dispatch halted kind=dynamic_worker_loss"
    refute_match(/^DONE /, @out.string)
    assert_equal 1, @out.string.scan(/^IN_DOUBT /).length
  end

  def test_tty_and_redirected_streams_share_plain_output
    tty_out = StringIO.new
    tty_out.define_singleton_method(:tty?) { true }
    display = build_display
    tty = WorkloadOrchestrator::LiveExecutionDisplay.new(plan: @plan, output: @output, out: tty_out)
    display.accept_workers([registry_worker(@record)])
    tty.accept_workers([registry_worker(@record)])
    display.refresh
    tty.refresh

    assert_equal @out.string, tty_out.string
    refute_match(/[\e\r]/, tty_out.string)
  end

  def test_runner_streams_failed_exit_and_exception_with_breaker
    [false, true].each do |raise_error|
      FileUtils.rm_rf(@output)
      @out.truncate(0)
      @out.rewind
      row = JSON.parse(@plan.bytes)
      row["failure_policy"]["max_consecutive_failures"] = 1
      @plan = WorkloadOrchestrator::Plan.new(JSON.generate(row))
      executor = lambda do |*_args|
        raise "fixture command failure" if raise_error

        command_result(exit_status: 1)
      end
      runner = build_runner(source: Source.new { snapshot([@record]) }, executor: executor)
      assert_equal "circuit_broken", runner.run
      assert_equal 1, @out.string.scan(/^START /).length
      assert_equal 1, @out.string.scan(/^FAIL /).length
      assert_includes @out.string, "BREAKER consecutive failure limit reached"
      refute_match(/^(DONE|IN_DOUBT)/, @out.string)
      assert_equal 1, runner.store.counts["failed"]
    end
  end

  def test_interruption_is_displayed_separately_from_capacity_waiting
    runner = nil
    sleeper = ->(*) { runner.instance_variable_set(:@interrupt_signal, "TERM") }
    runner = build_runner(source: Source.new { snapshot([]) }, executor: ->(*) { command_result }, sleeper: sleeper)
    assert_equal "interrupted", runner.run
    assert_includes @out.string, "INTERRUPTED signal=TERM"
    assert_equal "interrupted", runner.store.status
  end

  def test_ordinary_failure_reason_is_not_reported_as_in_doubt
    worker = registry_worker(@record)
    display = build_display
    display.accept_workers([worker])
    display.refresh
    attempt = claim(@plan.jobs.first, worker)
    display.refresh
    @store.record_dynamic_terminal!(attempt: attempt, status: "failed", exit_status: 1,
                                    evidence: { "reason" => "ordinary_failure" })
    display.refresh

    assert_includes @out.string, "FAIL first"
    refute_match(/^IN_DOUBT /, @out.string)
  end

  def test_startup_running_and_worker_details_are_bounded
    row = JSON.parse(@plan.bytes)
    row["jobs"] = 12.times.map { |index| fixture_job("job-#{index}", pool_id: "pool-a", group_id: "group-#{index}") }
    plan = WorkloadOrchestrator::Plan.new(JSON.generate(row))
    output = File.join(@tmp, "bounded")
    store = WorkloadOrchestrator::ExecutionStore.new(plan: plan, output_dir: output, workdir: @workdir)
    store.prepare!
    store.start!
    workers = 12.times.map do |index|
      registry_worker(@record.merge("worker_id" => "worker-#{index}", "endpoint" => "http://127.0.0.1:#{20_000 + index}"))
    end
    plan.jobs.first(11).each_with_index do |job, index|
      store.record_dynamic_running!(job: job, worker: workers[index], environment_keys: [])
    end
    display = WorkloadOrchestrator::LiveExecutionDisplay.new(plan: plan, output: output, out: @out)
    display.accept_workers(workers)
    display.refresh

    assert_equal 10, @out.string.scan(/^RUNNING job-/).length
    assert_includes @out.string, "RUNNING 1 more; use status --json"
    assert_includes @out.string, "2 more READY workers"
    assert_includes @out.string, "0 complete / 11 running / 1 pending / 0 failed"
    previous = @out.string.dup
    display.refresh
    assert_equal previous, @out.string
  end

  def test_generation_reuse_does_not_make_replacement_busy
    original = registry_worker(@record)
    claim(@plan.jobs.first, original)
    replacement = registry_worker(@record.merge("generation_id" => "replacement-generation"), revision: 2)
    state = WorkloadOrchestrator::LiveExecutionReport.new(plan: @plan, output: @output).document(workers: [replacement])

    assert_equal 1, state.fetch("ready_workers")
    assert_equal 0, state.fetch("busy_workers")
    assert_equal 1, state.fetch("idle_workers")
    assert_equal original.generation_id, state.fetch("jobs").first.dig("worker_execution_identity", "generation_id")
    assert_equal "replacement-generation", state.fetch("workers").first.fetch("generation_id")
    assert_equal "running", @store.metadata_for(@plan.jobs.first).fetch("status")
  end

  def test_json_status_shape_is_unchanged_by_live_reporting
    expected = WorkloadOrchestrator::ExecutionReport.new(plan: @plan, output: @output).document
    display = build_display
    display.accept_workers([registry_worker(@record)])
    display.refresh
    plan_path = File.join(@tmp, "plan.json")
    File.write(plan_path, @plan.bytes)
    out = StringIO.new
    code = WorkloadOrchestrator::CLI.new(["status", plan_path, "--output", @output], out: out, err: StringIO.new).run

    assert_equal 0, code
    assert_equal expected, JSON.parse(out.string)
    refute_includes out.string, "ready_workers"
  end

  private

  def build_plan
    WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "live-fixture",
      "failure_policy" => { "max_consecutive_failures" => 10, "max_total_failures" => 10 },
      "pools" => [
        { "pool_id" => "pool-a", "required_labels" => ["inference"] },
        { "pool_id" => "pool-b", "required_labels" => ["pool-b"] }
      ],
      "jobs" => [fixture_job("first", pool_id: "pool-a", group_id: "group-a"),
                 fixture_job("second", pool_id: "pool-b", group_id: "group-b"),
                 fixture_job("third", pool_id: "pool-a", group_id: "group-c")]
    ))
  end

  def snapshot(workers, revision: 1)
    document = JSON.parse(File.read(FIXTURE))
    JSON.generate(document.merge("workers" => workers, "revision" => revision,
                                 "published_at" => (Time.iso8601("2030-01-01T00:00:00Z") + revision).iso8601))
  end

  def registry_worker(record, revision: 1)
    WorkloadOrchestrator::DynamicWorkerRegistry.new(snapshot([record], revision: revision), now: NOW).entries.first
  end

  def claim(job, worker)
    @store.record_dynamic_running!(job: job, worker: worker, environment_keys: [])
  end

  def build_display
    WorkloadOrchestrator::LiveExecutionDisplay.new(plan: @plan, output: @output, out: @out)
  end

  def build_runner(source:, executor:, sleeper: nil)
    WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: WorkloadOrchestrator::WorkerSet.new({}), workdir: @workdir,
      output_dir: @output, out: @out, worker_source: source, command_executor: executor,
      worker_registry_clock: -> { NOW }, worker_registry_sleeper: sleeper, worker_poll_interval: 0.001
    )
  end

  def fingerprint(record)
    models = record.dig("capabilities", "ollama", "models").map do |model|
      model.slice("context_length", "digest", "fully_gpu_resident", "model")
    end
    Digest::SHA256.hexdigest(JSON.generate("gpu_id" => record.dig("capabilities", "gpu_id"),
                                           "labels" => record.fetch("labels"), "ollama_models" => models))
  end
end
