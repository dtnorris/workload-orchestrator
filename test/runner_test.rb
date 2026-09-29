# frozen_string_literal: true

require_relative "test_helper"

class RunnerTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-runner-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp))
    @executed = []
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_runs_generic_command_and_resume_does_not_rerun_terminal_job
    code = 'File.open("counter.txt", "a") { |file| file.puts "run" }'
    plan = load_plan([job("job-1", code: code)])
    runner = build_runner(plan)

    assert_equal "completed", runner.run
    assert_equal "completed", build_runner(plan).run(resume: true)
    assert_equal ["run"], File.readlines(File.join(@workdir, "counter.txt"), chomp: true)

    metadata = JSON.parse(File.read(File.join(@output, "runs", "job-1", "metadata.json")))
    assert_equal "complete", metadata.fetch("status")
    assert_equal 1, metadata.fetch("attempt")
    assert File.file?(File.join(@output, "plan.json"))
    assert File.file?(File.join(@output, "jobs.json"))
  end

  def test_pool_concurrency_assigns_distinct_workers
    workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp, extra_local: true))
    pool = command_pool(worker_names: %w[local local2], max_concurrency: 2)
    plan_path = write_plan(
      @tmp,
      pools: [pool],
      jobs: [job("job-1", code: "sleep 0.05"), job("job-2", code: "sleep 0.05")]
    )
    plan = WorkloadOrchestrator::Plan.load(plan_path)
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan, workers: workers, workdir: @workdir, output_dir: @output, out: StringIO.new
    )

    assert_equal "completed", runner.run
    assigned = plan.jobs.map do |entry|
      JSON.parse(File.read(File.join(@output, "runs", entry.id, "metadata.json"))).fetch("worker")
    end
    assert_equal %w[local local2], assigned.sort
  end

  def test_grouped_jobs_run_group_major_across_pools
    second_pool = command_pool.merge("pool_id" => "second-pool")
    plan_path = write_plan(
      @tmp,
      pools: [command_pool, second_pool],
      jobs: [
        fixture_job("a-one", group_id: "adventure-a"),
        fixture_job("a-two", pool_id: "second-pool", group_id: "adventure-a"),
        fixture_job("b-one", group_id: "adventure-b"),
        fixture_job("b-two", pool_id: "second-pool", group_id: "adventure-b")
      ]
    )
    plan = WorkloadOrchestrator::Plan.load(plan_path)

    assert_equal "completed", build_runner(plan, command_executor: fake_executor).run
    assert_equal %w[a-one a-two b-one b-two], @executed
  end

  def test_ungrouped_jobs_retain_pool_major_order
    second_pool = command_pool.merge("pool_id" => "second-pool")
    plan_path = write_plan(
      @tmp,
      pools: [command_pool, second_pool],
      jobs: [
        fixture_job("a-one"),
        fixture_job("a-two", pool_id: "second-pool"),
        fixture_job("b-one"),
        fixture_job("b-two", pool_id: "second-pool")
      ]
    )
    plan = WorkloadOrchestrator::Plan.load(plan_path)

    assert_equal "completed", build_runner(plan, command_executor: fake_executor).run
    assert_equal %w[a-one b-one a-two b-two], @executed
  end

  def test_job_output_includes_stable_plan_position_and_total
    plan = load_plan(
      [
        fixture_job("job-1"),
        fixture_job("job-2"),
        fixture_job("job-3")
      ]
    )
    out = StringIO.new
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan, workers: @workers, workdir: @workdir, output_dir: @output, out: out,
      command_executor: fake_executor
    )

    assert_equal "completed", runner.run
    assert_includes out.string, "[1/3] [local] job-1"
    assert_includes out.string, "[2/3] [local] job-2"
    assert_includes out.string, "[3/3] [local] job-3"
  end

  def test_environment_can_explicitly_remove_inherited_value
    code = 'puts ENV.key?("WLO_TEST_SECRET") ? ENV.fetch("WLO_TEST_SECRET") : "unset"'
    plan = load_plan([job("job-1", code: code, env: { "WLO_TEST_SECRET" => nil })])
    ENV["WLO_TEST_SECRET"] = "parent"

    assert_equal "completed", build_runner(plan).run
    stdout = File.read(File.join(@output, "runs", "job-1", "stdout.log"))
    assert_equal "unset\n", stdout
  ensure
    ENV.delete("WLO_TEST_SECRET")
  end

  def test_real_command_records_exit_status_and_both_output_streams
    plan = load_plan([job("failed", code: 'puts "stdout"; warn "stderr"; exit 7')])

    assert_equal "workload_failed", build_runner(plan).run
    root = File.join(@output, "runs", "failed")
    assert_equal "stdout\n", File.read(File.join(root, "stdout.log"))
    assert_equal "stderr\n", File.read(File.join(root, "stderr.log"))
    metadata = JSON.parse(File.read(File.join(root, "metadata.json")))
    assert_equal "failed", metadata.fetch("status")
    assert_equal 7, metadata.fetch("exit_status")
  end

  def test_executor_error_is_recorded_as_failed_attempt
    plan = load_plan([fixture_job("failed")])
    executor = lambda do |*_args, **_options|
      raise IOError, "fixture launch failure"
    end

    assert_equal "workload_failed", build_runner(plan, command_executor: executor).run
    root = File.join(@output, "runs", "failed")
    metadata = JSON.parse(File.read(File.join(root, "metadata.json")))
    assert_equal "failed", metadata.fetch("status")
    assert_nil metadata.fetch("exit_status")
    assert_equal "fixture launch failure", metadata.fetch("error")
    assert_equal "IOError: fixture launch failure\n", File.read(File.join(root, "stderr.log"))
  end

  def test_plan_identity_cannot_change_inside_existing_output
    first = load_plan([fixture_job("job-1")])
    assert_equal "completed", build_runner(first, command_executor: fake_executor).run

    changed_job = fixture_job("job-1")
    changed_job["argv"][-1] = "changed"
    changed_path = write_plan(@tmp, jobs: [changed_job], name: "changed.json")
    changed = WorkloadOrchestrator::Plan.load(changed_path)
    error = assert_raises(WorkloadOrchestrator::Error) { build_runner(changed).run }

    assert_includes error.message, "different execution identity"
  end

  def test_circuit_breaker_stops_new_dispatch_and_requires_acknowledgement
    jobs = [
      fixture_job("fail-1"), fixture_job("fail-2"),
      fixture_job("later-1"), fixture_job("later-2")
    ]
    plan = load_plan(jobs)
    executor = fake_executor { |id| command_result(exit_status: id.start_with?("fail-") ? 1 : 0) }

    assert_equal "circuit_broken", build_runner(plan, command_executor: executor).run
    assert_equal %w[fail-1 fail-2], @executed
    assert_raises(WorkloadOrchestrator::Error) { build_runner(plan).run(resume: true) }

    status = build_runner(plan, command_executor: executor).run(resume: true, acknowledge_circuit_breaker: true)
    assert_equal "workload_failed", status
    assert_equal %w[fail-1 fail-2 later-1 later-2], @executed
    assert_equal({ "complete" => 2, "failed" => 2 }, compact_counts(plan))
  end

  def test_graceful_pause_prevents_dispatch_until_resume
    plan = load_plan([fixture_job("job-1")])
    executor = fake_executor
    runner = build_runner(plan, command_executor: executor)
    runner.store.prepare!
    runner.store.pause!

    assert_equal "paused", runner.run
    assert_empty @executed
    assert_equal "completed", build_runner(plan, command_executor: executor).run(resume: true)
    assert_equal ["job-1"], @executed
  end

  def test_positive_cost_worker_is_rejected_before_output_is_created
    paid_workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp, rate: 0.01))
    plan = load_plan([fixture_job("job-1")])
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: paid_workers,
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new
    )

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run }
    assert_includes error.message, "refuses paid worker"
    refute Dir.exist?(@output)
  end

  private

  def fake_executor(&result)
    lambda do |_environment, *argv, chdir:|
      raise "unexpected workdir: #{chdir}" unless chdir == @workdir

      id = argv.last
      @executed << id
      result ? result.call(id) : command_result
    end
  end

  def load_plan(jobs)
    WorkloadOrchestrator::Plan.load(write_plan(@tmp, jobs: jobs))
  end

  def build_runner(plan, command_executor: Open3.method(:capture3))
    WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: @workers,
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      command_executor: command_executor
    )
  end

  def compact_counts(plan)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: plan, workdir: @workdir)
    store.prepare!
    store.counts.reject { |_key, value| value.zero? }
  end
end
