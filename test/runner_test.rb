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

  def test_plan_identity_cannot_change_inside_existing_output
    first = load_plan([job("job-1", code: "exit 0")])
    assert_equal "completed", build_runner(first).run

    changed_path = write_plan(@tmp, jobs: [job("job-1", code: "puts :changed")], name: "changed.json")
    changed = WorkloadOrchestrator::Plan.load(changed_path)
    error = assert_raises(WorkloadOrchestrator::Error) { build_runner(changed).run }

    assert_includes error.message, "different execution identity"
  end

  def test_circuit_breaker_stops_new_dispatch_and_requires_acknowledgement
    marker = File.join(@workdir, "later.txt")
    jobs = [
      job("fail-1", code: "exit 1"),
      job("fail-2", code: "exit 1"),
      job("later-1", code: "File.write(#{marker.inspect}, 'one')"),
      job("later-2", code: "File.open(#{marker.inspect}, 'a') { |file| file.write('two') }")
    ]
    plan = load_plan(jobs)

    assert_equal "circuit_broken", build_runner(plan).run
    refute File.exist?(marker)
    assert_raises(WorkloadOrchestrator::Error) { build_runner(plan).run(resume: true) }

    status = build_runner(plan).run(resume: true, acknowledge_circuit_breaker: true)
    assert_equal "workload_failed", status
    assert_equal "onetwo", File.read(marker)
    assert_equal({ "complete" => 2, "failed" => 2 }, compact_counts(plan))
  end

  def test_graceful_pause_prevents_dispatch_until_resume
    marker = File.join(@workdir, "paused.txt")
    plan = load_plan([job("job-1", code: "File.write(#{marker.inspect}, 'ran')")])
    runner = build_runner(plan)
    runner.store.prepare!
    runner.store.pause!

    assert_equal "paused", runner.run
    refute File.exist?(marker)
    assert_equal "completed", build_runner(plan).run(resume: true)
    assert_equal "ran", File.read(marker)
  end

  def test_positive_cost_worker_is_rejected_before_output_is_created
    paid_workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp, rate: 0.01))
    plan = load_plan([job("job-1", code: "exit 0")])
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

  def load_plan(jobs)
    WorkloadOrchestrator::Plan.load(write_plan(@tmp, jobs: jobs))
  end

  def build_runner(plan)
    WorkloadOrchestrator::Runner.new(
      plan: plan,
      workers: @workers,
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new
    )
  end

  def compact_counts(plan)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: plan, workdir: @workdir)
    store.prepare!
    store.counts.reject { |_key, value| value.zero? }
  end
end
