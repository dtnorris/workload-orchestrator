# frozen_string_literal: true

require_relative "test_helper"

class ConsumerDemandTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-demand-")
    @now = Time.now.utc
    @plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, jobs: [fixture_job("one"), fixture_job("two")]))
    @store = store
    @worker = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp)).fetch("local")
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_missing_and_historical_heartbeat_are_unknown_without_writes
    assert_equal "missing", demand.fetch("state")
    refute_path_exists @store.output_dir
    @store.prepare!
    before = File.binread(File.join(@store.output_dir, "execution.json"))
    assert_equal "missing_heartbeat", demand.fetch("state")
    refute demand.fetch("quiescent")
    assert_equal before, File.binread(File.join(@store.output_dir, "execution.json"))
  end

  def test_active_pending_and_waiting_capacity_are_capped_by_pool_concurrency
    activate
    assert_equal "active", demand.fetch("state")
    assert_equal 1, demand.fetch("runnable_count")
    assert_equal 0, demand.fetch("bound_count")
    assert demand.fetch("fresh")
  end

  def test_running_work_is_bound_even_after_heartbeat_expires
    activate
    @store.record_running!(job: @plan.jobs.first, worker: @worker, environment_keys: [])
    assert_equal 1, demand.fetch("bound_count")
    assert_equal 0, demand.fetch("runnable_count")
    @now += 31
    assert_equal "stale", demand.fetch("state")
    refute demand.fetch("quiescent")
  end

  def test_restart_preserves_liveness_and_consumer_identity
    activate
    before = demand
    @store = store
    assert_equal before, demand
    @now += 31
    refute demand.fetch("fresh")
    assert_equal 0, demand.fetch("runnable_count")
  end

  def test_pause_completion_failure_and_recovery_required_have_no_runnable_demand
    activate
    @store.pause!
    assert_equal "paused", demand.fetch("state")
    assert_equal 0, demand.fetch("runnable_count")
    assert demand.fetch("quiescent")
    @store.clear_pause!
    @plan.jobs.each { |job| terminal(job, "complete") }
    @store.finish!
    assert_equal "completed", demand.fetch("state")
    assert demand.fetch("quiescent")
    @store.record_interruption!("INT")
    assert_equal "interrupted", demand.fetch("state")
    assert_equal 0, demand.fetch("runnable_count")
  end

  def test_terminal_failure_and_legacy_remote_uncertainty
    activate
    terminal(@plan.jobs.first, "failed", evidence: { "kind" => "remote_in_doubt" })
    terminal(@plan.jobs.last, "complete")
    @store.finish!
    assert_equal "workload_failed", demand.fetch("state")
    assert_equal 1, demand.fetch("uncertain_count")
    refute demand.fetch("quiescent")
  end

  def test_public_output_has_no_attempt_mapping_or_provider_domain_vocabulary
    activate
    document = JSON.generate(demand)
    refute_match(/RunPod|AdventureFinder|campaign|price|pod_id|worker_id|attempt_id|metadata.json/i, document)
    refute_match(/RunPod|AdventureFinder|campaign|price|pod_id/i,
                 File.read(File.expand_path("../lib/workload_orchestrator/consumer_demand.rb", __dir__)))
  end

  def test_public_cli_returns_json_and_requires_exact_arguments
    activate
    out = StringIO.new
    err = StringIO.new
    args = ["consumer-demand", File.join(@tmp, "plan.json"), "--workdir", @tmp,
            "--output", @store.output_dir, "--pool", "local-pool"]
    assert_equal 0, WorkloadOrchestrator::CLI.new(args, out:, err:).run, err.string
    assert_equal demand.fetch("consumer_id"), JSON.parse(out.string).fetch("consumer_id")
    assert_equal 1, WorkloadOrchestrator::CLI.new(args[0..1], out:, err:).run
    assert_includes err.string, "--workdir"
  end

  def test_interrupted_attempt_remains_bound_after_archiving
    activate
    terminal(@plan.jobs.first, "interrupted")
    assert_equal 1, demand.fetch("bound_count")
    refute demand.fetch("quiescent")
  end

  private

  def store
    WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @tmp, output_dir: File.join(@tmp, "out"))
  end

  def activate
    @store.prepare!
    @store.start!
    @store.consumer_heartbeat!(now: @now)
  end

  def demand
    @store.consumer_demand(pool_id: "local-pool", now: @now)
  end

  def terminal(job, status, evidence: nil)
    @store.record_running!(job:, worker: @worker, environment_keys: [])
    @store.record_terminal!(job:, status:, started_at: @now, exit_status: status == "complete" ? 0 : 1, evidence:)
  end
end
