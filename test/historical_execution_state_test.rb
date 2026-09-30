# frozen_string_literal: true

require_relative "test_helper"

class HistoricalExecutionStateTest < Minitest::Test
  include WloTestSupport

  def setup
    @root = Dir.mktmpdir("wlo-historical-state-")
    @plan = WorkloadOrchestrator::Plan.load(
      write_plan(@root, jobs: [job("archived", code: "puts :unused")])
    )
    @output = File.join(@root, "output")
    @store = WorkloadOrchestrator::ExecutionStore.new(
      output_dir: @output, plan: @plan, workdir: @root
    )
    @store.prepare!
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_preserved_workload_status_tracks_historical_cleanup_evidence
    assert_equal "pending", @store.finish!(resource_cleanup_pending: true)
    assert_equal "pending", @store.workload_status
    assert_equal "cleanup_pending", @store.status

    @store.record_resource_disposition!("phase" => "in_progress")
    assert_equal "cleanup_failed", @store.status
    @store.record_resource_disposition!("phase" => "verified_provider_absence")
    assert_equal "pending", @store.status
  end

  def test_persisted_running_attempt_becomes_explicit_remote_in_doubt_evidence
    worker = WorkloadOrchestrator::Worker.new(
      "archived-remote", "type" => "command", "labels" => [],
      "hourly_rate_usd" => 0, "job_env" => {}
    )
    job = @plan.jobs.first
    @store.record_running!(job: job, worker: worker, environment_keys: [])

    @store.reconcile_remote_running!

    metadata = @store.metadata_for(job)
    assert_equal "failed", metadata.fetch("status")
    assert_equal "remote_in_doubt", metadata.dig("evidence", "kind")
    assert @store.dispatch_halted?
    assert_includes File.read(File.join(@output, "runs/archived/stderr.log")), "attempt is in doubt"
  end
end
