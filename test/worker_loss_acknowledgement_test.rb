# frozen_string_literal: true

require_relative "test_helper"
require "digest"

# Small portable contract tests complement the original-execution offline proof.
class WorkerLossAcknowledgementTest < Minitest::Test
  include WloTestSupport

  # Create one genuine loss transition, a second failed job and pending work.
  def setup
    @tmp = Dir.mktmpdir("wlo-loss-review-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@workdir, "output")
    FileUtils.mkdir_p(@workdir)
    jobs = %w[lost invalid pending].map { |id| fixture_job(id) }
    policy = { "max_consecutive_failures" => 1, "max_total_failures" => 1 }
    @plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, jobs: jobs, failure_policy: policy))
    @store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: @plan, workdir: @workdir)
    @store.prepare!
    @store.with_execution_lock { nil }
    @now = Time.utc(2030)
    @snapshot = JSON.parse(File.binread(File.expand_path(
                                          "../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__
                                        )))
    @snapshot.merge!("published_at" => @now.iso8601, "expires_at" => (@now + 60).iso8601, "revision" => 1)
    @poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: WorkloadOrchestrator::StaticWorkerSource.new(JSON.generate(@snapshot)),
      checkpoint_path: File.join(@output, "checkpoint.json"), clock: -> { @now }
    )
    @poller.poll_once
    @worker = @poller.current_workers.first
    failed = @store.record_dynamic_running!(job: @plan.jobs[1], worker: @poller.current_workers.first,
                                            environment_keys: [])
    @store.record_dynamic_terminal!(attempt: failed, status: "failed", exit_status: 42)
    @store.acknowledge_circuit_breaker!
    @attempt = @store.record_dynamic_running!(job: @plan.jobs.first, worker: @poller.current_workers.first,
                                              environment_keys: [])
    @snapshot.merge!("workers" => [], "revision" => 2, "published_at" => (@now + 1).iso8601)
    @now += 1
    @poller.instance_variable_set(:@source, WorkloadOrchestrator::StaticWorkerSource.new(JSON.generate(@snapshot)))
    @poller.poll_once
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: @store).reconcile!(@poller)
    @store.record_dynamic_terminal!(attempt: @attempt, status: "complete", exit_status: 0)
    @review = review
  end

  # Remove only the test-owned disposable root.
  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_acknowledgement_preserves_failures_and_pending_without_retry
    metadata = File.binread(metadata_path)
    jobs = File.binread(File.join(@output, "jobs.json"))
    before = File.binread(File.join(@output, "execution.json"))
    assert authorize(dry_run: true).fetch("dry_run")
    assert_equal before, File.binread(File.join(@output, "execution.json"))
    refute authorize.fetch("dry_run")
    assert_equal metadata, File.binread(metadata_path)
    assert_equal jobs, File.binread(File.join(@output, "jobs.json"))
    assert_equal({ "failed" => 2, "pending" => 1 }, @store.counts)
    refute @store.dispatch_halted?
    refute @store.circuit_tripped?
    refute @store.restore_dynamic_worker_loss_halt!
    @store.prepare!
    assert authorize.fetch("idempotent")
  end

  def test_changed_review_and_changed_evidence_are_rejected_before_mutation
    before = File.binread(File.join(@output, "execution.json"))
    assert_raises(WorkloadOrchestrator::Error) { @store.acknowledge_worker_loss!(bytes: JSON.generate(@review), sha256: "0" * 64) }
    File.binwrite(metadata_path, "#{File.binread(metadata_path)} ")
    assert_raises(WorkloadOrchestrator::Error) { authorize }
    assert_equal before, File.binread(File.join(@output, "execution.json"))
  end

  def test_changed_evidence_after_acknowledgement_blocks_prepare
    authorize
    File.binwrite(metadata_path, "#{File.binread(metadata_path)} ")
    assert_raises(WorkloadOrchestrator::Error) { @store.prepare! }
  end

  def test_new_failure_retrips_breaker_and_old_authorization_cannot_clear_it
    authorize
    attempt = @store.record_dynamic_running!(job: @plan.jobs.last, worker: @worker, environment_keys: [])
    @store.record_dynamic_terminal!(attempt: attempt, status: "failed", exit_status: 17)
    assert @store.circuit_tripped?
    before = File.binread(File.join(@output, "execution.json"))
    assert authorize.fetch("idempotent")
    assert_equal before, File.binread(File.join(@output, "execution.json"))
    assert @store.circuit_tripped?
  end

  def test_missing_failed_attempt_from_review_is_rejected
    @review.fetch("attempts").delete("invalid")
    assert_raises(WorkloadOrchestrator::Error) { authorize }
    assert @store.dispatch_halted?
  end

  private

  # Return the current loss metadata path.
  def metadata_path
    File.join(@output, "runs/lost/metadata.json")
  end

  # Create an exact hash-bound review of all existing attempt evidence.
  def review
    files = Dir.glob(File.join(@output, "runs/**/*")).select { |path| File.file?(path) }.to_h do |path|
      [path.delete_prefix("#{@workdir}/"), Digest::SHA256.file(path).hexdigest]
    end
    attempts = @plan.jobs.filter_map do |job|
      path = File.join(@output, "runs", job.id, "metadata.json")
      [job.id, Digest::SHA256.file(path).hexdigest] if File.file?(path)
    end.to_h
    { "contract_version" => "wlo-worker-loss-review/v0.1", "action" => "acknowledge_loss_without_retry",
      "operator" => "offline test", "reason" => "reviewed loss and domain failure", "plan_id" => @plan.id,
      "plan_sha256" => @plan.sha256, "workdir" => @workdir, "job_id" => "lost", "attempt" => 1,
      "execution_sha256" => Digest::SHA256.file(File.join(@output, "execution.json")).hexdigest,
      "jobs_sha256" => Digest::SHA256.file(File.join(@output, "jobs.json")).hexdigest,
      "workdir_evidence_sha256" => @store.send(:evidence_sha256, @workdir),
      "attempts" => attempts, "preserved_files" => files, "preserved_directories" => {} }
  end

  # Invoke the public store operation with the exact review digest.
  def authorize(dry_run: false)
    bytes = JSON.generate(@review)
    @store.acknowledge_worker_loss!(bytes: bytes, sha256: Digest::SHA256.hexdigest(bytes), dry_run: dry_run)
  end
end
