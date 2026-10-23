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
    jobs = %w[lost invalid second pending].map { |id| fixture_job(id) }
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
    assert_equal({ "failed" => 2, "pending" => 2 }, @store.counts)
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

  # A retained, fully validated acknowledgement exempts only its exact loss.
  def test_second_loss_can_be_reviewed_without_retrying_the_first
    authorize
    original = File.binread(metadata_path)
    second_loss
    before = File.binread(File.join(@output, "execution.json"))
    assert authorize(dry_run: true).fetch("dry_run")
    assert_equal before, File.binread(File.join(@output, "execution.json"))
    refute authorize.fetch("idempotent")
    assert_equal original, File.binread(metadata_path)
    assert_equal({ "failed" => 3, "pending" => 1 }, @store.counts)
    assert_equal 2, JSON.parse(File.binread(File.join(@output, "execution.json"))).fetch("worker_loss_acknowledgements").length
    @store.prepare!
    refute @store.restore_dynamic_worker_loss_halt!
    assert_equal "execute", @store.action_check("resume").fetch("disposition")
  end

  # Validate the whole old review, even when its target metadata pin still
  # matches. Each invalid history must refuse without changing any store bytes.
  def test_invalid_prior_acknowledgements_cannot_exempt_a_retained_loss
    authorize
    second_loss
    original = File.binread(File.join(@output, "execution.json"))
    corruptions = {
      "missing" => ->(state) { state.delete("worker_loss_acknowledgements") },
      "history type" => ->(state) { state["worker_loss_acknowledgements"] = {} },
      "raw bytes" => ->(state) { state.fetch("worker_loss_acknowledgements").first["review_bytes"] += " " },
      "digest" => ->(state) { state.fetch("worker_loss_acknowledgements").first["review_sha256"] = "0" * 64 },
      "parsed copy" => ->(state) { state.fetch("worker_loss_acknowledgements").first.fetch("review")["reason"] = "altered" },
      "missing raw review" => ->(state) { state.fetch("worker_loss_acknowledgements").first.delete("review_bytes") }
    }
    corruptions.each do |label, corrupt|
      state = JSON.parse(original)
      corrupt.call(state)
      File.binwrite(File.join(@output, "execution.json"), JSON.generate(state))
      @review = review.merge("job_id" => "second")
      assert_refused_without_mutation(label)
    end
  end

  # Recomputed review bytes/digests cannot substitute another execution, job,
  # attempt or stale pin for A. Native review validation must still apply.
  def test_wrong_prior_review_identity_or_evidence_is_not_authorization
    authorize
    second_loss
    original = File.binread(File.join(@output, "execution.json"))
    corruptions = {
      "plan" => ->(prior) { prior["plan_id"] = "another-plan" },
      "plan hash" => ->(prior) { prior["plan_sha256"] = "0" * 64 },
      "workdir" => ->(prior) { prior["workdir"] = @tmp },
      "job" => ->(prior) { prior["job_id"] = "invalid" },
      "attempt" => ->(prior) { prior["attempt"] = 2 },
      "omitted target pin" => ->(prior) { prior.fetch("attempts").delete("lost") },
      "stale pin" => ->(prior) { prior.fetch("attempts")["lost"] = "0" * 64 },
      "retained file" => ->(prior) { prior.fetch("preserved_files")["output/runs/lost/metadata.json"] = "0" * 64 },
      "retained directory" => ->(prior) { prior.fetch("preserved_directories")["output/runs/lost"] = [] }
    }
    corruptions.each do |label, corrupt|
      state = JSON.parse(original)
      row = state.fetch("worker_loss_acknowledgements").first
      corrupt.call(row.fetch("review"))
      row["review_bytes"] = JSON.generate(row.fetch("review"))
      row["review_sha256"] = Digest::SHA256.hexdigest(row.fetch("review_bytes"))
      File.binwrite(File.join(@output, "execution.json"), JSON.generate(state))
      @review = review.merge("job_id" => "second")
      assert_refused_without_mutation(label)
    end
    state = JSON.parse(original)
    row = state.fetch("worker_loss_acknowledgements").first
    row["review_bytes"] = "{malformed"
    row["review_sha256"] = Digest::SHA256.hexdigest(row.fetch("review_bytes"))
    File.binwrite(File.join(@output, "execution.json"), JSON.generate(state))
    assert_refused_without_mutation("malformed raw review")
  end

  # Full prior evidence stays binding even if B's new review pins the alteration.
  def test_changed_prior_attempt_or_directory_refuses_second_acknowledgement
    authorize
    second_loss
    original = File.binread(metadata_path)
    changes = [
      ->(document) { document["late_evidence"] = [] },
      ->(document) { document["attempt"] = 2 },
      ->(document) { document["job_id"] = "second" },
      ->(document) { document.fetch("worker_execution_identity")["worker_id"] = "wrong-worker" }
    ]
    changes.each do |change|
      document = JSON.parse(original)
      change.call(document)
      File.binwrite(metadata_path, JSON.generate(document))
      @review = review.merge("job_id" => "second")
      assert_refused_without_mutation("changed prior attempt")
    end
    File.binwrite(metadata_path, original)
    File.write(File.join(@output, "runs/lost/extra"), "altered directory membership")
    @review = review.merge("job_id" => "second")
    assert_refused_without_mutation("changed prior directory")
  end

  # A third genuinely unreviewed native loss cannot hide behind valid A history.
  def test_additional_unreviewed_loss_refuses_second_acknowledgement
    authorize
    second_loss
    attempt = @store.record_dynamic_running!(job: @plan.jobs.last, worker: @worker, environment_keys: [])
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: @store).reconcile!(@poller)
    @store.record_dynamic_terminal!(attempt: attempt, status: "complete", exit_status: 0)
    @review = review.merge("job_id" => "second")
    assert_refused_without_mutation("third unreviewed loss")
  end

  # Retain the running/interrupted refusal gates when
  # a valid prior acknowledgement is present; no protected bytes may change.
  def test_second_acknowledgement_retains_existing_stopped_state_gates
    authorize
    second_loss
    attempt = @store.record_dynamic_running!(job: @plan.jobs.last, worker: @worker, environment_keys: [])
    @review = review.merge("job_id" => "second")
    assert_refused_without_mutation("running")
    @store.record_dynamic_terminal!(attempt: attempt, status: "interrupted", exit_status: nil)
    @review = review.merge("job_id" => "second")
    assert_refused_without_mutation("interrupted")
  end

  # An unrelated halt must refuse independently of running/interrupted gates.
  def test_unrelated_halt_refuses_second_acknowledgement
    authorize
    second_loss
    path = File.join(@output, "execution.json")
    state = JSON.parse(File.binread(path))
    state["dispatch_halt"] = { "kind" => "worker_registry", "error" => "unrelated fatal source" }
    File.binwrite(path, JSON.generate(state))
    @review = review.merge("job_id" => "second")
    assert_refused_without_mutation("unrelated fatal halt")
  end

  # Existing owner locking refuses B even though every review pin is valid.
  def test_competing_execution_owner_refuses_second_acknowledgement
    authorize
    second_loss
    @store.with_execution_lock { assert_refused_without_mutation("active executor") }
  end

  # Retry authorization is independently protected; it cannot be converted into
  # pending-only continuation by a second loss acknowledgement.
  def test_retry_pending_refuses_second_acknowledgement
    authorize
    @store.authorize_retry!(job_ids: ["invalid"], reason: "synthetic retry selection")
    second_loss
    assert_refused_without_mutation("retry pending")
  end

  # A copied output path cannot reuse the old review's relative metadata pins.
  def test_prior_acknowledgement_is_bound_to_its_output
    authorize
    second_loss
    copied = File.join(@workdir, "copied-output")
    FileUtils.cp_r(@output, copied)
    store = WorkloadOrchestrator::ExecutionStore.new(output_dir: copied, plan: @plan, workdir: @workdir)
    before = protected_hashes
    copied_before = Dir.glob(File.join(copied, "**", "*"), File::FNM_DOTMATCH)
                       .select { |path| File.file?(path) }.to_h { |path| [path, File.binread(path)] }
    bytes = JSON.generate(@review)
    assert_raises(WorkloadOrchestrator::Error) do
      store.acknowledge_worker_loss!(bytes:, sha256: Digest::SHA256.hexdigest(bytes))
    end
    assert_equal before, protected_hashes
    assert(copied_before.all? { |path, content| File.binread(path) == content })
  end

  # The existing crash-window restoration must skip A and restore only B.
  def test_crash_window_restoration_does_not_resurrect_acknowledged_loss
    authorize
    second_loss
    state_path = File.join(@output, "execution.json")
    state = JSON.parse(File.binread(state_path))
    state.delete("dispatch_halt")
    File.binwrite(state_path, JSON.generate(state))
    @store.prepare!
    assert @store.restore_dynamic_worker_loss_halt!
    assert_equal "second", @store.dispatch_halt.fetch("job_id")
    @review = review.merge("job_id" => "second")
    authorize
    refute @store.restore_dynamic_worker_loss_halt!
  end

  # An I/O failure before atomic replacement leaves B wholly unauthorized;
  # after replacement, the full audit/control transition is visible on restart.
  # This tests commit visibility, not power-loss durability after failed fsync.
  def test_second_acknowledgement_has_no_partial_atomic_authorization
    authorize
    second_loss
    original_write = @store.method(:atomic_write)
    state_path = File.join(@output, "execution.json")
    @store.define_singleton_method(:atomic_write) do |path, content|
      raise Errno::EIO, "before atomic replacement" if path == state_path
      original_write.call(path, content)
    end
    assert_refused_without_mutation("before atomic replacement")
    @store.define_singleton_method(:atomic_write) do |path, content|
      original_write.call(path, content)
      raise Errno::EIO, "after atomic replacement" if path == state_path
    end
    assert_raises(WorkloadOrchestrator::Error) { authorize }
    @store = WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan: @plan, workdir: @workdir)
    @store.prepare!
    state = JSON.parse(File.binread(state_path))
    assert_equal 2, state.fetch("worker_loss_acknowledgements").length
    refute state.key?("dispatch_halt")
    refute state.dig("circuit_breaker", "tripped")
    assert authorize.fetch("idempotent")
  end

  # Acknowledgement has no pause authority and an old authorization cannot clear
  # B. Both manual and token-owned holds survive B's audit/control transition.
  def test_sequential_acknowledgements_preserve_pauses_and_later_halt
    authorize
    first_review = @review
    @store.pause!
    pauses = WorkloadOrchestrator::PauseRequests.new(@output)
    pauses.request!("a" * 64)
    second_loss
    second_review = @review
    before = protected_hashes
    @review = first_review
    assert authorize.fetch("idempotent")
    assert_equal before, protected_hashes
    @review = review.merge("job_id" => second_review.fetch("job_id"))
    # B's native loss transition reports infrastructure failure without
    # releasing either pre-existing pause hold.
    state = JSON.parse(File.binread(File.join(@output, "execution.json")))
    assert_equal "infrastructure_failed", state.fetch("status")
    controls = protected_hashes.select { |path, _| path.include?("/control/") }
    authorize
    assert @store.paused?
    assert_equal controls, protected_hashes.select { |path, _| path.include?("/control/") }
    assert_equal "blocked", @store.action_check("run").fetch("disposition")
    runner = WorkloadOrchestrator::Runner.new(
      plan: @plan, workdir: @workdir, output_dir: @output, workers: WorkloadOrchestrator::WorkerSet.new({}),
      worker_source: WorkloadOrchestrator::StaticWorkerSource.new(JSON.generate(@snapshot)),
      command_executor: ->(*) { raise "paused continuation dispatched" }, out: StringIO.new
    )
    assert_equal "paused", runner.run(resume: true, preserve_pauses: true)
    refute_path_exists File.join(@output, "runs/pending/metadata.json")
    assert_equal controls, protected_hashes.select { |path, _| path.include?("/control/") }
  end

  private

  # Compare every protected byte/hash around a refused public authorization.
  def assert_refused_without_mutation(label)
    before = protected_hashes
    assert_raises(WorkloadOrchestrator::Error, label) { authorize }
    assert_equal before, protected_hashes, label
  end

  # Return hashes of every retained output file, including existing lock files.
  def protected_hashes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path, Digest::SHA256.file(path).hexdigest] }
  end

  # Start and reconcile a different original job through the native store and
  # accepted registry, retaining its late result before producing its review.
  def second_loss
    attempt = @store.record_dynamic_running!(job: @plan.jobs[2], worker: @worker, environment_keys: [])
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store: @store).reconcile!(@poller)
    @store.record_dynamic_terminal!(attempt: attempt, status: "complete", exit_status: 0)
    @review = review.merge("job_id" => "second")
  end

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
      "attempts" => attempts, "preserved_files" => files,
      "preserved_directories" => attempts.keys.to_h do |id|
        ["output/runs/#{id}", Dir.children(File.join(@output, "runs", id)).sort]
      end }
  end

  # Invoke the public store operation with the exact review digest.
  def authorize(dry_run: false)
    bytes = JSON.generate(@review)
    @store.acknowledge_worker_loss!(bytes: bytes, sha256: Digest::SHA256.hexdigest(bytes), dry_run: dry_run)
  end
end
