# frozen_string_literal: true

# Explicit offline process gate; excluded from the ordinary suite's runtime
# ceiling. Evidence is retained in the invocation-owned TMPDIR for inspection.
require_relative "../test_helper"
require "digest"
require "timeout"

# Drive two genuine losses in one execution, then resume only original pending
# work. Every prepare, action-check, acknowledgement and runner is a fresh process.
class FoWlo05Acceptance < Minitest::Test
  WLO = File.expand_path("../..", __dir__)
  FIXTURE = File.join(__dir__, "fo_wlo_05/fixture.rb")

  # Create only non-narrative synthetic jobs in an invocation-owned fixture.
  def setup
    @root = File.realpath(Dir.mktmpdir("fo-wlo-05-"))
    @workdir = File.join(@root, "work")
    @output = File.join(@workdir, "output")
    FileUtils.mkdir_p(@workdir)
    @children = {}
    @sequence = 0
    @revision = 0
    @registry = JSON.parse(File.binread(File.join(WLO, "contracts/dynamic-worker-registry/v0.1/minimal-valid.json")))
    @worker = @registry.fetch("workers").first
    @plan_path = File.join(@root, "input.json")
    File.write(@plan_path, JSON.generate(
      "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "fo-wlo-05-offline",
      "failure_policy" => { "max_consecutive_failures" => 1, "max_total_failures" => 1,
                            "non_operational_exit_statuses" => [42] },
      "pools" => [{ "pool_id" => "synthetic", "required_labels" => ["inference"],
                    "requirements" => { "ollama" => { "model" => "qualified-model:latest",
                      "expected_digest" => "a" * 64, "required_context_length" => 131_072 } } }],
      "jobs" => %w[complete domain loss-a loss-b pending].map do |id|
        { "job_id" => id, "pool_id" => "synthetic",
          "argv" => [RbConfig.ruby, FIXTURE, @root, "payload", id] }
      end
    ))
  end

  # Reap only owned processes; retain binary logs, receipts and execution bytes.
  def teardown
    @children.keys.each { |pid| stop_child(pid) }
    warn "FO-WLO-05 evidence: #{@root}"
  end

  # Positive journey proves preservation, fresh-process validation, exact audit,
  # preview, idempotence and dispatch of the original pending job only.
  def test_sequential_public_journey
    journey
  end

  # Removing A's real handoff makes the same composed journey fail at B review.
  def test_missing_prior_handoff_breaks_the_same_journey
    error = assert_raises(Minitest::Assertion) { journey(remove_prior: true) }
    assert_includes error.message, "unreviewed dynamic worker loss remains"
  end

  private

  # Run the continuous journey; only the negative control corrupts store bytes.
  # All positive execution transitions come from real runners/public controls.
  def journey(remove_prior: false)
    publish(true)
    pid = spawn_fixture("run")
    # Retain a genuine completed result and domain failure before either loss;
    # explicitly reset that ordinary failure's breaker in a separate process.
    finish_child(pid)
    assert_equal "complete", metadata("complete").fetch("status")
    assert_equal ["failed", "non_operational"], metadata("domain").values_at("status", "failure_class")
    finish_child(spawn_fixture("reset-breaker"))
    pid = spawn_fixture("resume")
    await { File.file?(File.join(@root, "loss-a.started")) }
    publish(false)
    await { metadata("loss-a").dig("evidence", "kind") == "dynamic_worker_loss_in_doubt" }
    File.write(File.join(@root, "loss-a.release"), "late result\n")
    finish_child(pid)
    assert_loss("loss-a")
    preserved = retained_hashes
    review_a = review("loss-a")
    acknowledge(review_a)
    assert_equal preserved, retained_hashes
    fresh_prepare
    assert_equal "execute", action_check.fetch("disposition")

    publish(true)
    pid = spawn_fixture("resume")
    await { File.file?(File.join(@root, "loss-b.started")) }
    publish(false)
    await { metadata("loss-b").dig("evidence", "kind") == "dynamic_worker_loss_in_doubt" }
    File.write(File.join(@root, "loss-b.release"), "late result\n")
    finish_child(pid)
    assert_loss("loss-b")
    assert_equal preserved, retained_hashes.select { |path, _| preserved.key?(path) }
    before = tree_hashes
    assert_equal "blocked", action_check.fetch("disposition")
    finish_child(spawn_fixture("resume"), success: false)
    assert_equal before, tree_hashes
    acknowledge(review_a, idempotent: true)
    assert_equal before, tree_hashes
    fresh_prepare
    assert_equal "loss-b", execution.dig("dispatch_halt", "job_id")
    if remove_prior
      state = execution
      state.delete("worker_loss_acknowledgements")
      File.write(File.join(@output, "execution.json"), JSON.generate(state))
    end

    review_b = review("loss-b")
    before = tree_hashes
    File.open(File.join(@output, ".execution.lock"), File::RDONLY) do |owner|
      assert owner.flock(File::LOCK_EX | File::LOCK_NB)
      acknowledge(review_b, refused: true)
    end
    assert_equal before, tree_hashes
    acknowledge(review_b, dry_run: true)
    assert_equal before, tree_hashes
    preserved = retained_hashes
    prior = execution
    event = acknowledge(review_b)
    assert_equal prior.fetch("dispatch_halt"), event.fetch("halt_before")
    assert_equal prior.fetch("circuit_breaker"), event.fetch("breaker_before")
    assert_equal 2, execution.fetch("worker_loss_acknowledgements").length
    assert_equal prior.dig("circuit_breaker", "history"), execution.dig("circuit_breaker", "history")
    assert_equal prior.dig("circuit_breaker", "generation") + 1, execution.dig("circuit_breaker", "generation")
    refute execution.dig("circuit_breaker", "tripped")
    refute execution.key?("dispatch_halt")
    assert_equal "pending", execution.fetch("status")
    assert_equal preserved, retained_hashes
    refute_path_exists File.join(@root, "pending.started")
    fresh_prepare
    assert_equal "execute", action_check.fetch("disposition")
    before = tree_hashes
    acknowledge(review_b, idempotent: true)
    assert_equal before, tree_hashes
    publish(true)
    finish_child(spawn_fixture("resume"))
    assert_equal "complete", metadata("pending").fetch("status")
    assert_equal preserved, retained_hashes.select { |path, _| preserved.key?(path) }
    %w[loss-a loss-b].each { |id| assert_loss(id) }
    assert_equal "workload_failed", execution.fetch("status")
    jobs = JSON.parse(File.binread(File.join(@output, "jobs.json"))).fetch("jobs")
    assert_equal({ "complete" => 2, "failed" => 3 }, jobs.map { |job| job.fetch("status") }.tally)
    refute execution.key?("retry_pending")
    refute_path_exists File.join(@output, "attempts")
    File.binwrite(File.join(@output, "runs/loss-a/stdout.log"), "changed")
    before = tree_hashes
    fresh_prepare(success: false)
    assert_equal "blocked", action_check.fetch("disposition")
    assert_equal before, tree_hashes
  end

  # Publish an atomic synthetic registry with monotonic revision/time. No WLO
  # state is edited; one worker limits the scheduler to one actual payload.
  def publish(ready)
    @revision += 1
    document = @registry.merge("revision" => @revision,
      "published_at" => (Time.utc(2030) + @revision).iso8601, "workers" => ready ? [@worker] : [])
    path = File.join(@root, "registry.json")
    File.binwrite("#{path}.tmp", JSON.generate(document))
    File.rename("#{path}.tmp", path)
  end

  # Generate the existing finite review format over actual stopped native bytes.
  # Review files live outside the workdir whose digest they pin.
  def review(id)
    plan = WorkloadOrchestrator::Plan.load(@plan_path)
    store = WorkloadOrchestrator::ExecutionStore.new(plan:, workdir: @workdir, output_dir: @output)
    files = retained_hashes.transform_keys { |path| "output/#{path}" }
    attempts = plan.jobs.filter_map do |job|
      digest = files["output/runs/#{job.id}/metadata.json"]
      [job.id, digest] if digest
    end.to_h
    directories = attempts.keys.to_h { |job_id| ["output/runs/#{job_id}", Dir.children(File.join(@output, "runs", job_id)).sort] }
    bytes = JSON.generate("contract_version" => "wlo-worker-loss-review/v0.1",
      "action" => "acknowledge_loss_without_retry", "operator" => "offline synthetic operator",
      "reason" => "retain in-doubt failure; permit a separate pending-only resume",
      "plan_id" => plan.id, "plan_sha256" => plan.sha256, "workdir" => @workdir,
      "job_id" => id, "attempt" => 1, "attempts" => attempts,
      "execution_sha256" => Digest::SHA256.file(File.join(@output, "execution.json")).hexdigest,
      "jobs_sha256" => Digest::SHA256.file(File.join(@output, "jobs.json")).hexdigest,
      "workdir_evidence_sha256" => store.send(:evidence_sha256, @workdir),
      "preserved_files" => files, "preserved_directories" => directories)
    path = File.join(@root, "#{id}.review.json")
    File.binwrite(path, bytes)
    [path, Digest::SHA256.hexdigest(bytes)]
  end

  # Invoke the actual public CLI in a fresh process; return its exact audit.
  def acknowledge(review, dry_run: false, idempotent: false, refused: false)
    args = ["acknowledge-worker-loss", @plan_path, review.first, "--workdir", @workdir,
            "--output", @output, "--review-sha256", review.last]
    args << "--dry-run" if dry_run
    stdout = cli(*args, success: !refused)
    return if refused

    JSON.parse(stdout).tap do |event|
      assert event.fetch("dry_run") if dry_run
      assert event.fetch("idempotent") if idempotent
    end
  end

  # Read public admissibility in a fresh process, without preparation or writes.
  def action_check
    JSON.parse(cli("action-check", @plan_path, "--workdir", @workdir, "--output", @output,
                   "--action", "resume", "--json"))
  end

  # Exercise native preparation in a fresh process; changed evidence must fail.
  def fresh_prepare(success: true)
    finish_child(spawn_fixture("prepare"), success:)
  end

  # Execute an exact CLI argv and return retained binary stdout after success.
  def cli(*args, success: true)
    pid = spawn_child([RbConfig.ruby, File.join(WLO, "bin/wlo"), *args])
    finish_child(pid, success:)
  end

  # Start a bounded native runner/preparer process in the synthetic root.
  def spawn_fixture(operation)
    spawn_child([RbConfig.ruby, FIXTURE, @root, operation])
  end

  # Isolate environment, retain binary streams and exact argv, and track only
  # this test's children. Independent fixture/payload deadlines survive us.
  def spawn_child(argv)
    @sequence += 1
    prefix = File.join(@root, "process-#{@sequence}")
    environment = { "PATH" => ENV.fetch("PATH"), "TMPDIR" => File.dirname(@root) }
    pid = Process.spawn(environment, *argv, unsetenv_others: true, pgroup: true,
                        out: "#{prefix}.stdout", err: "#{prefix}.stderr")
    @children[pid] = [prefix, argv]
    pid
  end

  # Wait at most eight seconds, record raw status and preserve both byte streams.
  # Unexpected success/failure raises an assertion with inspect-safe diagnostics.
  def finish_child(pid, success: true)
    prefix, argv = @children.fetch(pid)
    status = Timeout.timeout(8) { Process.waitpid2(pid).last }
    @children.delete(pid)
    File.write("#{prefix}.json", JSON.generate("argv" => argv, "exitstatus" => status.exitstatus, "termsig" => status.termsig))
    stdout, stderr = %w[stdout stderr].map { |channel| File.binread("#{prefix}.#{channel}") }
    assert_equal success, status.success?, "#{argv.inspect}: #{stdout.inspect} #{stderr.inspect}"
    stdout
  ensure
    stop_child(pid) if @children.key?(pid)
  end

  # Terminate and reap only an unreaped child owned by this test.
  def stop_child(pid)
    Process.kill("TERM", pid)
    begin
      Timeout.timeout(2) { Process.waitpid(pid) }
    rescue Timeout::Error
      Process.kill("KILL", pid)
      Process.waitpid(pid)
    end
    @children.delete(pid)
  end

  # Wait for an external observation with a strict independent deadline.
  def await
    Timeout.timeout(5) do
      until yield
        sleep 0.005
      end
    end
  end

  # Return native metadata, or an empty object before its first atomic write.
  def metadata(id)
    path = File.join(@output, "runs", id, "metadata.json")
    File.file?(path) ? JSON.parse(File.binread(path)) : {}
  end

  # Return the native execution audit without changing it.
  def execution
    JSON.parse(File.binread(File.join(@output, "execution.json")))
  end

  # Prove terminal failure, original binding/attempt and retained binary result.
  def assert_loss(id)
    document = metadata(id)
    assert_equal ["failed", 1, false], [document["status"], document["attempt"], document.dig("evidence", "outcome_known")]
    assert_equal "dynamic_worker_loss_in_doubt", document.dig("evidence", "kind")
    assert_equal 1, document.fetch("late_evidence").length
    assert_equal "complete", document.dig("late_evidence", 0, "status")
    assert_equal @worker.fetch("generation_id"), document.dig("worker_execution_identity", "generation_id")
    assert_equal document.fetch("worker_execution_identity"), document.dig("evidence", "worker_execution_identity")
    assert_equal document.fetch("worker_registry_binding"), document.dig("evidence", "worker_registry_binding")
    binding = document.fetch("worker_registry_binding")
    publication = Dir.glob(File.join(@output, "dynamic-workers/observations/*/stdout.bin")).find do |path|
      Digest::SHA256.file(path).hexdigest == binding.fetch("registry_snapshot_sha256")
    end
    refute_nil publication
    accepted = JSON.parse(File.binread(publication))
    assert_equal binding.fetch("registry_revision"), accepted.fetch("revision")
    assert_equal @worker, accepted.fetch("workers").first
    assert_equal "out\x00\xff".b, File.binread(File.join(@output, "runs", id, "stdout.log"))
    assert_equal "err\x00\xfe".b, File.binread(File.join(@output, "runs", id, "stderr.log"))
  end

  # Hash frozen plan and every existing attempt/log; future jobs may add files.
  def retained_hashes
    tree_hashes.select { |path, _| path == "plan.json" || path.start_with?("runs/") }
  end

  # Hash the complete protected output tree for read-only/refusal comparisons.
  def tree_hashes
    Dir.glob(File.join(@output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix("#{@output}/"), Digest::SHA256.file(path).hexdigest] }
  end
end
