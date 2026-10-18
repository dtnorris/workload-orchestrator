# frozen_string_literal: true

# Explicit composed gate, outside the ordinary suite. Uses real read-only
# producer roots and native WLO CLI/command sources; no network or inference.
require_relative "../test_helper"
require "open3"
require "timeout"
require_relative "fo24a/offline_guard"

# One uninterrupted manager: adopt/offer both sources, hold work, selectively
# drain local, finish its original attempt, continue remote, deliberately re-offer.
class FoWlo03Acceptance < Minitest::Test
  WLO = File.expand_path("../..", __dir__)
  SUPPORT = File.join(__dir__, "fo_wlo_03")
  PINNED = {"local" => ["local-ollama-workers", "7a169b788fcff0d6fa4c2a41fe6349abb23b703e"],
            "remote" => ["runpod-ollama-fleet", "8ae771fef266668816231209f4ac326fadfe4bda"]}.freeze

  # Own a disposable fixture and retain diagnostics/evidence after the gate.
  def setup
    @root = File.realpath(Dir.mktmpdir("fo-wlo-03-"))
    @observations = []
    @pid = nil
    @status = nil
    @roots = PINNED.to_h do |name, (repository, revision)|
      path = File.expand_path(ENV.fetch("FO03_#{name.upcase}_ROOT", File.join(File.dirname(WLO), repository)))
      # Shell-free, read-only provenance, explicitly allowed by the offline guard.
      argv = ["git", "-C", path, "rev-parse", "HEAD"]
      bytes, error, status = Fo24aOfflineGuard.permit(argv) { Open3.capture3(*argv) }
      raise "unpinned #{repository}: #{bytes} #{error}" unless status.success? && bytes.strip == revision
      [name, path]
    end
    write_fixture
    seed_retained_attempts
  end

  # Signal/reap only the unreaped child started here. WLO cancels its own child
  # groups on TERM; payloads and publishers also have independent deadlines.
  def teardown
    if @pid
      Process.kill("TERM", @pid)
      begin
        @status = Timeout.timeout(3) { Process.waitpid2(@pid).last }
      rescue Timeout::Error
        Process.kill("KILL", @pid)
        @status = Process.waitpid2(@pid).last
      end
      @pid = nil
    end
    File.write(File.join(@root, "observations.json"), JSON.pretty_generate(@observations))
    File.write(File.join(@root, "child-status.json"), JSON.generate("exitstatus" => @status&.exitstatus, "termsig" => @status&.termsig))
    warn "FO-WLO-03 evidence: #{@root}"
  end

  # Positive continuous journey requires every real publication/control handoff.
  def test_continuous_offer_drain_remote_progress_and_reoffer
    journey
  end

  # The very same journey must fail at its source exclusion assertion when both
  # real consent checks are deliberately bypassed in the disposable manager.
  def test_critical_boundary_bypass_is_detected
    File.write(File.join(@root, "bypass-exclusion"), "negative control\n")
    error = assert_raises(Minitest::Assertion) { journey }
    assert_includes error.message, "excluded source dispatched"
  end

  private

  # Execute real controls against a fresh CLI object, preserving every response.
  def control(action, name = "local", *extra)
    out, err = StringIO.new, StringIO.new
    code = WorkloadOrchestrator::CLI.new(["source-#{action}", "--output", @output, "--source", name, *extra], out:, err:).run
    assert_equal 0, code, err.string
    JSON.parse(out.string).tap { |row| @observations << {"action" => action, "row" => row} }
  end

  # Public JSON status is the readiness predicate; bounded waits retain failures.
  def await(description)
    Timeout.timeout(8) do
      loop do
        result = yield
        return result if result
        sleep 0.02
      end
    end
  rescue Timeout::Error
    flunk "timeout #{description}; #{diagnostics}"
  end

  # Read public source/execution status without manually interpreting controls.
  def report
    out, err = StringIO.new, StringIO.new
    code = WorkloadOrchestrator::CLI.new(["status", @plan_path, "--output", @output, "--json"], out:, err:).run
    assert_equal 0, code, err.string
    JSON.parse(out.string)
  end

  # Observe real job state through public status and independently started payload.
  def await_job(id, status)
    await("#{id} #{status}") do
      row = report.fetch("jobs").find { |job| job.fetch("job_id") == id }
      row if row["status"] == status && (status != "running" || File.file?(File.join(@root, "#{id}.started")))
    end
  end

  # Release only a synthetic payload; leave all WLO evidence to its owner.
  def release(id)
    File.write(File.join(@root, "#{id}.release"), "release\n")
  end

  # Preserve exact metadata/log/outcome/history bytes, including binary output.
  def evidence(id)
    Dir.glob(File.join(@output, "runs", id, "**", "*")).select { |path| File.file?(path) }
       .to_h { |path| [path.delete_prefix(@output), Digest::SHA256.file(path).hexdigest] }
  end

  # Match immutable attempts to exact bytes emitted by the actual publisher.
  def assert_binding(id, name)
    metadata = JSON.parse(File.binread(File.join(@output, "runs", id, "metadata.json")))
    binding = metadata.fetch("worker_registry_binding")
    publications = File.readlines(File.join(@root, name, "publications.jsonl"), chomp: true)
    bytes = publications.find { |row| Digest::SHA256.hexdigest(row) == binding.fetch("registry_snapshot_sha256") }
    refute_nil bytes, "missing actual publisher-to-WLO handoff"
    document = JSON.parse(bytes)
    worker = document.fetch("workers").first
    assert_equal worker.slice("worker_id", "generation_id", "endpoint", "capability_fingerprint")
      .merge("registry_id" => document.fetch("registry_id")), metadata.fetch("worker_execution_identity")
    assert_equal document.fetch("revision"), binding.fetch("registry_revision")
    assert_equal worker.fetch("capabilities"), metadata.dig("worker_snapshot", "capabilities")
    assert_equal 1, metadata.fetch("attempt")
    assert_equal metadata.dig("worker_execution_identity", "endpoint"),
      JSON.parse(File.read(File.join(@root, "#{id}.started"))).fetch("endpoint")
    metadata
  end

  # Drive the operator journey in one manager without pause, retry or restart.
  def journey
    launch
    await("registered source authority") { File.file?(File.join(@output, "source-offers.json")) }
    %w[local remote].each do |name|
      assert_equal "legacy/unmanaged", control("status", name).fetch("mode")
      control("adopt", name)
      refute control("status", name).fetch("offer_eligible")
      control("offer", name, "--duration", "120")
      File.write(File.join(@root, name, "available"), "synthetic observation\n")
    end
    await_job("local-held", "running")
    await_job("remote-held", "running")
    original = assert_binding("local-held", "local")
    remote = assert_binding("remote-held", "remote")
    control("drain")
    draining = await("effective exclusion while inference runs") do
      row = control("status")
      row if row["effective"] && row["drain_result"] == "draining"
    end
    assert_equal 1, draining.fetch("active_attempt_count")
    assert draining.fetch("authoritative_boundary")
    assert draining.fetch("inference_running")
    fresh = fresh_source_status
    assert_equal draining.fetch("execution_id"), fresh.fetch("execution_id")
    assert fresh.fetch("effective")
    release("local-held")
    await_job("local-held", "complete")
    local_evidence = evidence("local-held")
    drained = await("fully drained WLO-owned local work") do
      row = control("status")
      row if row["drain_result"] == "fully_drained"
    end
    assert_equal 0, drained.fetch("active_attempt_count")
    release("remote-held")
    await_job("remote-held", "complete")
    remote_evidence = evidence("remote-held")
    await_job("remote-later", "running")
    assert_binding("remote-later", "remote")
    release("remote-later")
    await_job("remote-later", "complete")
    count = File.readlines(File.join(@root, "local", "publications.jsonl")).length
    await("later real READY publications while local remains excluded") do
      File.readlines(File.join(@root, "local", "publications.jsonl")).length >= count + 2
    end
    assert_equal "pending", report.fetch("jobs").find { |job| job["job_id"] == "local-later" }.fetch("status"), "excluded source dispatched"
    refute_path_exists File.join(@root, "local-later.started"), "excluded source dispatched"
    assert report.fetch("executor_active")
    control("offer", "local", "--duration", "120")
    await_job("local-later", "running")
    assert_binding("local-later", "local")
    release("local-later")
    @status = Timeout.timeout(8) { Process.waitpid2(@pid).last }
    @pid = nil
    assert_equal 2, @status.exitstatus, diagnostics
    assert_equal local_evidence, evidence("local-held")
    assert_equal remote_evidence, evidence("remote-held")
    assert_equal original.fetch("worker_execution_identity"), assert_binding("local-held", "local").fetch("worker_execution_identity")
    assert_equal remote.fetch("worker_execution_identity"), assert_binding("remote-held", "remote").fetch("worker_execution_identity")
    assert_equal @plan_bytes, File.binread(File.join(@output, "plan.json"))
    assert_equal @source_bytes, File.binread(File.join(@root, "sources.yml"))
    assert_equal @retained_attempts, retained_attempts
    state = JSON.parse(File.binread(File.join(@output, "execution.json")))
    assert_equal @retry_history, state.fetch("retry_history")
    assert_equal({ "retained-failed" => 1 }, state.fetch("retry_pending", {}))
    retried = JSON.parse(File.binread(File.join(@output, "runs", "retained-failed", "metadata.json")))
    assert_equal 2, retried.fetch("attempt")
    assert_equal "failed", retried.fetch("status")
    assert_equal "synthetic local-held\x00\xFF\n".b, File.binread(File.join(@output, "runs", "local-held", "stdout.log"))
    refute control("status").fetch("effective"), "dead owner acknowledgment must be historical"
  end

  # Run the real CLI in one owned process with an empty inherited environment;
  # exact argv guard and independent runtime limits survive test assertion errors.
  def launch
    argv = [RbConfig.ruby, File.join(SUPPORT, "manager.rb"), WLO, @root]
    environment = {"PATH" => File.dirname(RbConfig.ruby), "TMPDIR" => @root,
      "RUBYOPT" => "-r#{File.join(__dir__, 'fo24a/offline_guard.rb')}"}
    @pid = Fo24aOfflineGuard.permit(argv) do
      Process.spawn(environment, *argv, unsetenv_others: true, pgroup: true,
        out: File.join(@root, "manager.stdout"), err: File.join(@root, "manager.stderr"))
    end
  end

  # Prove a new OS process reconstructs the same acknowledgment through public
  # CLI output, with no inherited credentials or publisher invocation.
  def fresh_source_status
    argv = [RbConfig.ruby, File.join(WLO, "bin/wlo"), "source-status", "--output", @output, "--source", "local"]
    environment = {"PATH" => File.dirname(RbConfig.ruby), "TMPDIR" => @root,
      "RUBYOPT" => "-r#{File.join(__dir__, 'fo24a/offline_guard.rb')}"}
    out, err, status = Fo24aOfflineGuard.permit(argv) { Open3.capture3(environment, *argv, unsetenv_others: true) }
    assert_equal 0, status.exitstatus, err
    JSON.parse(out).tap { |row| @observations << {"action" => "fresh-process-status", "row" => row} }
  end

  # Build historical terminal and explicitly retried evidence using the actual
  # LOW publisher and public store APIs. No synthetic payload is run here.
  def seed_retained_attempts
    directory = File.join(@root, "local")
    File.write(File.join(directory, "available"), "synthetic observation\n")
    argv = [RbConfig.ruby, File.join(SUPPORT, "publisher.rb"), "local", @roots.fetch("local"), directory]
    environment = {"PATH" => File.dirname(RbConfig.ruby), "TMPDIR" => @root,
      "RUBYOPT" => "-r#{File.join(__dir__, 'fo24a/offline_guard.rb')}"}
    bytes, error, status = Fo24aOfflineGuard.permit(argv) { Open3.capture3(environment, *argv, unsetenv_others: true) }
    raise "retained publisher failed: #{error}" unless status.success?

    worker = WorkloadOrchestrator::DynamicWorkerRegistry.new(bytes, now: Time.now.utc).entries.first
    plan = WorkloadOrchestrator::Plan.load(@plan_path)
    store = WorkloadOrchestrator::ExecutionStore.new(plan:, workdir: @root, output_dir: @output).prepare!
    plan.jobs.first(2).each_with_index do |job, index|
      attempt = store.record_dynamic_running!(job:, worker:, environment_keys: [])
      store.write_logs(job, "retained #{job.id}\x00\xFF\n".b, "")
      store.record_dynamic_terminal!(attempt:, status: index.zero? ? "complete" : "failed", exit_status: index.zero? ? 0 : 7)
    end
    store.retry_failed!(reason: "synthetic prior operator retry", job_ids: ["retained-failed"])
    attempt = store.record_dynamic_running!(job: plan.jobs.fetch(1), worker:, environment_keys: [])
    store.write_logs(plan.jobs.fetch(1), "retained retry failure\x00\xFF\n".b, "")
    store.record_dynamic_terminal!(attempt:, status: "failed", exit_status: 7)
    @retained_attempts = retained_attempts
    @retry_history = JSON.parse(File.binread(File.join(@output, "execution.json"))).fetch("retry_history")
    File.delete(File.join(directory, "available"))
  end

  # Preserve all original current and archived attempt bytes, including retries.
  def retained_attempts
    paths = %w[retained-ok retained-failed].flat_map do |id|
      Dir.glob(File.join(@output, "{runs,attempts}", id, "**", "*"))
    end
    paths.select { |path| File.file?(path) }.to_h do |path|
      [path.delete_prefix(@output), Digest::SHA256.file(path).hexdigest]
    end
  end

  # Write fake external observations and synthetic plan only. No registry JSON
  # is authored here: every accepted snapshot comes from its actual publisher.
  def write_fixture
    @output = File.join(@root, "execution")
    @plan_path = File.join(@root, "input.json")
    sources = @roots.map do |name, component|
      directory = File.join(@root, name)
      FileUtils.mkdir_p(directory)
      {"name" => name, "policy" => "optional", "command" => RbConfig.ruby,
       "args" => [File.join(SUPPORT, "publisher.rb"), name, component, directory],
       "environment" => {}, "workdir" => @root}
    end
    @source_bytes = YAML.dump("contract_version" => "wlo-worker-sources/v0.2", "sources" => sources)
    File.write(File.join(@root, "sources.yml"), @source_bytes)
    jobs = [["retained-ok", "local", []], ["retained-failed", "local", []],
            ["local-held", "local", []], ["remote-held", "remote", []],
            ["remote-later", "remote", ["remote-held"]], ["local-later", "local", %w[remote-later local-held]]]
    @plan_bytes = JSON.generate("contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "fo-wlo-03-offline",
      "failure_policy" => {"max_consecutive_failures" => 10, "max_total_failures" => 10},
      "pools" => %w[local remote].map do |name|
        {"pool_id" => name, "required_labels" => [name], "requirements" => {"ollama" => {
          "model" => "qualified:latest", "expected_digest" => "a" * 64,
          "required_context_length" => 131_072, "require_fully_gpu_resident" => true}}}
      end,
      "jobs" => jobs.map do |id, pool, dependencies|
        {"job_id" => id, "pool_id" => pool, "depends_on_job_ids" => dependencies,
         "argv" => [RbConfig.ruby, File.join(SUPPORT, "job.rb"), @root, id]}
      end)
    File.write(@plan_path, @plan_bytes)
  end

  # Preserve raw binary stdout/stderr files; show a short inspect-safe diagnostic.
  def diagnostics
    %w[manager.stdout manager.stderr].map do |name|
      path = File.join(@root, name)
      "#{name}=#{File.file?(path) ? File.binread(path).inspect : 'not started'}"
    end.join("\n")
  end
end
