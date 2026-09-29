# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

class RemoteRunnerTest < Minitest::Test
  class FakeLifecycle
    attr_reader :checks

    def initialize
      @checks = 0
    end

    def check!
      @checks += 1
      { "state" => "ARMED" }
    end
  end

  class FakeCapacitySession
    attr_accessor :output_dir, :cleanup_phase
    attr_reader :calls, :outcomes, :lifecycle, :admission_calls
    attr_accessor :admission_error

    def initialize(handoff)
      @handoff = handoff
      @calls = []
      @outcomes = []
      @admission_calls = 0
      @lifecycle = FakeLifecycle.new
    end

    def with_capacity(authorize_paid:, resume:)
      @calls << { authorize_paid: authorize_paid, resume: resume }
      outcome = yield({ "remote-pool" => @handoff }, @lifecycle)
      @outcomes << outcome
      if output_dir
        root = File.join(output_dir, "capacity")
        FileUtils.mkdir_p(root)
        path = File.join(root, "session.json")
        if File.file?(path)
          FileUtils.mkdir_p(File.join(root, "sessions"))
          path = File.join(root, "sessions", "session-#{@outcomes.length}.json")
        end
        File.write(path, JSON.generate("disposition" => {
          "phase" => @cleanup_phase || (outcome.retain_capacity ? "retained_for_pause" : "verified_provider_absence")
        }))
      end
      raise WorkloadOrchestrator::Error, "cleanup was not verified" if @cleanup_phase
      outcome.value
    end

    def admit_worker(pool_id:, handoff:, lifecycle:)
      @admission_calls += 1
      raise @admission_error if @admission_error
      handoff.merge("target" => handoff.fetch("target").merge("worker_indices" => [1, 2]),
                    "bootstrap_samples_seconds" => [0.001, 0.001])
    end
  end

  class AlwaysUseful
    def evaluate(**inputs)
      { "expand" => inputs.fetch(:unclaimed).positive?, "reason" => "fixture_useful" }
    end
  end

  class FakeClient
    attr_accessor :modes, :started, :release, :delay
    attr_reader :requests

    def initialize(modes = {})
      @modes = modes
      @requests = []
    end

    def dispatch(request:, workdir:, output_dir:, timeout_seconds:)
      job = request.fetch("jobs").fetch(0)
      @requests << request
      @started << job.fetch("job_id") if @started
      @release.pop if @release
      sleep(@delay) if @delay
      mode = @modes.fetch(job.fetch("job_id"), :complete)
      raise WorkloadOrchestrator::Error, "simulated transport loss" if mode == :transport

      FileUtils.mkdir_p(File.join(output_dir, "jobs", job.fetch("job_id")))
      File.write(File.join(output_dir, "jobs", job.fetch("job_id"), "stdout.log"), "remote stdout\n")
      File.write(File.join(output_dir, "jobs", job.fetch("job_id"), "stderr.log"), "remote stderr\n")
      failed = mode == :workload
      infrastructure = mode == :infrastructure
      row = unless infrastructure
              {
                "job_id" => job.fetch("job_id"), "status" => failed ? "failed" : "completed",
                "exit_status" => failed ? 7 : 0,
                "stdout_path" => File.join("jobs", job.fetch("job_id"), "stdout.log"),
                "stderr_path" => File.join("jobs", job.fetch("job_id"), "stderr.log")
              }
            end
      document = {
        "contract_version" => WorkloadOrchestrator::RpofContract::DISPATCH_SUMMARY,
        "fleet_key" => request.dig("target", "fleet_key"), "fleet_id" => "fleet-1",
        "worker_indices" => request.dig("target", "worker_indices"),
        "status" => infrastructure ? "infrastructure_failed" : failed ? "workload_failed" : "completed",
        "job_count" => 1, "completed_count" => failed || infrastructure ? 0 : 1,
        "failed_count" => failed ? 1 : 0, "not_started_count" => infrastructure ? 1 : 0,
        "not_started_job_ids" => infrastructure ? [job.fetch("job_id")] : [],
        "jobs" => row ? [row] : []
      }
      WorkloadOrchestrator::RpofClient::Result.new(
        document: document, exit_status: document["status"] == "completed" ? 0 : 1,
        stdout: "client stdout\n", stderr: "client stderr\n"
      )
    end
  end

  def setup
    @root = Dir.mktmpdir("wlo-remote-runner-")
    @workdir = File.join(@root, "work")
    @output = File.join(@root, "output")
    FileUtils.mkdir_p(@workdir)
    @plan = remote_plan(%w[first second third])
    @workers = WorkloadOrchestrator::WorkerSet.new({})
    @handoff = {
      "target" => { "fleet_key" => "pool-handle", "expected_fleet_id" => "fleet-1", "worker_indices" => [1] },
      "deadline_at_utc" => (Time.now.utc + 60).iso8601
    }
  end

  def teardown
    FileUtils.remove_entry(@root)
  end

  def test_wlo_owns_remote_attempts_logs_jobs_and_explicit_failed_retry
    client = FakeClient.new("second" => :workload)
    session = FakeCapacitySession.new(@handoff)
    assert_equal "workload_failed", runner(client, session).run
    assert_equal %w[first second third], client.requests.map { |request| request.dig("jobs", 0, "job_id") }
    assert client.requests.all? { |request| request.fetch("jobs").length == 1 }
    failed = metadata("second")
    assert_equal "failed", failed.fetch("status")
    assert_equal 7, failed.fetch("exit_status")
    assert_includes File.read(File.join(@output, "runs", "second", "stdout.log")), "remote stdout"
    refute session.outcomes.first.retain_capacity

    store.retry_failed!(all: false, job_ids: ["second"], reason: "fixed", acknowledge_circuit_breaker: false)
    client.modes = {}
    assert_equal "completed", runner(client, session).run(resume: true)
    assert_equal 2, metadata("second").fetch("attempt")
    assert File.directory?(File.join(@output, "attempts", "second", "attempt-1", "provider-attempt-1"))
    assert_equal false, session.outcomes.last.retain_capacity
    assert_equal [false, true], session.calls.map { |call| call.fetch(:resume) }
  end

  def test_infrastructure_failure_stops_dispatch_and_requires_explicit_retry
    client = FakeClient.new("first" => :infrastructure)
    session = FakeCapacitySession.new(@handoff)
    assert_equal "infrastructure_failed", runner(client, session).run
    assert_equal ["first"], client.requests.map { |request| request.dig("jobs", 0, "job_id") }
    assert_raises(WorkloadOrchestrator::Error) { runner(client, session).run(resume: true) }

    store.retry_failed!(all: false, job_ids: ["first"], reason: "transport repaired",
                        acknowledge_circuit_breaker: false)
    client.modes = {}
    assert_equal "completed", runner(client, session).run(resume: true)
    assert_equal({ "complete" => 3 }, compact_counts)
  end

  def test_transport_failure_stops_dispatch_and_retains_in_doubt_evidence
    client = FakeClient.new("first" => :transport)
    session = FakeCapacitySession.new(@handoff)

    assert_equal "infrastructure_failed", runner(client, session).run
    assert_equal ["first"], client.requests.map { |request| request.dig("jobs", 0, "job_id") }
    assert_equal "remote_in_doubt", metadata("first").dig("evidence", "kind")
    assert_includes metadata("first").fetch("error"), "simulated transport loss"
    refute session.outcomes.first.retain_capacity
  end

  def test_completed_jobs_do_not_report_success_when_terminal_cleanup_is_unverified
    client = FakeClient.new
    session = FakeCapacitySession.new(@handoff)
    session.cleanup_phase = "in_progress"

    assert_equal "cleanup_failed", runner(client, session).run
    assert_equal({ "complete" => 3 }, compact_counts)
    execution = JSON.parse(File.read(File.join(@output, "execution.json")))
    assert_equal "completed", execution.fetch("workload_status")
    assert_equal "in_progress", execution.dig("resource_disposition", "phase")
    assert_equal "cleanup_failed", execution.fetch("status")
  end

  def test_pause_drains_active_remote_job_and_resume_uses_same_capacity_scope
    client = FakeClient.new
    client.started = Queue.new
    client.release = Queue.new
    session = FakeCapacitySession.new(@handoff)
    subject = runner(client, session)
    thread = Thread.new { subject.run }
    assert_equal "first", client.started.pop
    subject.store.pause!
    client.release << true

    assert_equal "paused", thread.value
    assert_equal ["first"], client.requests.map { |request| request.dig("jobs", 0, "job_id") }
    client.release = nil
    assert_equal "completed", runner(client, session).run(resume: true)
    assert_equal({ "complete" => 3 }, compact_counts)
  end

  def test_interruption_stops_new_dispatch_drains_active_and_requests_terminal_cleanup
    client = FakeClient.new
    client.started = Queue.new
    client.release = Queue.new
    session = FakeCapacitySession.new(@handoff)
    subject = runner(client, session)
    thread = Thread.new { subject.run }
    assert_equal "first", client.started.pop
    subject.instance_variable_set(:@interrupt_signal, "TERM")
    client.release << true

    assert_equal "interrupted", thread.value
    assert_equal ["first"], client.requests.map { |request| request.dig("jobs", 0, "job_id") }
    refute session.outcomes.first.retain_capacity
    assert_equal "interrupted", store.status
    assert_equal "TERM", JSON.parse(File.read(File.join(@output, "execution.json"))).dig("interruption", "signal")
    assert_equal "verified_provider_absence", JSON.parse(File.read(File.join(@output, "execution.json")))
                                               .dig("resource_disposition", "phase")
  end

  def test_stale_remote_running_attempt_becomes_in_doubt_and_is_not_replayed
    subject = runner(FakeClient.new, FakeCapacitySession.new(@handoff))
    subject.store.prepare!
    worker = WorkloadOrchestrator::Runner::RemoteWorker.new(name: "rpof:remote-pool:burst_1", index: 1)
    subject.store.record_running!(job: @plan.jobs.first, worker: worker, environment_keys: [])
    client = FakeClient.new
    session = FakeCapacitySession.new(@handoff)

    error = assert_raises(WorkloadOrchestrator::Error) { runner(client, session).run(resume: true) }
    assert_includes error.message, "explicitly retry"
    assert_empty client.requests
    assert_equal "remote_in_doubt", metadata("first").dig("evidence", "kind")
  end

  def test_admitted_worker_receives_unclaimed_jobs_and_decisions_are_durable
    @plan = remote_plan(%w[first second third fourth], desired: 2)
    client = FakeClient.new
    client.delay = 0.002
    client.started = Queue.new
    client.release = Queue.new
    session = FakeCapacitySession.new(@handoff)
    task = Thread.new { runner(client, session, admission_policy: AlwaysUseful.new).run }
    assert_equal "first", client.started.pop
    client.release << true
    assert_equal "second", client.started.pop
    third = client.started.pop
    assert_includes %w[third fourth], third
    4.times { client.release << true }
    assert_equal "completed", task.value
    assert_includes client.requests.map { |row| row.dig("target", "worker_indices") }, [2]
    decisions = File.readlines(File.join(@output, "worker-admissions.jsonl")).map { |line| JSON.parse(line) }
    assert decisions.any? { |row| row.dig("decision", "reason") == "admitted" }
  end

  def test_failed_optional_admission_disables_expansion_but_completes_existing_queue
    @plan = remote_plan(%w[first second third fourth fifth], desired: 2, concurrency: 2)
    client = FakeClient.new
    client.delay = 0.002
    client.started = Queue.new
    client.release = Queue.new
    session = FakeCapacitySession.new(@handoff)
    session.admission_error = WorkloadOrchestrator::Error.new("capacity fixture failed")
    task = Thread.new { runner(client, session, admission_policy: AlwaysUseful.new).run }
    assert_equal "first", client.started.pop
    client.release << true
    assert_equal "second", client.started.pop
    Timeout.timeout(2) do
      sleep 0.01 until session.admission_calls == 1 && File.file?(File.join(@output, "worker-admissions.jsonl")) &&
                       File.read(File.join(@output, "worker-admissions.jsonl")).include?("admission_failed")
    end
    4.times { client.release << true }
    assert_equal "completed", task.value
    assert_equal({ "complete" => 5 }, compact_counts)
    refute store.dispatch_halted?
    assert_equal 1, session.admission_calls
    assert_equal [1], client.requests.map { |row| row.dig("target", "worker_indices") }.flatten.uniq
    decisions = File.readlines(File.join(@output, "worker-admissions.jsonl")).map { |line| JSON.parse(line) }
    assert_equal 1, decisions.count { |row| row.dig("decision", "reason") == "admission_failed" }
    assert_includes decisions.find { |row| row.dig("decision", "reason") == "admission_failed" }
                             .dig("decision", "error"), "capacity fixture failed"
    assert runner(client, session, admission_policy: AlwaysUseful.new).send(:expansion_disabled?, "remote-pool")
    assert_equal "verified_provider_absence", JSON.parse(File.read(File.join(@output, "execution.json")))
                                             .dig("resource_disposition", "phase")
  end

  def test_real_policy_expands_from_minimum_to_allowed_concurrency
    @plan = remote_plan(%w[first second third fourth fifth], desired: 3, concurrency: 2)
    @handoff["bootstrap_samples_seconds"] = [0.001]
    client = FakeClient.new
    client.started = Queue.new
    client.release = Queue.new
    session = FakeCapacitySession.new(@handoff)
    base = Time.now
    offset = 0
    Time.stub(:now, -> { base + offset }) do
      task = Thread.new { runner(client, session).run }
      assert_equal "first", client.started.pop
      offset = 20
      client.release << true
      assert_equal "second", client.started.pop
      third = Timeout.timeout(2) { client.started.pop }
      assert_includes %w[third fourth fifth], third
      4.times { client.release << true }
      assert_equal "completed", task.value
    end
    assert_equal 1, session.admission_calls
    assert_includes client.requests.map { |row| row.dig("target", "worker_indices") }, [2]
    decisions = File.readlines(File.join(@output, "worker-admissions.jsonl")).map { |line| JSON.parse(line) }
    decision = decisions.find { |row| row.dig("decision", "reason") == "useful_capacity" }
    assert decision.dig("decision", "estimated_seconds_saved") >= 5
    assert_equal "verified_provider_absence", JSON.parse(File.read(File.join(@output, "execution.json")))
                                             .dig("resource_disposition", "phase")
  end

  private

  def remote_plan(ids, desired: 1, concurrency: 1)
    plan = {
      "contract_version" => WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION,
      "plan_id" => "remote-fixture",
      "failure_policy" => { "max_consecutive_failures" => 10, "max_total_failures" => 10 },
      "pools" => [{
        "pool_id" => "remote-pool", "requirements" => { "ollama" => {
          "model" => "fixture:latest", "expected_digest" => "a" * 64,
          "required_context_length" => 32_768, "require_fully_gpu_resident" => true
        } }
      }],
      "jobs" => ids.map { |id| { "job_id" => id, "pool_id" => "remote-pool", "argv" => ["fixture", id] } }
    }
    profile = {
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{
        "pool_id" => "remote-pool", "backend" => "rpof", "max_concurrency" => concurrency,
        "min_workers" => 1, "desired_workers" => desired, "max_hourly_rate_usd" => 1.0
      }],
      "budget" => { "max_hourly_rate_usd" => 1.0, "max_total_cost_usd" => 2.0, "max_runtime_seconds" => 120 }
    }
    raw = JSON.pretty_generate(plan) + "\n"
    WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(profile)).bind(
      WorkloadOrchestrator::Plan.new(raw)
    )
  end

  def runner(client, session, admission_policy: WorkloadOrchestrator::WorkerAdmissionPolicy.new)
    session.output_dir = @output
    WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: @workers, workdir: @workdir, output_dir: @output,
      out: StringIO.new, rpof_client: client, capacity_session: session, admission_policy: admission_policy
    )
  end

  def store
    WorkloadOrchestrator::ExecutionStore.new(
      output_dir: @output, plan: @plan, workdir: @workdir,
      workers_sha256: @workers.execution_sha256(@plan)
    )
  end

  def metadata(id)
    JSON.parse(File.read(File.join(@output, "runs", id, "metadata.json")))
  end

  def compact_counts
    store.prepare!
    store.counts.reject { |_key, value| value.zero? }
  end
end
