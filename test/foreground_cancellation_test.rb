# frozen_string_literal: true

require_relative "test_helper"

class ForegroundCancellationTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")
  REGISTRY_FIXTURE = File.expand_path("fixtures/dynamic-worker-registry-v0.1.json", __dir__)

  class RecordingCancellationTarget
    attr_reader :requests, :wait_count

    def initialize
      @requests = []
      @wait_count = 0
    end

    def cancel(signal:, force:)
      @requests << [signal, force]
      1
    end

    def wait_for_cancellation
      @wait_count += 1
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-foreground-cancellation-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    @bin = File.expand_path("../bin/wlo", __dir__)
    FileUtils.mkdir_p(@workdir)
    @workers_path = write_workers(@tmp, extra_local: true)
    @workers = WorkloadOrchestrator::WorkerSet.load(@workers_path)
    @owned_pids = []
  end

  def teardown
    @owned_pids.each { |pid| terminate_exact_process(pid) }
    FileUtils.remove_entry(@tmp) if File.exist?(@tmp)
  end

  def test_fc01_waiting_execution_stops_promptly_without_launching_work
    plan = dynamic_plan
    source = blocking_command_source
    pid = fork_runner(
      plan,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_poll_interval: 0.01
    )
    await { execution_status == "running" && File.file?(work_path("source.pid")) }
    source_pid = Integer(File.read(work_path("source.pid")))

    started = monotonic_now
    Process.kill("INT", pid)
    status = wait_for_child(pid)

    assert_equal 130, status.exitstatus
    assert_operator monotonic_now - started, :<, 1.0
    assert_equal "interrupted", execution_status
    assert_equal "INT", execution_state.dig("interruption", "signal")
    refute Dir.exist?(File.join(@output, "runs"))
    await { !process_alive?(source_pid) }
  end

  def test_fc02_fc03_fc06_fc08_owned_tree_is_cancelled_without_provider_interaction
    sentinel = Process.spawn(RbConfig.ruby, "-e", "sleep 30", pgroup: true,
                             out: File::NULL, err: File::NULL)
    @owned_pids << sentinel
    plan_path = write_plan(@tmp, jobs: [job("tree", code: tree_job_code)])
    stdout_path = File.join(@tmp, "cli.stdout")
    stderr_path = File.join(@tmp, "cli.stderr")
    pid = spawn_cli(plan_path, stdout_path, stderr_path)
    await { File.file?(work_path("job.pid")) && File.file?(work_path("grandchild.pid")) }
    job_pid = Integer(File.read(work_path("job.pid")))
    grandchild_pid = Integer(File.read(work_path("grandchild.pid")))

    Process.kill("INT", pid)
    status = wait_for_child(pid)

    assert_equal 130, status.exitstatus
    await { !process_alive?(job_pid) && !process_alive?(grandchild_pid) }
    assert process_alive?(sentinel), "unrelated sentinel was terminated"
    metadata = metadata_for("tree")
    assert_equal "interrupted", metadata.fetch("status")
    assert_equal "foreground_cancellation", metadata.dig("evidence", "kind")
    assert_includes %w[term kill], metadata.dig("evidence", "termination_mode")
    assert_equal 15, metadata.fetch("term_signal")
    assert_equal metadata.dig("evidence", "pid"), metadata.dig("evidence", "process_group_id")
    assert_equal "partial stdout\n", File.read(run_path("tree", "stdout.log"))
    assert_equal "partial stderr\n", File.read(run_path("tree", "stderr.log"))
    assert_includes File.read(stdout_path), "Cancellation requested"
    assert_includes File.read(stderr_path), "Execution interrupted; retained evidence"
    assert_includes File.read(stderr_path), "Provider capacity was not changed"
    refute WorkloadOrchestrator.const_defined?(:RpofClient, false)
    assert_empty Dir.glob(File.join(@output, "runs", "**", "provider-*"))
  end

  def test_fc04_fc07_multiple_children_stop_dispatch_and_require_explicit_retry
    jobs = %w[first second third].map { |id| job(id, code: retryable_blocking_job_code(id)) }
    plan_path = write_plan(
      @tmp,
      jobs: jobs,
      pools: [command_pool(worker_names: %w[local local2], max_concurrency: 2)]
    )
    plan = WorkloadOrchestrator::Plan.load(plan_path)
    pid = fork_runner(plan)
    await { %w[first second].all? { |id| File.file?(work_path("#{id}.started")) } }

    Process.kill("INT", pid)
    assert_equal 130, wait_for_child(pid).exitstatus
    refute File.exist?(work_path("third.started"))
    assert_equal %w[interrupted interrupted pending], plan.jobs.map { |entry| job_status(entry) }

    error = assert_raises(WorkloadOrchestrator::Error) { runner_for(plan).run(resume: true) }
    assert_includes error.message, "explicit retry authorization"
    refute File.exist?(work_path("third.started"))

    File.write(work_path("release"), "yes")
    store = store_for(plan)
    assert_equal %w[first second], store.retry_failed!(all: true, reason: "operator reviewed cancellation")
    assert_equal "completed", runner_for(plan).run(resume: true)
    assert_equal %w[complete complete complete], plan.jobs.map { |entry| job_status(entry) }
    assert_equal [2, 2, 1], plan.jobs.map { |entry| metadata_for(entry.id).fetch("attempt") }
    assert_equal 1, execution_state.fetch("interruption_history").length
  end

  def test_fc05_fc09_second_ctrl_c_accelerates_term_resistant_child
    plan = WorkloadOrchestrator::Plan.load(
      write_plan(@tmp, jobs: [job("stubborn", code: term_resistant_job_code)])
    )
    pid = fork_runner(plan)
    await { File.file?(work_path("stubborn.started")) }

    started = monotonic_now
    Process.kill("INT", pid)
    await { File.file?(work_path("term.seen")) }
    Process.kill("INT", pid)
    status = wait_for_child(pid)

    assert_equal 130, status.exitstatus
    assert_operator monotonic_now - started, :<, 0.8
    assert_equal "kill", metadata_for("stubborn").dig("evidence", "termination_mode")
    assert_equal 9, metadata_for("stubborn").fetch("term_signal")
  end

  def test_pa01_pa04_pause_drains_without_signalling_and_resume_preserves_results
    jobs = %w[first second third].map { |id| job(id, code: pausable_job_code(id)) }
    plan = WorkloadOrchestrator::Plan.load(write_plan(
      @tmp,
      jobs: jobs,
      pools: [command_pool(worker_names: %w[local local2], max_concurrency: 2)]
    ))
    pid = fork_runner(plan)
    await { %w[first second].all? { |id| File.file?(work_path("#{id}.started")) } }

    store_for(plan).pause!
    File.write(work_path("release"), "yes")
    status = wait_for_child(pid)

    assert_equal 0, status.exitstatus
    assert_equal "paused", execution_status
    assert_equal %w[complete complete pending], plan.jobs.map { |entry| job_status(entry) }
    refute File.exist?(work_path("term.received"))
    refute File.exist?(work_path("third.started"))

    assert_equal "completed", runner_for(plan).run(resume: true)
    assert_equal %w[complete complete complete], plan.jobs.map { |entry| job_status(entry) }
    assert_equal [1, 1, 1], plan.jobs.map { |entry| metadata_for(entry.id).fetch("attempt") }
    refute File.exist?(work_path("term.received"))
  end

  def test_signal_supervisor_handles_first_and_second_requests_in_process
    target = RecordingCancellationTarget.new
    plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, jobs: [fixture_job("signal-test")]))
    runner = runner_for(
      plan,
      command_executor: target,
      worker_source: Object.new
    )

    begin
      runner.send(:install_interrupt_handlers)
      runner.send(:receive_interrupt, "INT")
      runner.send(:receive_interrupt, "TERM")
    ensure
      runner.send(:restore_interrupt_handlers)
    end
    runner.send(:receive_interrupt, "TERM")
    runner.send(:supervise_interrupts)
    runner.send(:wait_for_owned_cancellation)

    assert_equal "INT", runner.interrupt_signal
    assert_equal [["INT", false], ["INT", true]], target.requests
    assert_equal 1, target.wait_count
    assert_includes runner.send(
      :format_counts,
      "complete" => 0, "failed" => 0, "running" => 0, "pending" => 0, "interrupted" => 1
    ), "interrupted=1"

    status = Struct.new(:exitstatus, :termsig).new(nil, 9)
    error = WorkloadOrchestrator::CommandCancelled.new(
      stdout: "partial output", stderr: "partial error", status: status,
      evidence: {
        signal: "INT", requested_at: "2030-01-01T00:00:00Z",
        termination_mode: "kill", pid: 123, process_group_id: 123
      }
    )
    assert_equal({
                   "kind" => "foreground_cancellation", "signal" => "INT",
                   "requested_at" => "2030-01-01T00:00:00Z", "termination_mode" => "kill",
                   "exit_status" => nil, "term_signal" => 9, "pid" => 123,
                   "process_group_id" => 123
                 }, runner.send(:cancellation_evidence, error))
  end

  def test_in_process_command_cancellation_persists_interrupted_evidence
    status = Struct.new(:exitstatus, :termsig).new(nil, 15)
    error = WorkloadOrchestrator::CommandCancelled.new(
      stdout: "retained stdout\n", stderr: "retained stderr\n", status: status,
      evidence: {
        signal: "INT", requested_at: "2030-01-01T00:00:00Z",
        termination_mode: "term", pid: 456, process_group_id: 456
      }
    )
    plan = WorkloadOrchestrator::Plan.load(write_plan(
      @tmp, jobs: [fixture_job("in-process-cancel")]
    ))
    runner = runner_for(plan, command_executor: ->(*) { raise error })

    assert_equal "interrupted", runner.run
    metadata = metadata_for("in-process-cancel")
    assert_equal "interrupted", metadata.fetch("status")
    assert_equal 15, metadata.fetch("term_signal")
    assert_equal 456, metadata.dig("evidence", "process_group_id")
    assert_equal "retained stdout\n", File.read(run_path("in-process-cancel", "stdout.log"))
    assert_equal "retained stderr\n", File.read(run_path("in-process-cancel", "stderr.log"))
  end

  private

  def dynamic_plan
    WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "waiting-cancellation",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 3 },
      "pools" => [{
        "pool_id" => "model", "required_labels" => ["inference"],
        "requirements" => { "ollama" => { "model" => "model", "expected_digest" => DIGEST } }
      }],
      "jobs" => [fixture_job("pending", pool_id: "model")]
    ))
  end

  def empty_snapshot
    document = JSON.parse(File.read(REGISTRY_FIXTURE))
    JSON.generate(document.merge(
      "revision" => 7,
      "published_at" => "2030-01-01T00:00:00Z",
      "expires_at" => "2030-01-01T00:05:00Z",
      "workers" => []
    ))
  end

  def blocking_command_source
    counter = work_path("source.polled")
    pid_path = work_path("source.pid")
    code = <<~RUBY
      if File.exist?(#{counter.dump})
        File.write(#{pid_path.dump}, Process.pid.to_s)
        sleep 30
      else
        File.write(#{counter.dump}, "yes")
        STDOUT.write(#{empty_snapshot.dump})
      end
    RUBY
    WorkloadOrchestrator::CommandWorkerSource.new(RbConfig.ruby, ["-e", code])
  end

  def tree_job_code
    grandchild = 'File.write("grandchild.pid", Process.pid.to_s); sleep 30'
    <<~RUBY
      STDOUT.sync = true
      STDERR.sync = true
      puts "partial stdout"
      warn "partial stderr"
      child = Process.spawn(#{RbConfig.ruby.dump}, "-e", #{grandchild.dump})
      File.write("job.pid", Process.pid.to_s)
      Process.wait(child)
    RUBY
  end

  def retryable_blocking_job_code(id)
    <<~RUBY
      File.write(#{"#{id}.started".dump}, Process.pid.to_s)
      sleep 30 unless File.exist?("release")
    RUBY
  end

  def term_resistant_job_code
    <<~RUBY
      trap("TERM") { File.write("term.seen", "yes") }
      File.write("stubborn.started", Process.pid.to_s)
      sleep 30
    RUBY
  end

  def pausable_job_code(id)
    <<~RUBY
      trap("TERM") { File.write("term.received", "#{id}"); exit 9 }
      File.write(#{"#{id}.started".dump}, Process.pid.to_s)
      sleep 0.01 until File.exist?("release")
    RUBY
  end

  def spawn_cli(plan_path, stdout_path, stderr_path)
    pid = Process.spawn(
      RbConfig.ruby, @bin, "run", plan_path,
      "--workdir", @workdir, "--output", @output,
      "--workers-config", @workers_path,
      out: stdout_path, err: stderr_path
    )
    @owned_pids << pid
    pid
  end

  def fork_runner(plan, workers: @workers, **options)
    pid = Process.fork do
      status = runner_for(plan, workers: workers, **options).run
      exit!(status == "interrupted" ? 130 : 0)
    rescue StandardError => e
      File.write(File.join(@tmp, "child-error"), "#{e.class}: #{e.message}\n#{e.backtrace.join("\n")}")
      exit!(1)
    end
    @owned_pids << pid
    pid
  end

  def runner_for(plan, workers: @workers, **options)
    WorkloadOrchestrator::Runner.new(
      plan: plan, workers: workers, workdir: @workdir, output_dir: @output,
      out: StringIO.new, **options
    )
  end

  def store_for(plan)
    WorkloadOrchestrator::ExecutionStore.new(
      plan: plan, workdir: @workdir, output_dir: @output
    )
  end

  def wait_for_child(pid, timeout: 5)
    deadline = monotonic_now + timeout
    loop do
      waited, status = Process.waitpid2(pid, Process::WNOHANG)
      if waited
        @owned_pids.delete(pid)
        return status
      end
      if monotonic_now >= deadline
        detail = File.file?(File.join(@tmp, "child-error")) ? File.read(File.join(@tmp, "child-error")) : ""
        raise "timed out waiting for child #{pid} #{detail}"
      end

      sleep 0.01
    end
  end

  def await(timeout: 3)
    deadline = monotonic_now + timeout
    until yield
      raise "timed out waiting for fixture" if monotonic_now >= deadline

      sleep 0.01
    end
  end

  def terminate_exact_process(pid)
    Process.kill("KILL", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  end

  def process_alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end

  def execution_state
    JSON.parse(File.read(File.join(@output, "execution.json")))
  end

  def execution_status
    execution_state.fetch("status") if File.file?(File.join(@output, "execution.json"))
  end

  def metadata_for(id)
    JSON.parse(File.read(run_path(id, "metadata.json")))
  end

  def job_status(job)
    path = run_path(job.id, "metadata.json")
    File.file?(path) ? JSON.parse(File.read(path)).fetch("status") : "pending"
  end

  def run_path(id, name)
    File.join(@output, "runs", id, name)
  end

  def work_path(name)
    File.join(@workdir, name)
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end
