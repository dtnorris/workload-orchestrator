# frozen_string_literal: true

require_relative "test_helper"

class DynamicRunnerTest < Minitest::Test
  include WloTestSupport

  FIXTURE = File.expand_path("../contracts/dynamic-worker-registry/v0.1/minimal-valid.json", __dir__)
  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(*snapshots)
      super()
      @snapshots = snapshots
      @calls = 0
    end

    def latest_snapshot
      snapshot = @snapshots.fetch([calls, @snapshots.length - 1].min)
      @calls += 1
      snapshot
    end
  end

  class CallbackSource < WorkloadOrchestrator::WorkerSource
    def initialize(&callback)
      super()
      @callback = callback
    end

    def latest_snapshot
      @callback.call
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-dynamic-runner-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @plan = WorkloadOrchestrator::Plan.load(
      write_plan(@tmp, jobs: [fixture_job("pending-job")])
    )
    @workers = WorkloadOrchestrator::WorkerSet.new({})
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_execution_with_pending_work_and_zero_workers_remains_live_until_paused
    source = SequenceSource.new(snapshot(revision: 7, workers: []))
    runner = nil
    waits = []
    sleeper = lambda do |seconds, _stop|
      waits << seconds
      runner.store.pause!
    end
    runner = build_runner(source, sleeper)

    assert_equal "paused", runner.run
    assert_equal 1, source.calls
    assert_equal [5.0], waits
    assert_empty runner.worker_registry_poller.current_workers
    assert_equal({ "pending" => 1 }, compact_counts)
  end

  def test_worker_arriving_on_later_poll_updates_current_view_without_dispatching
    worker = fixture_document.fetch("workers").first
    source = SequenceSource.new(
      snapshot(revision: 7, workers: []),
      snapshot(revision: 8, published_at: "2030-01-01T00:00:20Z", workers: [worker])
    )
    runner = nil
    waits = 0
    sleeper = lambda do |_seconds, _stop|
      waits += 1
      runner.store.pause! if waits == 2
    end
    runner = build_runner(source, sleeper)

    assert_equal "paused", runner.run
    assert_equal 2, source.calls
    assert_equal ["worker-1"], runner.worker_registry_poller.ready_workers.map(&:worker_id)
    assert runner.worker_registry_poller.ready_workers.frozen?
    assert_equal({ "pending" => 1 }, compact_counts)
    refute Dir.exist?(File.join(@output, "runs"))
  end

  def test_pause_before_start_prevents_polling
    source = SequenceSource.new(snapshot(revision: 7, workers: []))
    runner = build_runner(source, ->(*) { raise "unexpected sleep" })
    runner.store.prepare!
    runner.store.pause!

    assert_equal "paused", runner.run
    assert_equal 0, source.calls
    assert_nil runner.worker_registry_poller
  end

  def test_fatal_registry_error_halts_execution_fail_closed
    source = SequenceSource.new(snapshot(
                                  revision: 7,
                                  expires_at: NOW.iso8601,
                                  workers: []
                                ))
    runner = build_runner(source, ->(*) { raise "unexpected sleep" })

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run }

    assert_includes error.message, "expired"
    assert_equal "infrastructure_failed", runner.store.status
    assert_equal "worker_registry", runner.store.dispatch_halt.fetch("kind")
    assert_equal({ "pending" => 1 }, compact_counts)

    resume_error = assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true) }
    assert_includes resume_error.message, "worker registry polling is halted"
  end

  def test_resume_uses_checkpoint_and_rejects_registry_rollback
    first_source = SequenceSource.new(snapshot(revision: 8, workers: []))
    first_runner = nil
    sleeper = lambda do |_seconds, _stop|
      first_runner.store.pause!
    end
    first_runner = build_runner(first_source, sleeper)
    assert_equal "paused", first_runner.run

    resumed = build_runner(
      SequenceSource.new(snapshot(revision: 7, published_at: "2029-12-31T23:59:30Z", workers: [])),
      ->(*) { raise "unexpected sleep" }
    )
    error = assert_raises(WorkloadOrchestrator::Error) { resumed.run(resume: true) }

    assert_includes error.message, "rolled back"
    assert_equal "worker_registry", resumed.store.dispatch_halt.fetch("kind")
  end

  def test_terminal_dynamic_work_finishes_without_polling
    source = SequenceSource.new(snapshot(revision: 7, workers: []))
    runner = build_runner(source, ->(*) { raise "unexpected sleep" })
    runner.store.prepare!
    started_at = runner.store.record_running!(
      job: @plan.jobs.first,
      worker: WorkloadOrchestrator::Worker.new("completed-worker", "type" => "command"),
      environment_keys: []
    )
    runner.store.record_terminal!(
      job: @plan.jobs.first,
      status: "complete",
      started_at: started_at,
      exit_status: 0
    )

    assert_equal "completed", runner.run
    assert_equal 0, source.calls
    assert_nil runner.worker_registry_poller
  end

  def test_interrupt_stops_polling_and_is_recorded
    source = SequenceSource.new(snapshot(revision: 7, workers: []))
    runner = nil
    sleeper = lambda do |*_args|
      runner.instance_variable_set(:@interrupt_signal, "TERM")
    end
    runner = build_runner(source, sleeper)

    assert_equal "interrupted", runner.run
    assert_equal "TERM", JSON.parse(File.read(File.join(@output, "execution.json"))).dig("interruption", "signal")
  end

  def test_registry_error_after_interrupt_records_interruption
    runner = nil
    source = CallbackSource.new do
      runner.instance_variable_set(:@interrupt_signal, "TERM")
      snapshot(revision: 7, expires_at: NOW.iso8601, workers: [])
    end
    runner = build_runner(source, ->(*) { raise "unexpected sleep" })

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run }

    assert_includes error.message, "expired"
    assert_equal "interrupted", runner.store.status
  end

  def test_registry_error_does_not_replace_an_existing_dispatch_halt
    runner = nil
    source = CallbackSource.new do
      runner.store.record_dispatch_halt!(kind: "worker_registry", error: "existing evidence")
      snapshot(revision: 7, expires_at: NOW.iso8601, workers: [])
    end
    runner = build_runner(source, ->(*) { raise "unexpected sleep" })

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run }

    assert_includes error.message, "expired"
    assert_equal "existing evidence", runner.store.dispatch_halt.fetch("error")
  end

  private

  def build_runner(source, sleeper)
    WorkloadOrchestrator::Runner.new(
      plan: @plan,
      workers: @workers,
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: source,
      worker_registry_clock: -> { NOW },
      worker_registry_sleeper: sleeper
    )
  end

  def fixture_document
    JSON.parse(File.read(FIXTURE))
  end

  def snapshot(revision:, workers:, published_at: "2030-01-01T00:00:00Z",
               expires_at: "2030-01-01T00:05:00Z")
    JSON.generate(fixture_document.merge(
                    "revision" => revision,
                    "published_at" => published_at,
                    "expires_at" => expires_at,
                    "workers" => workers
                  ))
  end

  def compact_counts
    @plan.jobs.each_with_object(Hash.new(0)) do |job, counts|
      metadata = File.join(@output, "runs", job.id, "metadata.json")
      counts[File.file?(metadata) ? JSON.parse(File.read(metadata)).fetch("status") : "pending"] += 1
    end
  end
end
