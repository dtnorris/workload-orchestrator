# frozen_string_literal: true

# Offline process boundary for FO-WLO-05. Only registry transport and the clock
# are synthetic; execution, scheduling, claims, payloads and controls are native.
require_relative "../../../lib/workload_orchestrator"

module FoWlo05Fixture
  # Read the test publisher's atomic registry file; no endpoint is contacted.
  class FileSource < WorkloadOrchestrator::WorkerSource
    # Bind an existing synthetic publication path. Returns the source.
    def initialize(path)
      super()
      @path = path
    end

    # Return exact publication bytes; missing files raise SystemCallError.
    def latest_snapshot
      File.binread(@path)
    end
  end

  # Run one bounded fixture operation in a fresh process, printing its outcome.
  # Invalid native evidence raises the native error and yields a nonzero exit.
  def self.run(root, operation, id = nil)
    Thread.new { sleep 12; Process.kill("KILL", Process.pid) }
    if operation == "payload"
      File.write(File.join(root, "#{id}.started"), "started\n")
      unless %w[complete domain pending].include?(id)
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 6
        until File.file?(File.join(root, "#{id}.release"))
          raise "synthetic payload deadline" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          sleep 0.005
        end
      end
      $stdout.binmode.write("out\x00\xff".b)
      $stderr.binmode.write("err\x00\xfe".b)
      exit(id == "domain" ? 42 : 0)
    end

    plan = WorkloadOrchestrator::Plan.load(File.join(root, "input.json"))
    workdir = File.join(root, "work")
    output = File.join(workdir, "output")
    if %w[prepare reset-breaker].include?(operation)
      store = WorkloadOrchestrator::ExecutionStore.new(plan:, workdir:, output_dir: output)
      store.with_execution_lock do
        operation == "prepare" ? store.prepare! : store.acknowledge_circuit_breaker!
      end
      puts store.status
    else
      runner = WorkloadOrchestrator::Runner.new(
        plan:, workdir:, output_dir: output, workers: WorkloadOrchestrator::WorkerSet.new({}),
        worker_source: FileSource.new(File.join(root, "registry.json")),
        worker_poll_interval: 0.02, worker_registry_clock: -> { Time.utc(2030, 1, 1, 0, 2) }
      )
      puts runner.run(resume: operation == "resume", preserve_pauses: operation == "resume")
    end
  end
end

FoWlo05Fixture.run(*ARGV) if $PROGRAM_NAME == __FILE__
