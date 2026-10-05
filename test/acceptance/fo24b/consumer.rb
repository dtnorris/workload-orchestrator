# frozen_string_literal: true

require_relative "../fo24a/offline_guard"
require "json"
require "tmpdir"
require "fileutils"
require "stringio"
require "timeout"
require_relative "../../../contracts/dynamic-worker-registry/v0.1/conformance"
Fo24aOfflineGuard.component!("workload_orchestrator")
repo, input = ARGV
$LOAD_PATH.unshift(File.join(repo, "lib"))
require "workload_orchestrator"

# Transport scripting is test-only. Provider-owned publication/readiness was
# performed in separate FO-24A producer processes; only public bytes enter WLO.
class Fo24bSource < WorkloadOrchestrator::WorkerSource
  attr_accessor :bytes
  def initialize(document) = @bytes = JSON.generate(document)
  def latest_snapshot = bytes
end

class Fo24bConsumer
  W = WorkloadOrchestrator
  NOW = Time.utc(2030, 1, 1, 0, 1, 0)
  Result = Struct.new(:exitstatus) { def success? = exitstatus.zero? }

  def initialize(payload, root)
    @payload, @root, @checks = payload, root, 0
  end

  def check(value, message)
    @checks += 1
    raise message unless value
  end

  def copy(value) = JSON.parse(JSON.generate(value))
  def document(owner, scenario = "valid") = copy(@payload.fetch("providers").fetch(owner).fetch(scenario))

  def setup(name, owners: %w[low rpof], jobs: 1, breaker: 3)
    @dir = File.join(@root, name)
    FileUtils.mkdir_p(@dir)
    @now = NOW
    @sources = owners.to_h { |owner| [owner, Fo24bSource.new(document(owner))] }
    @calls = []
    @effect = nil
    argv = [RbConfig.ruby, File.expand_path("../fo24a/job.rb", __dir__), "success"]
    path = File.join(@dir, "plan.json")
    File.write(path, JSON.generate(
      "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "neutral-faults",
      "failure_policy" => { "max_consecutive_failures" => breaker, "max_total_failures" => breaker },
      "pools" => [{ "pool_id" => "generic", "required_labels" => ["inference"],
                    "requirements" => { "ollama" => @payload.fetch("request").fetch("ollama") } }],
      "jobs" => jobs.times.map { |n| { "job_id" => "job-#{n + 1}", "pool_id" => "generic", "argv" => argv,
                                     "depends_on_job_ids" => n.zero? ? [] : ["job-#{n}"] } }
    ))
    @plan = W::Plan.load(path)
    @store = W::ExecutionStore.new(plan: @plan, workdir: @dir, output_dir: File.join(@dir, "execution"))
    @store.prepare!
  end

  def sources(order = @sources.keys, policy: "optional")
    W::WorkerSourceSet.new(order.each_with_index.map do |owner, index|
      # Arbitrary source names deliberately carry no provider meaning.
      W::WorkerSourceSet::Entry.new(name: "zone-#{index}", source: @sources.fetch(owner), policy: policy)
    end)
  end

  def publish(owner, scenario = "valid", empty: false)
    @now += 1
    source = @sources.fetch(owner)
    previous = JSON.parse(source.bytes)
    replacement = document(owner, scenario)
    replacement["workers"] = [] if empty
    replacement["revision"] = previous.fetch("revision") + 1
    replacement["published_at"] = @now.iso8601
    replacement["expires_at"] = (@now + 60).iso8601
    source.bytes = JSON.generate(replacement)
  end

  def poller(order = @sources.keys, policy: "optional")
    W::WorkerRegistrySet.new(sources: sources(order, policy: policy),
      checkpoint_root: File.join(@dir, "observation-#{order.join('-')}-#{policy}"), clock: -> { @now })
  end

  def incompatible(owner)
    publish(owner)
    snapshot = JSON.parse(@sources.fetch(owner).bytes)
    worker = snapshot.fetch("workers").first
    worker.dig("capabilities", "ollama", "models").first["digest"] = "b" * 64
    worker["capability_fingerprint"] = DynamicWorkerRegistryV01::Conformance.capability_fingerprint(worker)
    @sources.fetch(owner).bytes = JSON.generate(snapshot)
  end

  def assignments(poller)
    return [] if poller.dispatch_blocked?
    W::DynamicScheduler.new(plan: @plan, store: @store).assignments(
      workers: poller.ready_workers, current_workers: poller.current_workers)
  end

  def run(resume: false, real_command: false, wait: nil)
    executor = lambda do |environment, *argv, chdir:|
      metadata = @store.metadata_for(@plan.jobs.find { |job| @store.metadata_for(job)&.fetch("status") == "running" })
      check(environment.fetch("WLO_WORKER_ENDPOINT") == metadata.dig("worker_execution_identity", "endpoint"), "dispatch endpoint differs from persisted binding")
      wire = @sources.values.map(&:bytes).find { |value| JSON.parse(value).fetch("registry_id") == metadata.dig("worker_execution_identity", "registry_id") }
      check(metadata.fetch("worker_registry_binding") == {
        "registry_revision" => JSON.parse(wire).fetch("revision"),
        "registry_snapshot_sha256" => Digest::SHA256.hexdigest(wire)
      }, "dispatch revision/hash differs from the selected public source")
      @calls << copy(metadata)
      code = @effect&.call(metadata)
      real_command ? W::OwnedCommandRunner.new.call(environment, *argv, chdir: chdir) : ["scripted stdout\n", "scripted stderr\n", Result.new(code.is_a?(Integer) ? code : 0)]
    end
    @runner = W::Runner.new(plan: @plan, workers: W::WorkerSet.new({}), workdir: @dir,
      output_dir: @store.output_dir, out: StringIO.new, worker_source: sources,
      worker_registry_clock: -> { @now }, worker_poll_interval: 0.001,
      worker_registry_sleeper: wait, command_executor: executor)
    argv = @plan.jobs.first.argv
    Fo24aOfflineGuard.permit(argv) { Timeout.timeout(10) { @runner.run(resume: resume) } }
  end

  def metadata(index = 0) = @store.metadata_for(@plan.jobs.fetch(index))
  def bytes(index = 0) = File.binread(File.join(@store.output_dir, "runs", @plan.jobs.fetch(index).id, "metadata.json"))

  def truth(row, owner:, generation: "generation-1", attempt: 1, status: "complete", index: 0, exit_status: 0)
    worker = document(owner, generation == "generation-1" ? "valid" : "generation_2").fetch("workers").first
    identity = worker.slice("worker_id", "generation_id", "endpoint", "capability_fingerprint").merge("registry_id" => document(owner).fetch("registry_id"))
    check(row.fetch("job_id") == @plan.jobs.fetch(index).id, "job identity changed")
    check(row.fetch("attempt") == attempt, "attempt number changed")
    check(row.fetch("worker_execution_identity") == identity, "exact generation/source identity changed")
    check(row.fetch("worker_snapshot").slice(*identity.keys) == identity, "snapshot differs from attempt")
    check(row.fetch("worker_registry_binding").fetch("registry_revision").is_a?(Integer), "missing revision")
    check(row.dig("worker_registry_binding", "registry_snapshot_sha256").match?(/\A[0-9a-f]{64}\z/), "missing snapshot hash")
    check(row.fetch("status") == status && row.fetch("exit_status") == exit_status, "false terminal outcome")
    check(Time.iso8601(row.fetch("completed_at")) >= Time.iso8601(row.fetch("started_at")), "terminal precedes start")
    check(row.fetch("elapsed_seconds") >= 0, "negative elapsed time")
    launched = @calls.find { |call| call["job_id"] == row["job_id"] && call["attempt"] == attempt }
    if launched
      %w[started_at worker_execution_identity worker_registry_binding worker_snapshot].each do |key|
        check(launched.fetch(key) == row.fetch(key), "retained #{key} rewritten after dispatch")
      end
    end
  end

  def retry_to(owner, generation: "generation-2", old:)
    @sources.each_key { |kind| publish(kind, kind == owner ? "generation_2" : "valid", empty: kind != owner) }
    @store.retry_failed!(all: true, reason: "scripted operator reviewed uncertain side effects")
    check(@store.paused?, "retry implicitly resumed")
    @effect = nil
    check(run(resume: true, real_command: true) == "completed", "explicit retry did not complete")
    truth(metadata, owner: owner, generation: generation, attempt: 2)
    archive = File.join(@store.output_dir, "attempts/job-1/attempt-1/metadata.json")
    check(File.binread(archive) == old, "superseded attempt history rewritten")
    current = bytes
    check(run(resume: true) == "completed" && bytes == current, "duplicate completion")
    check(@calls.length == 2, "unexpected extra dispatch")
  end

  def exercise
    %w[low rpof].each do |owner|
      setup("baseline-#{owner}", owners: [owner])
      check(run(real_command: true) == "completed", "baseline failed")
      truth(metadata, owner: owner)
      puts "PASS #{owner}-only production publication -> real Runner/OwnedCommandRunner"
    end
    setup("mixed")
    forward = assignments(poller.poll_once).first.worker.execution_identity
    reverse = assignments(poller(@sources.keys.reverse).poll_once).first.worker.execution_identity
    check(forward == reverse, "source order changed deterministic placement")
    check(poller.poll_once.ready_workers.length == 2, "mixed sources missing")
    check(run(real_command: true) == "completed", "mixed baseline failed")
    chosen = forward.first == document("low").fetch("registry_id") ? "low" : "rpof"
    truth(metadata, owner: chosen)
    puts "PASS mixed baseline, both compatible, arbitrary source names/order, exact chosen identity"

    %w[low rpof].each do |owner|
      other = ( %w[low rpof] - [owner] ).first
      %w[not_ready unavailable stale incompatible disappeared].each do |fault|
        setup("#{owner}-#{fault}")
        before = poller.poll_once
        check(before.ready_workers.length == 2, "pre-fault planning evidence missing")
        case fault
        when "disappeared" then publish(owner, empty: true)
        when "stale"
          @now += 600
          publish(other)
        when "incompatible"
          incompatible(owner)
        else publish(owner, fault)
        end
        before.poll_once
        check(assignments(before).map { |a| a.worker.registry_id } == [document(other).fetch("registry_id")], "#{owner} #{fault}: bad evidence remained eligible: #{before.source_health.inspect}")
        check(run(real_command: true) == "completed", "healthy peer did not dispatch")
        truth(metadata, owner: other)
        puts "PASS #{owner} #{fault} before dispatch -> #{other}; no dispatch on invalid source"
      end
    end

    setup("both-lost")
    observed = poller.poll_once
    @sources.each_key { |owner| publish(owner, empty: true) }
    check(assignments(observed.poll_once).empty?, "missing workers still selected")
    check(run(wait: ->(*) { @store.pause! }) == "paused", "both lost fabricated completion")
    check(@calls.empty? && metadata.nil?, "both lost dispatched")
    publish("rpof", "generation_2")
    check(run(resume: true, real_command: true) == "completed", "registry recovery did not enable dispatch")
    truth(metadata, owner: "rpof", generation: "generation-2")
    puts "PASS both sources lost -> pending; valid registry recovery -> future dispatch"

    %w[low rpof].each do |owner|
      %w[not_ready unavailable stale].each do |fault|
        setup("solo-#{owner}-#{fault}", owners: [owner])
        fault == "stale" ? @now += 600 : publish(owner, fault)
        check(run(wait: ->(*) { @store.pause! }) == "paused", "invalid solo source did not wait")
        check(@calls.empty? && metadata.nil?, "invalid solo source dispatched")
      end
    end
    setup("neither-compatible")
    @sources.each_key { |owner| incompatible(owner) }
    check(run(wait: ->(*) { @store.pause! }) == "paused", "neither eligible fabricated completion")
    check(@calls.empty? && metadata.nil?, "neither eligible dispatched")
    puts "PASS LOW-only/RPOF-only stale/unavailable/not-ready and neither eligible: no dispatch"

    setup("required-source-expiry")
    required = poller(policy: "required").poll_once
    @now += 600
    publish("rpof")
    required.poll_once
    check(required.ready_workers.length == 1 && assignments(required).empty?, "required expired source failed open")
    publish("low", "generation_2")
    check(assignments(required.poll_once).length == 1, "required source recovery remained blocked")
    puts "PASS required source expiry blocks despite healthy peer; recovery restores policy admissibility"

    setup("running-readiness", jobs: 2)
    publish("low", empty: true)
    @effect = lambda do |_row|
      publish("rpof", "not_ready")
      Timeout.timeout(5) { Thread.pass until @runner.worker_registry_poller.ready_workers.empty? }
      check(metadata.fetch("status") == "running" && !@store.dispatch_halted?, "readiness loss rewrote running attempt")
    end
    check(run(wait: ->(*) { @store.pause! if metadata&.fetch("status") == "complete" }) == "paused", "readiness did not drain")
    truth(metadata, owner: "rpof")
    check(metadata(1).nil?, "NOT_READY received new work")
    old = bytes
    publish("rpof", "generation_2")
    @effect = nil
    check(run(resume: true, real_command: true) == "completed" && bytes == old, "readiness recovery rewrote history")
    truth(metadata(1), owner: "rpof", generation: "generation-2", index: 1)
    puts "PASS provider active/NOT_READY blocks new work, preserves running attempt, readiness recovery resumes"

    %w[low rpof].each do |owner|
      other = (%w[low rpof] - [owner]).first
      %w[loss replacement interruption failure command_failure].each do |fault|
        setup("#{owner}-running-#{fault}")
        publish(other, empty: true)
        @effect = lambda do |_row|
          case fault
          when "loss", "replacement"
            publish(owner, "generation_2", empty: fault == "loss")
            Timeout.timeout(5) { Thread.pass until @store.dispatch_halted? }
          when "failure" then raise "scripted worker transport failure"
          when "command_failure" then 7
          when "interruption"
            raise W::CommandCancelled.new(stdout: "partial output", stderr: "partial error",
              status: Struct.new(:exitstatus, :termsig).new(nil, 15),
              evidence: { signal: "INT", requested_at: Time.now.utc.iso8601, termination_mode: "term", pid: 123, process_group_id: 123 })
          end
        end
        expected = { "loss" => "infrastructure_failed", "replacement" => "infrastructure_failed",
                     "interruption" => "interrupted", "failure" => "workload_failed", "command_failure" => "workload_failed" }.fetch(fault)
        check(run == expected, "#{fault} outcome incorrect")
        truth(metadata, owner: owner, status: fault == "interruption" ? "interrupted" : "failed", exit_status: fault == "command_failure" ? 7 : nil)
        if %w[loss replacement].include?(fault)
          check(metadata.dig("evidence", "outcome_known") == false, "worker loss fabricated known outcome")
          check(metadata.dig("late_evidence", 0, "status") == "complete", "late result not retained as evidence")
          check(@store.action_check("resume")["disposition"] == "blocked", "loss resumed without review")
        elsif fault == "interruption"
          check(metadata.dig("evidence", "term_signal") == 15, "interruption signal lost")
          check(@store.action_check("resume")["disposition"] == "blocked", "interruption implicitly retried")
        end
        old = bytes
        retry_to(fault == "replacement" ? owner : other, old: old)
        puts "PASS #{owner} running #{fault}, exact old attempt/archive, explicit retry on current generation"
      end
    end

    %w[low rpof].each do |owner|
      other = (%w[low rpof] - [owner]).first
      setup("pause-#{owner}", jobs: 2)
      publish(other, empty: true)
      @effect = ->(*) { @store.pause! }
      check(run == "paused", "pause failed to drain")
      truth(metadata, owner: owner)
      old = bytes
      publish(owner, empty: true)
      publish(other, "generation_2")
      check(@store.action_check("resume")["disposition"] == "execute", "paused execution not resumable")
      @effect = nil
      check(run(resume: true, real_command: true) == "completed", "resume failed")
      check(bytes == old, "source change rewrote drained paused attempt")
      truth(metadata(1), owner: other, generation: "generation-2", index: 1)
      puts "PASS pause/drain #{owner}, source change while paused, resume on #{other} generation-2"
    end

    setup("mixed-breaker", jobs: 3, breaker: 2)
    @effect = ->(*) { raise "scripted consecutive failure" }
    check(run == "circuit_broken", "breaker did not trip")
    check(poller.poll_once.ready_workers.length == 2, "providers did not remain READY")
    check(@calls.length == 2 && metadata(2).nil?, "breaker allowed dispatch")
    old = [bytes, bytes(1)]
    2.times { |index| truth(metadata(index), owner: "low", status: "failed", exit_status: nil, index: index) }
    check(@store.action_check("resume")["disposition"] == "blocked", "READY overrode breaker")
    begin
      run(resume: true)
      raise "implicit breaker acknowledgement"
    rescue W::Error => e
      check(e.message.include?("circuit breaker"), "wrong resume rejection")
    end
    check([bytes, bytes(1)] == old && @calls.length == 2, "blocked resume rewrote attempts")
    @store.retry_failed!(all: true, reason: "scripted repair reviewed", acknowledge_circuit_breaker: true)
    check(@store.paused?, "breaker acknowledgement implicitly resumed")
    @effect = nil
    check(run(resume: true, real_command: true) == "completed", "reviewed breaker recovery failed")
    2.times do |index|
      truth(metadata(index), owner: "low", attempt: 2, index: index)
      check(File.binread(File.join(@store.output_dir, "attempts/job-#{index + 1}/attempt-1/metadata.json")) == old[index], "breaker recovery erased fault history")
    end
    truth(metadata(2), owner: "low", index: 2)
    puts "PASS mixed READY != dispatch allowed; breaker blocks resume; explicit acknowledgement/retry preserves history"
    puts "PASS #{@checks} composed execution/identity assertions; provider implementations, AdventureFinder and AFW absent"
  end
end

Dir.mktmpdir("fo24b-flows-") { |root| Fo24bConsumer.new(JSON.parse(File.binread(input)), root).exercise }
Fo24aOfflineGuard.assert_clean!
