# frozen_string_literal: true

require_relative "offline_guard"
require "json"
require "tmpdir"
require "fileutils"
require "stringio"
require "timeout"
Fo24aOfflineGuard.component!("workload_orchestrator")
repo, input, destination = ARGV
$LOAD_PATH.unshift(File.join(repo, "lib"))
require "workload_orchestrator"

payload = JSON.parse(File.binread(input))
now = Time.utc(2030, 1, 1, 0, 1, 0)
request = WorkloadOrchestrator::OllamaCapabilityRequest.new(JSON.generate(payload.fetch("request")))
results = {}
Dir.mktmpdir("fo24a-consumer-") do |root|
  payload.fetch("cases").each do |name, spec|
    output = File.join(root, name)
    FileUtils.mkdir_p(output)
    outcome = spec.fetch("outcome")
    argv = [RbConfig.ruby, File.join(__dir__, "job.rb"), outcome]
    document = {
      "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "neutral-plan",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 2 },
      "pools" => [{ "pool_id" => "generic", "required_labels" => ["inference"],
                    "requirements" => { "ollama" => payload.fetch("request").fetch("ollama") } }],
      "jobs" => [{ "job_id" => "neutral-job", "pool_id" => "generic", "argv" => argv }]
    }
    plan_path = File.join(output, "input-plan.json")
    File.write(plan_path, JSON.generate(document))
    plan = WorkloadOrchestrator::Plan.load(plan_path)
    registry_path = File.join(output, "input-registry.json")
    bytes = JSON.generate(spec.fetch("snapshot"))
    File.write(registry_path, bytes)
    source = WorkloadOrchestrator::StaticWorkerSource.from_file(registry_path)
    runner = nil
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan, workers: WorkloadOrchestrator::WorkerSet.new({}), workdir: output,
      output_dir: File.join(output, "execution"), out: StringIO.new, worker_source: source,
      worker_registry_clock: -> { now }, worker_poll_interval: 0.001,
      worker_registry_sleeper: ->(*) { runner.store.pause! }
    )
    status = Fo24aOfflineGuard.permit(argv) { Timeout.timeout(10) { runner.run } }
    jobs = JSON.parse(File.binread(File.join(output, "execution", "jobs.json"))).fetch("jobs")
    metadata_path = File.join(output, "execution", "runs", "neutral-job", "metadata.json")
    row = { "execution_status" => status, "job_status" => jobs.fetch(0).fetch("status") }
    if outcome == "no_dispatch"
      raise "#{name}: dispatch occurred" if File.exist?(metadata_path)
      raise "#{name}: waiting job did not remain pending" unless row["job_status"] == "pending" && status == "paused"
      row["source_health"] = runner.worker_registry_poller.source_health
    else
      metadata = JSON.parse(File.binread(metadata_path))
      worker = spec.fetch("snapshot").fetch("workers").fetch(0)
      expected = worker.slice("worker_id", "generation_id", "endpoint", "capability_fingerprint").merge(
        "registry_id" => spec.fetch("snapshot").fetch("registry_id")
      )
      raise "#{name}: false attempt identity" unless metadata.fetch("worker_execution_identity") == expected
      expected_binding = {
        "registry_revision" => spec.fetch("snapshot").fetch("revision"),
        "registry_snapshot_sha256" => Digest::SHA256.hexdigest(bytes)
      }
      raise "#{name}: false registry binding" unless metadata.fetch("worker_registry_binding") == expected_binding
      raise "#{name}: false capability evidence" unless metadata.dig("worker_snapshot", "capabilities") == worker.fetch("capabilities")
      wanted = outcome == "failure" ? "failed" : "complete"
      raise "#{name}: false job result #{row.inspect}" unless row["job_status"] == wanted
      raise "#{name}: false exit status" unless metadata.fetch("exit_status") == (outcome == "failure" ? 7 : 0)
      row["identity"] = metadata.fetch("worker_execution_identity")
      row["exit_status"] = metadata.fetch("exit_status")
    end
    results[name] = row
  end
end

# Identity comparison is deliberately separate from lifecycle replacement.
%w[neutral low rpof].each do |origin|
  first = results.fetch("#{origin}-valid").fetch("identity")
  second = results.fetch("#{origin}-generation_2").fetch("identity")
  raise "logical worker changed" unless first.fetch("worker_id") == second.fetch("worker_id")
  raise "new generation substituted into old attempt" if first == second
  one = WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(payload.fetch("cases").fetch("#{origin}-valid").fetch("snapshot")), now: now)
  two = WorkloadOrchestrator::DynamicWorkerRegistry.new(JSON.generate(payload.fetch("cases").fetch("#{origin}-generation_2").fetch("snapshot")), now: now)
  bound = WorkloadOrchestrator::DynamicWorkerBinding.from_worker(one.entries.first)
  replacement = WorkloadOrchestrator::DynamicWorkerBinding.from_worker(two.entries.first)
  raise "stale binding matched replacement" if bound.same_identity?(replacement)
  raise "binding was rewritten" unless bound.execution_identity == first
end
Fo24aOfflineGuard.assert_clean!
File.write(destination, JSON.generate("results" => results, "fingerprint" => request.fingerprint,
  "loaded_features" => $LOADED_FEATURES))
