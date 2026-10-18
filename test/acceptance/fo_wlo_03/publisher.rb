# frozen_string_literal: true

# Actual public producer components; only observed runtime/provider facts are
# synthetic. This process cannot start a provider client or inference command.
require_relative "../fo24a/offline_guard"
require "json"
require "fileutils"
require "time"

kind, component, root = ARGV
abort "invalid publisher arguments" unless %w[local remote].include?(kind) && root && component
Fo24aOfflineGuard.component!(kind == "local" ? "local_ollama_workers" : "runpod_ollama_fleet")
$LOAD_PATH.unshift(File.join(component, "lib"))
available = File.file?(File.join(root, "available"))
abort "synthetic publisher failure" if File.file?(File.join(root, "failed"))
if File.file?(File.join(root, "delayed"))
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
  sleep 0.01 while File.file?(File.join(root, "delayed")) && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
end

# Strict fake observation ports expose no runtime mutation methods.
class Fo03Observations
  # Bind explicitly supplied observation methods; unexpected calls raise.
  def initialize(**methods)
    methods.each { |name, implementation| define_singleton_method(name, &implementation) }
  end
end

identity = {"worker_id" => "#{kind}-worker", "generation_id" => "generation-1",
            "endpoint" => "http://127.0.0.1:#{kind == 'local' ? 19441 : 19442}"}
model = {"model" => "qualified:latest", "digest" => "a" * 64, "context_length" => 131_072, "fully_gpu_resident" => true}
now = Time.now.utc
if kind == "local"
  require "local_ollama_workers"
  evidence = LocalOllamaWorkers::CapabilityEvidenceStore.new(root: File.join(root, "evidence"))
  evidence.record!(identity:, ollama_version: "synthetic", gpu_id: "synthetic-gpu", model: model.merge(
    "observed_at" => now.iso8601, "runtime_size_bytes" => 100, "runtime_size_vram_bytes" => 100,
    "source" => LocalOllamaWorkers::CapabilityEvidenceStore::SOURCE
  )) unless evidence.present?
  running = model.merge("runtime_size_bytes" => 100, "runtime_size_vram_bytes" => 100)
  source = LocalOllamaWorkers::LocalWorkerSource.new(
    observer: Fo03Observations.new(observe: -> { available ? identity : nil }),
    client: Fo03Observations.new(endpoint: -> { identity.fetch("endpoint") }, version: -> { "synthetic" },
      installed_models: -> { [model.slice("model", "digest")] }, running_models: -> { [running] }), evidence_store: evidence
  )
  document = LocalOllamaWorkers::RegistryPublisher.new(state_root: File.join(root, "publisher"),
    worker_source: source, ttl_seconds: 120, id_generator: -> { "fo03-local" }).snapshot
else
  require "runpod_ollama_fleet/dynamic_worker_registry"
  worker = {"index" => 1, "pod_id" => "synthetic-pod", "worker_id" => identity.fetch("worker_id"),
            "generation_id" => identity.fetch("generation_id"), "generation" => 1, "status" => "active",
            "gpu_id" => "synthetic-gpu", "local_ollama_url" => identity.fetch("endpoint")}
  fleet = {"fleet_id" => "synthetic-fleet", "status" => "active", "workers" => available ? [worker] : []}
  # These files are fake external bootstrap/tunnel observations, not registries.
  FileUtils.mkdir_p([File.join(root, "bootstrap", "fixture"), File.join(root, "tunnels")])
  File.write(File.join(root, "bootstrap", "current"), "fixture\n")
  File.write(File.join(root, "bootstrap", "fixture", "bootstrap.json"), JSON.generate(
    "fleet_id" => "synthetic-fleet", "status" => "passed", "models" => [model.fetch("model")], "context" => 131_072,
    "expected_digests" => {model.fetch("model") => model.fetch("digest")},
    "workers" => [worker.slice("index", "pod_id", "worker_id", "generation_id").merge(
      "status" => "passed", "provenance_error" => nil, "provenance" => {"gpu" => {"name" => "synthetic-gpu"},
        "models" => {model.fetch("model") => model.merge("size_bytes" => 100, "size_vram_bytes" => 100)}})]
  ))
  File.write(File.join(root, "tunnels", "tunnels.json"), JSON.generate(
    "fleet_id" => "synthetic-fleet", "workers" => [worker.slice("index", "pod_id", "worker_id", "generation_id").merge(
      "endpoint" => identity.fetch("endpoint"), "pid" => 123, "process_identity" => {"synthetic" => true})]
  ))
  state = Fo03Observations.new(current: -> { fleet }, artifact_dir: ->(_fleet_id, name) { File.join(root, name) },
    registry_identity: lambda { |index:, observed_pod_id:|
      raise "provider identity mismatch" unless index == 1 && observed_pod_id == "synthetic-pod"
      identity.slice("worker_id", "generation_id")
    })
  document = RunpodOllamaFleet::DynamicWorkerRegistry.new(state_root: File.join(root, "publisher"), repo_root: root,
    fleet_sources: [{"fleet_key" => "synthetic", "state" => state}], ttl_seconds: 120,
    process_adapter: Fo03Observations.new(alive?: ->(pid) { pid == 123 }, matches?: ->(pid, _) { pid == 123 }),
    health_checker: Fo03Observations.new(check: ->(_endpoint) { Struct.new(:healthy).new(true) }),
    id_generator: -> { "fo03-remote" }).snapshot
end
bytes = JSON.generate(document)
File.open(File.join(root, "publications.jsonl"), "ab") { |file| file.puts(bytes) }
STDOUT.write(bytes)
Fo24aOfflineGuard.assert_clean!
