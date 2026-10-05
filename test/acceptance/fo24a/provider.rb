# frozen_string_literal: true

require_relative "offline_guard"
require "json"
require "tmpdir"
require "fileutils"
require "time"

kind, repo, request_path, destination = ARGV
abort "provider arguments missing" unless destination && %w[low rpof].include?(kind)
Fo24aOfflineGuard.component!(kind == "low" ? "local_ollama_workers" : "runpod_ollama_fleet")
$LOAD_PATH.unshift(File.join(repo, "lib"))
if kind == "low"
  require "local_ollama_workers"
  request_class = LocalOllamaWorkers::OllamaCapabilityRequest
  request_error = LocalOllamaWorkers::Error
else
  require "runpod_ollama_fleet/dynamic_worker_registry"
  require "runpod_ollama_fleet/ollama_capability_request"
  require "runpod_ollama_fleet/worker_bringup_identity"
  require "runpod_ollama_fleet/worker_bringup_readiness_gate"
  request_class = RunpodOllamaFleet::OllamaCapabilityRequest
  request_error = RunpodOllamaFleet::OllamaCapabilityRequest::Error
end

# Strict fakes implement only the observed boundary methods. Any other call
# (including provider create/delete, HTTP inference, SSH) raises immediately.
class ScriptedPort
  def initialize(**methods)
    methods.each { |name, implementation| define_singleton_method(name, &implementation) }
  end
  def method_missing(name, *) = Fo24aOfflineGuard.deny!("unsupported fake method #{name}")
  def respond_to_missing?(*args) = false
end

NOW = Time.utc(2030, 1, 1, 0, 1, 0)
document = JSON.parse(File.binread(request_path))
request = request_class.new(JSON.generate(document))
runtime = document.fetch("ollama")
model = {
  "model" => runtime.fetch("model"), "digest" => runtime.fetch("expected_digest"),
  "context_length" => runtime.fetch("required_context_length"), "fully_gpu_resident" => true
}
gpu = runtime.fetch("required_gpu_id")
validate = lambda do |candidate, observed|
  if kind == "low"
    candidate.validate_observed!(running: {
      "model" => observed.fetch("model"), "digest" => observed.fetch("expected_digest"),
      "context_length" => observed.fetch("required_context_length"),
      "fully_gpu_resident" => observed.fetch("require_fully_gpu_resident")
    }, gpu_id: observed.fetch("required_gpu_id", gpu))
  else
    candidate.validate_profile!(profile: observed, hardware: {
      "qualified_gpu_ids" => [observed.fetch("required_gpu_id", gpu)]
    })
  end
end
validate.call(request, runtime)
changes = {
  "model" => "other:latest", "expected_digest" => "b" * 64,
  "required_context_length" => 65_536, "require_fully_gpu_resident" => false,
  "required_gpu_id" => "Other GPU"
}
checks = {}
changes.each do |field, value|
  changed = runtime.merge(field => value)
  begin
    validate.call(request, changed)
    raise "#{kind}: accepted mismatched #{field}"
  rescue request_error
    checks["mismatch_#{field}"] = true
  end
  other = request_class.new(JSON.generate(document.merge("ollama" => changed)))
  raise "runtime identity did not change" if other.fingerprint == request.fingerprint
  checks["identity_#{field}"] = other.fingerprint
end
false_request = request_class.new(JSON.generate(document.merge("ollama" => runtime.merge("require_fully_gpu_resident" => false))))
validate.call(false_request, runtime.merge("require_fully_gpu_resident" => false))
begin
  validate.call(false_request, runtime)
  raise "false residency must compare exactly"
rescue request_error
  checks["false_residency_exact"] = true
end
no_gpu = request_class.new(JSON.generate(document.merge("ollama" => runtime.reject { |k, _| k == "required_gpu_id" })))
validate.call(no_gpu, runtime.merge("required_gpu_id" => "Other GPU"))
checks["optional_gpu"] = no_gpu.fingerprint
begin
  request_class.new(JSON.generate(document.merge("provenance" => "forbidden")))
  raise "provenance entered capability request"
rescue request_error
  checks["provenance_rejected"] = true
end
checks["external_provenance_identity"] = %w[first second].map do |tag|
  envelope = { "request" => document, "provenance" => tag }
  request_class.new(JSON.generate(envelope.fetch("request"))).fingerprint
end

snapshots = {}
Dir.mktmpdir("fo24a-provider-") do |temporary|
  %w[valid unavailable not_ready invalid_capability generation_2 stale_generation private_details].each do |scenario|
    root = File.join(temporary, scenario)
    FileUtils.mkdir_p(root)
    generation = %w[generation_2 stale_generation].include?(scenario) ? "generation-2" : "generation-1"
    identity = { "worker_id" => "#{kind}-worker", "generation_id" => generation,
                 "endpoint" => "http://127.0.0.1:11441" }
    if kind == "low"
      original = identity.merge("generation_id" => scenario == "stale_generation" ? "generation-1" : generation)
      observed = original
      running = model.merge("runtime_size_bytes" => 30_000, "runtime_size_vram_bytes" => 30_000)
      observer = ScriptedPort.new(observe: -> { observed })
      client = ScriptedPort.new(
        endpoint: -> { identity.fetch("endpoint") }, version: -> { "fixture-version" },
        installed_models: -> { [running.slice("model", "digest")] }, running_models: -> { [running] },
        preload!: lambda { |model:, context_length:|
          raise "unscripted preload" unless model == runtime.fetch("model") && context_length == 131_072
        }
      )
      store = LocalOllamaWorkers::CapabilityEvidenceStore.new(root: root)
      LocalOllamaWorkers::CapabilityBootstrapper.new(
        observer: observer, client: client, evidence_store: store, clock: -> { NOW },
        hardware_probe: ScriptedPort.new(gpu_id: -> { gpu })
      ).bootstrap(capability_request: request)
      observed = identity
      observed = nil if %w[unavailable not_ready].include?(scenario)
      running = running.merge("digest" => "b" * 64) if scenario == "invalid_capability"
      # A provider-only sidecar is deliberately outside the wire contract.
      File.write(File.join(root, "private-details.json"), '{"opaque":"changed"}') if scenario == "private_details"
      source = LocalOllamaWorkers::LocalWorkerSource.new(observer: observer, client: client, evidence_store: store)
      snapshots[scenario] = LocalOllamaWorkers::RegistryPublisher.new(
        state_root: root, worker_source: source, clock: -> { NOW }, id_generator: -> { "low-acceptance" }
      ).snapshot
    else
      worker = {
        "index" => 1, "pod_id" => "fake-pod", "worker_id" => identity.fetch("worker_id"),
        "generation_id" => generation, "generation" => generation == "generation-1" ? 1 : 2,
        "host" => "192.0.2.1", "ssh_port" => 22001, "gpu_id" => gpu,
        "status" => scenario == "unavailable" ? "destroyed" : "active",
        "local_ollama_url" => identity.fetch("endpoint")
      }
      worker["opaque_provider_detail"] = "irrelevant" if scenario == "private_details"
      fleet = { "fleet_id" => "fake-fleet", "status" => "active", "gpu" => { "id" => gpu }, "workers" => [worker] }
      state = ScriptedPort.new(
        current: -> { fleet }, artifact_dir: ->(_id, name) { File.join(root, name) },
        registry_identity: lambda { |index:, observed_pod_id:|
          raise "unscripted identity" unless index == 1 && observed_pod_id == "fake-pod"
          identity.slice("worker_id", "generation_id")
        }
      )
      profile = runtime.merge("profile_id" => "main")
      bound_worker = scenario == "stale_generation" ? worker.merge("generation_id" => "generation-1", "generation" => 1) : worker
      bringup = RunpodOllamaFleet::WorkerBringupIdentity.new(
        campaign_identity_sha256: "c" * 64, profile: profile, worker: bound_worker,
        generation_id: bound_worker.fetch("generation_id"), capability_request: request
      )
      RunpodOllamaFleet::WorkerBringupState.new(root: root, clock: -> { NOW }).with_current(bringup) do |record, _persist|
        unless scenario == "not_ready"
          record["readiness_prerequisites_satisfied"] = true
          record["overall_status"] = "prerequisites_passed"
          RunpodOllamaFleet::WorkerBringupState::STAGES.each { |stage| record.fetch(stage)["status"] = "passed" }
        end
      end
      gate = RunpodOllamaFleet::WorkerBringupReadinessGate.new(
        root: root, campaign_identity_sha256: "c" * 64, requirements: { "main" => request }
      )
      evidence_model = model.merge("size_bytes" => 30_000, "size_vram_bytes" => 30_000)
      evidence_model["digest"] = "b" * 64 if scenario == "invalid_capability"
      bootstrap = {
        "fleet_id" => "fake-fleet",
        "status" => "passed", "models" => [model.fetch("model")], "context" => 131_072,
        "expected_digests" => { model.fetch("model") => model.fetch("digest") },
        "workers" => [worker.slice("index", "pod_id", "worker_id", "generation_id").merge(
          "status" => "passed", "provenance_error" => nil,
          "provenance" => { "gpu" => { "name" => gpu }, "models" => { model.fetch("model") => evidence_model } }
        )]
      }
      FileUtils.mkdir_p(File.join(root, "bootstrap", "fixture"))
      File.write(File.join(root, "bootstrap", "current"), "fixture\n")
      File.write(File.join(root, "bootstrap", "fixture", "bootstrap.json"), JSON.generate(bootstrap))
      FileUtils.mkdir_p(File.join(root, "tunnels"))
      tunnel = worker.slice("index", "pod_id", "worker_id", "generation_id").merge(
        "endpoint" => identity.fetch("endpoint"), "pid" => 123, "process_identity" => { "scripted" => true }
      )
      File.write(File.join(root, "tunnels", "tunnels.json"), JSON.generate("fleet_id" => "fake-fleet", "workers" => [tunnel]))
      publisher = RunpodOllamaFleet::DynamicWorkerRegistry.new(
        state_root: File.join(root, "publisher"), repo_root: root, clock: -> { NOW },
        fleet_sources: [{ "fleet_key" => "main", "state" => state }], readiness_gate: gate,
        id_generator: -> { "rpof-acceptance" },
        process_adapter: ScriptedPort.new(alive?: ->(pid) { pid == 123 }, matches?: ->(pid, _) { pid == 123 }),
        health_checker: ScriptedPort.new(check: lambda { |endpoint|
          raise "unscripted endpoint" unless endpoint == identity.fetch("endpoint")
          Struct.new(:healthy).new(true)
        })
      )
      begin
        snapshots[scenario] = publisher.snapshot
        raise "invalid capability published" if scenario == "invalid_capability"
      rescue RunpodOllamaFleet::DynamicWorkerRegistry::Error => e
        raise unless scenario == "invalid_capability" && e.message.include?("digest evidence conflicts")
        checks["producer_rejected_invalid_capability"] = true
      end
    end
  end
end
Fo24aOfflineGuard.assert_clean!
File.write(destination, JSON.generate("snapshots" => snapshots, "checks" => checks,
  "fingerprint" => request.fingerprint, "loaded_features" => $LOADED_FEATURES))
