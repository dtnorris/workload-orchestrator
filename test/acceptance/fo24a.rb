#!/usr/bin/env ruby
# frozen_string_literal: true

# Run explicitly (not inside the ordinary unit suite):
# AF_PROJECT_ROOT=/workspace ruby test/acceptance/fo24a.rb
# or ruby test/acceptance/fo24a.rb /path/to/LOW /path/to/RPOF
# Each producer and WLO runs in a fresh process with only its own lib tree.
require_relative "fo24a/offline_guard"
require "json"
require "tmpdir"
require "fileutils"
require "digest"
require "time"
require "timeout"
require_relative "../../contracts/dynamic-worker-registry/v0.1/conformance"

ROOT = File.expand_path("../..", __dir__)
SUPPORT = File.join(__dir__, "fo24a")
CONTRACT = File.join(ROOT, "contracts/dynamic-worker-registry/v0.1")
workspace = ENV["AF_PROJECT_ROOT"]
low = ARGV[0] || (workspace && File.join(workspace, "local-ollama-workers"))
rpof = ARGV[1] || (workspace && File.join(workspace, "runpod-ollama-fleet"))
abort "Pass LOW and RPOF roots, or set AF_PROJECT_ROOT" unless low && rpof && ARGV.length <= 2
roots = { "wlo" => ROOT, "low" => File.expand_path(low), "rpof" => File.expand_path(rpof) }
roots.each_value { |path| abort "missing component lib: #{path}" unless File.directory?(File.join(path, "lib")) }
Fo24aOfflineGuard.self_check!

# Providers may copy the public corpus but never create a competing format.
manifest = File.readlines(File.join(CONTRACT, "SHA256SUMS"), chomp: true)
manifest.each do |line|
  expected, path = line.split(/\s+/, 2)
  [CONTRACT, File.join(low, "test/fixtures/dynamic-worker-registry-v0.1"),
   File.join(rpof, "test/fixtures/dynamic-worker-registry-v0.1")].each do |corpus|
    raise "conformance corpus drift: #{corpus}/#{path}" unless Digest::SHA256.file(File.join(corpus, path)).hexdigest == expected
  end
end
puts "PASS shared BD-03 conformance corpus and hashes"

# Production-only structural boundary check, independent of loaded test state.
forbidden = {
  "wlo" => /local[_-]ollama[_-]workers|runpod[_-]ollama[_-]fleet|LocalOllamaWorkers|LocalModelEvaluation|RunpodOllamaFleet|AdventureFinder|AfWorkloads|af[_-]workloads/i,
  "low" => /workload[_-]orchestrator|WorkloadOrchestrator|runpod[_-]ollama[_-]fleet|RunpodOllamaFleet|AdventureFinder|AfWorkloads|af[_-]workloads/i,
  "rpof" => /workload[_-]orchestrator|WorkloadOrchestrator|local[_-]ollama[_-]workers|LocalOllamaWorkers|AdventureFinder::|AfWorkloads|af[_-]workloads/i
}
roots.each do |owner, root|
  paths = Dir[File.join(root, "{lib,bin}", "**", "*")].select { |path| File.file?(path) }
  paths.each do |path|
    File.readlines(path).each_with_index do |line, index|
      next if line.lstrip.start_with?("#")
      raise "#{owner}: sibling implementation reference #{path}:#{index + 1}" if line.match?(forbidden.fetch(owner))
    end
  end
  puts "PASS #{owner} production dependency boundary (#{paths.length} files)"
end

run_child = lambda do |script, repo, input, destination, home, kind = nil|
  argv = [RbConfig.ruby, File.join(SUPPORT, script)]
  argv << kind if kind
  argv.concat([repo, input, destination])
  environment = {
    "HOME" => home, "PATH" => File.dirname(RbConfig.ruby),
    "RUBYOPT" => "-r#{File.join(SUPPORT, 'offline_guard.rb')}"
  }
  out, err, status = Fo24aOfflineGuard.permit(argv) do
    Timeout.timeout(45) { Open3.capture3(environment, *argv, chdir: repo, unsetenv_others: true) }
  end
  raise "#{script} failed: #{out}\n#{err}" unless status.success?
  JSON.parse(File.binread(destination))
end

copy = ->(value) { JSON.parse(JSON.generate(value)) }
request = JSON.parse(File.binread(File.join(ROOT, "contracts/ollama-capability-request/v0.1/canonical-valid.json")))
providers = {}
cases = {}
Dir.mktmpdir("fo24a-") do |root|
  request_path = File.join(root, "request.json")
  File.write(request_path, JSON.generate(request))
  %w[low rpof].each do |kind|
    # This directory contains only the provider under test. It is removed before
    # WLO starts, so WLO cannot retrieve evidence from provider runtime files.
    Dir.mktmpdir("fo24a-isolated-#{kind}-") do |isolated|
      FileUtils.cp_r(File.join(roots.fetch(kind), "lib"), isolated)
      providers[kind] = run_child.call("provider.rb", isolated, request_path,
        File.join(root, "#{kind}.json"), root, kind)
    end
    result = providers.fetch(kind)
    loaded = result.fetch("loaded_features").grep(%r{/(?:lib|bin)/}).join("\n")
    raise "#{kind}: sibling runtime loaded" if loaded.match?(forbidden.fetch(kind))
    snapshots = result.fetch("snapshots")
    snapshots.each do |name, snapshot|
      DynamicWorkerRegistryV01::Conformance.validate_document!(snapshot, now: Time.utc(2030, 1, 1, 0, 1, 0))
      dispatch = %w[valid generation_2 private_details].include?(name)
      cases["#{kind}-#{name}"] = { "snapshot" => snapshot, "outcome" => dispatch ? "success" : "no_dispatch" }
    end
    raise "private implementation detail leaked" unless snapshots.fetch("valid") == snapshots.fetch("private_details")
    raise "provenance altered runtime identity" unless result.dig("checks", "external_provenance_identity").uniq == [result.fetch("fingerprint")]
    raise "provider identity mismatch" unless snapshots.dig("valid", "workers", 0, "worker_id") == "#{kind}-worker"
    puts "PASS #{kind} real publication, capability admission, readiness, private-detail isolation"
  end
  raise "provider capability semantic drift" unless providers.fetch("low").fetch("checks").reject { |k, _| k.start_with?("producer_") } ==
    providers.fetch("rpof").fetch("checks").reject { |k, _| k.start_with?("producer_") }

  neutral = JSON.parse(File.binread(File.join(CONTRACT, "minimal-valid.json")))
  neutral["registry_id"] = "neutral-acceptance"
  cases["neutral-valid"] = { "snapshot" => neutral, "outcome" => "success" }
  newer = copy.call(neutral)
  newer.fetch("workers").first["generation_id"] = "new-neutral-generation"
  cases["neutral-generation_2"] = { "snapshot" => newer, "outcome" => "success" }
  cases["neutral-failure"] = { "snapshot" => neutral, "outcome" => "failure" }

  %w[neutral low rpof].each do |origin|
    base = cases.fetch("#{origin}-valid").fetch("snapshot")
    mutations = {
      "missing-worker-id" => ->(d, w, m) { w.delete("worker_id") },
      "missing-generation" => ->(d, w, m) { w.delete("generation_id") },
      "bad-endpoint" => ->(d, w, m) { w["endpoint"] = "ssh://not-ollama" },
      "bad-fingerprint" => ->(d, w, m) { w["capability_fingerprint"] = "f" * 64 },
      "model-mismatch" => ->(d, w, m) { m["model"] = "other:latest" },
      "digest-mismatch" => ->(d, w, m) { m["digest"] = "b" * 64 },
      "context-insufficient" => ->(d, w, m) { m["context_length"] = 65_536 },
      "context-higher" => ->(d, w, m) { m["context_length"] = 262_144 },
      "residency-false" => ->(d, w, m) { m["fully_gpu_resident"] = false },
      "residency-missing" => ->(d, w, m) { m.delete("fully_gpu_resident") },
      "gpu-mismatch" => ->(d, w, m) { w.fetch("capabilities")["gpu_id"] = "Other GPU" },
      "stale" => ->(d, w, m) { d["expires_at"] = "2030-01-01T00:01:00Z"; d["published_at"] = "2030-01-01T00:00:00Z" },
      "unavailable" => ->(d, w, m) { w["state"] = "UNAVAILABLE" },
      "not-ready" => ->(d, w, m) { w["state"] = "NOT_READY" },
      "duplicate-worker" => ->(d, w, m) { d["workers"] << copy.call(w) },
      "duplicate-endpoint" => ->(d, w, m) { d["workers"] << copy.call(w).merge("worker_id" => "distinct-id") },
      "unknown-provider-field" => ->(d, w, m) { w["provider_active"] = true },
      "invalid-id" => ->(d, w, m) { w["worker_id"] = "invalid/id" },
      "unsorted-labels" => ->(d, w, m) { w["labels"].reverse! },
      "noncanonical-time" => ->(d, w, m) { d["published_at"] = "2030-01-01T00:00:00+00:00" }
    }
    mutations.each do |name, mutation|
      snapshot = copy.call(base)
      worker = snapshot.fetch("workers").first
      mutation.call(snapshot, worker, worker.dig("capabilities", "ollama", "models").first)
      unless %w[bad-fingerprint residency-missing].include?(name)
        worker["capability_fingerprint"] = DynamicWorkerRegistryV01::Conformance.capability_fingerprint(worker)
      end
      cases["#{origin}-wire-#{name}"] = { "snapshot" => snapshot, "outcome" => "no_dispatch" }
    end
  end

  # Existing malformed corpus is also tested through real WLO execution.
  Dir[File.join(CONTRACT, "invalid", "*.json")].sort.each do |path|
    cases["corpus-#{File.basename(path, '.json')}"] = {
      "snapshot" => JSON.parse(File.binread(path)), "outcome" => "no_dispatch"
    }
  end
  input = File.join(root, "consumer-input.json")
  File.write(input, JSON.generate("request" => request, "cases" => cases))
  result = nil
  Dir.mktmpdir("fo24a-isolated-wlo-") do |isolated|
    FileUtils.cp_r(File.join(ROOT, "lib"), isolated)
    raise "unexpected sibling checkout" unless Dir.children(isolated) == ["lib"]
    result = run_child.call("consumer.rb", isolated, input, File.join(root, "consumer-result.json"), root)
  end
  raise "capability identity differs across components" unless providers.values.all? { |p| p.fetch("fingerprint") == result.fetch("fingerprint") }
  loaded = result.fetch("loaded_features").grep(%r{/(?:lib|bin)/}).join("\n")
  raise "WLO loaded sibling runtime" if loaded.match?(forbidden.fetch("wlo"))
  result.fetch("results").each { |name, row| puts "PASS #{name}: #{row.fetch('execution_status')}/#{row.fetch('job_status')}" }
  puts "PASS exact generation and complete attempt registry identity (neutral/LOW/RPOF)"
  puts "PASS AdventureFinder and AFW absent; provider implementations absent from WLO execution"
  puts "PASS #{cases.length} execution cases; no network, credentials, provider mutations, or inference"
end
Fo24aOfflineGuard.assert_clean!
puts "FO-24A PASS (mixed-source fault injection remains FO-24B)"
