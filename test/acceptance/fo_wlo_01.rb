# frozen_string_literal: true

# Explicit composed focused test; sibling implementations are never dependencies
# of WLO's ordinary suite. No external runtime, inference, or provider calls.
require_relative "../test_helper"
require "open3"

afw_root = File.expand_path(ENV.fetch("FO01_AFW_ROOT", "../../../af-workloads"), __dir__)
low_root = File.expand_path(ENV.fetch("FO01_LOW_ROOT", "../../../local-ollama-workers"), __dir__)
{ afw_root => "75920d931b505944161de151c53a0c701ddafd0a",
  low_root => "7a169b788fcff0d6fa4c2a41fe6349abb23b703e" }.each do |root, revision|
  stdout, stderr, status = Open3.capture3("git", "-C", root, "rev-parse", "HEAD")
  raise "unpinned dependency #{root}: #{stdout} #{stderr}" unless status.success? && stdout.strip == revision
end
require File.join(afw_root, "lib/af_workloads")
%w[error contract worker_record publisher_state registry_publisher].each do |component|
  require File.join(low_root, "lib/local_ollama_workers", component)
end

# Actual AFW public compilation -> exact artifact -> WLO ingestion/dispatch,
# with real LOW fingerprint/publication and only synthetic external boundaries.
class FoWlo01Acceptance < Minitest::Test
  include WloTestSupport
  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  # Supply deterministic catalog/scorer metadata at AFW's declared boundary.
  # Returns only synthetic interpretation/runtime data; performs no runtime IO.
  class CatalogFixture
    # Return one synthetic case for the provided catalog path; no exceptions.
    def interpretation(_catalog)
      dimension = {
        "name" => "Combat Emphasis", "slug" => "combat", "profile" => "synthetic",
        "model_alias" => "fixture", "qualification_path" => "qualification.yml", "runtime_key" => "base"
      }
      { "adventure_order" => ["ADV-0001"],
        "selected_adventures" => [{ "id" => "ADV-0001", "needs_levels" => false }],
        "dimension_contracts" => [dimension], "levels_contract" => dimension.merge("name" => "Levels") }
    end

    # Return the synthetic scorer revision; no Git or scorer process is run.
    def scorer_check!
      "b" * 40
    end

    # Return a fixed scorer policy, independent of scheduling context; no errors.
    def runtime_payloads
      { "base" => { "source" => {}, "llm" => { "max_tokens" => 8192 } } }
    end
  end

  # Retain all disposable producer, publisher, selected-context and log evidence
  # for review, even on failure. No production paths or credentials are used.
  def setup
    @root = File.realpath(Dir.mktmpdir("fo-wlo-01-"))
    FileUtils.mkdir_p(File.join(@root, "config"))
    FileUtils.mkdir_p(File.join(@root, "scorer/bin"))
    File.write(File.join(@root, "catalog.xlsx"), "synthetic catalog\n")
    File.write(File.join(@root, "qualification.yml"), "synthetic qualification; not production approval\n")
    File.write(File.join(@root, "scorer/bin/af-score"), "#!/bin/sh\nexit 99\n")
    File.chmod(0o755, File.join(@root, "scorer/bin/af-score"))
    File.write(File.join(@root, "config/models.yml"), YAML.dump("models" => {
      "fixture" => { "ollama_model" => "fixture-model", "qualified_manifest_sha256" => "a" * 64 }
    }))
  end

  # Print the retained synthetic evidence location; never delete failure evidence.
  def teardown
    warn "FO-WLO-01 synthetic evidence: #{@root}"
  end

  # Real producer selection must establish every matrix edge at real dispatch,
  # including legacy/default exact semantics. No hand-authored plan is accepted.
  def test_real_producer_to_dispatch_context_matrix
    selections = [
      [nil, :required_context_length, 131_072, [true, false]],
      ["0.4", :required_context_length, 131_072, [true, false]],
      ["0.4", :minimum_context_length, 131_072, [true, true]],
      ["0.4", :minimum_context_length, 262_144, [false, true]]
    ]
    selections.each_with_index do |(version, field, context, expected), selection|
      artifact = compile("synthetic-#{selection}", version: version, field: field, context: context)
      bytes = File.binread(artifact)
      plan = WorkloadOrchestrator::Plan.load(artifact)
      assert_equal bytes, plan.bytes
      assert_equal Digest::SHA256.hexdigest(bytes), plan.sha256
      snapshot = YAML.safe_load_file(File.join(File.dirname(artifact), "snapshot.yml"))
      assert_equal plan.sha256, snapshot.fetch("execution_plan_sha256")
      assert_equal version || "0.3", plan.contract_version.delete_prefix("wlo-execution-plan/v")
      [131_072, 262_144].each_with_index do |observed_context, capacity|
        dispatch(plan, observed_context, expected.fetch(capacity), "#{selection}-#{capacity}")
        assert_equal bytes, File.binread(artifact)
      end
    end
  end

  private

  # Compile a complete disposable workload with real AFW contract/provenance
  # writers and synthetic catalog/source/scorer inputs; returns exact plan path.
  # AFW validation exceptions propagate with evidence retained in @root.
  def compile(workload_id, version:, field:, context:)
    manifest = AFWorkloads::WorkloadCompiler.new(
      root: @root, workload_id: workload_id, catalog_path: File.join(@root, "catalog.xlsx"), ids: ["ADV-0001"],
      dimensions: ["Combat Emphasis"], scorer_repo: File.join(@root, "scorer"), plan_version: version,
      **{ field => context }, require_fully_gpu_resident: true, required_gpu_id: "synthetic GPU",
      batch_factory: -> { CatalogFixture.new }, git_head_resolver: ->(_repo) { "c" * 40 },
      source_preparer: ->(destination:, **_args) { File.write(destination, "synthetic prepared source\n") },
      preflight_runner: lambda do |destination:, prepared_source_path:, **_args|
        raise "missing synthetic source" unless File.file?(prepared_source_path)

        File.write(destination, JSON.generate("preflight" => "synthetic"))
      end,
      out: StringIO.new
    ).build
    File.join(manifest, "execution-plan.json")
  end

  # Publish a synthetic observed context through pinned LOW, consume it via WLO's
  # accepted registry, and dispatch through Runner. Only opaque inference argv is
  # faked; durable attempt evidence must already exist before that boundary.
  def dispatch(plan, observed_context, eligible, name)
    output = File.join(@root, "executions", name)
    publisher = LocalOllamaWorkers::RegistryPublisher.new(
      state_root: File.join(@root, "publishers", name), clock: -> { NOW }, id_generator: -> { "synthetic-registry" },
      worker_source: -> { [observation(observed_context)] }
    )
    published = JSON.generate(publisher.snapshot)
    FileUtils.mkdir_p(output)
    File.binwrite(File.join(@root, "publisher-#{name}.json"), published)
    calls = []
    executor = lambda do |environment, *argv, chdir:|
      metadata = JSON.parse(File.binread(File.join(output, "runs", plan.jobs.first.id, "metadata.json")))
      calls << metadata
      assert_equal "running", metadata.fetch("status")
      assert_equal plan.jobs.first.argv, argv
      assert_equal @root, chdir
      assert_equal "http://127.0.0.1:1", environment.fetch("WLO_WORKER_ENDPOINT")
      assert_equal observation(observed_context).fetch("capabilities"), metadata.dig("worker_snapshot", "capabilities")
      assert_equal 1, metadata.dig("worker_registry_binding", "registry_revision")
      assert_equal Digest::SHA256.hexdigest(published), metadata.dig("worker_registry_binding", "registry_snapshot_sha256")
      assert_equal JSON.parse(published).dig("workers", 0, "capability_fingerprint"),
                   metadata.dig("worker_execution_identity", "capability_fingerprint")
      assert_equal "synthetic-registry", metadata.dig("worker_execution_identity", "registry_id")
      assert_equal "synthetic-worker", metadata.dig("worker_execution_identity", "worker_id")
      assert_equal "synthetic-generation", metadata.dig("worker_execution_identity", "generation_id")
      command_result(stdout: "synthetic inference boundary\x00\n", stderr: "synthetic stderr\n")
    end
    runner = nil
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    sleeper = lambda do |*_args|
      raise "synthetic dispatch exceeded two seconds" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      eligible ? sleep(0.001) : runner.store.pause!
    end
    runner = WorkloadOrchestrator::Runner.new(
      plan: plan, workers: WorkloadOrchestrator::WorkerSet.new({}), workdir: @root, output_dir: output,
      worker_source: WorkloadOrchestrator::StaticWorkerSource.new(published), command_executor: executor,
      worker_registry_clock: -> { NOW }, worker_registry_sleeper: sleeper,
      out: StringIO.new
    )
    assert_equal eligible ? "completed" : "paused", runner.run
    assert_equal eligible ? 1 : 0, calls.length
    assert_equal plan.bytes, File.binread(File.join(output, "plan.json"))
    if eligible
      assert_equal "synthetic inference boundary\x00\n", File.binread(File.join(output, "runs", plan.jobs.first.id, "stdout.log"))
      assert_equal "synthetic stderr\n", File.binread(File.join(output, "runs", plan.jobs.first.id, "stderr.log"))
    else
      refute_path_exists File.join(output, "runs", plan.jobs.first.id, "metadata.json")
    end
  end

  # Return a factual synthetic observation at the external runtime boundary.
  # A model maximum/qualification context is deliberately absent; no IO/errors.
  def observation(context)
    { "worker_id" => "synthetic-worker", "generation_id" => "synthetic-generation",
      "endpoint" => "http://127.0.0.1:1", "state" => "READY", "labels" => %w[inference local ollama],
      "capabilities" => { "gpu_id" => "synthetic GPU", "ollama" => { "models" => [{
        "model" => "fixture-model", "digest" => "a" * 64, "context_length" => context, "fully_gpu_resident" => true
      }] } } }
  end
end
