# frozen_string_literal: true

# Cross-owner composition is confined to this test process. The separate WLO
# consumer process has no provider implementation available at all.
require_relative "../fo24a/offline_guard"
require "timeout"
rpof, wlo, input = ARGV
require "json"
FO24B_PUBLIC = JSON.parse(File.binread(input))
ARGV.clear
require File.join(wlo, "lib/workload_orchestrator")
%w[availability_fallback campaign_capacity_admission campaign_safety_gate
   campaign_budget_binding selected_worker_lifecycle runpod_budget
   runpod_budget_guardian].each { |name| require File.join(rpof, "test", "#{name}_test") }

# Reuse the established real WLO demand -> RPOF consumer-bound drain/removal
# composition, including old request/deadline/stop/concurrency/fallback guards.
ARGV.replace([wlo])
load File.join(rpof, "script/test-fo16-consumer-demand")
ARGV.replace(["--seed", "2402"])

# An accidental real client is a release-acceptance failure before credentials,
# HTTP, or mutation. Existing strict provider fakes do not inherit this class.
LocalModelEvaluation::RunpodClient.define_singleton_method(:new) do |*|
  Fo24aOfflineGuard.deny!("real provider client construction")
end
strict_fake = Module.new do
  def method_missing(name, *) = Fo24aOfflineGuard.deny!("unsupported provider fake method #{name}")
  def respond_to_missing?(*) = false
end
[AvailabilityFallbackTest::Provider, SelectedWorkerFixture::Provider,
 CampaignCapacityAdmissionTest::FakeProvider, CampaignLifecycleTest::Provider,
 RunpodBudgetGuardianTest::FakeProvider].each { |provider| provider.prepend(strict_fake) }

# Bound, exact local Ruby argv from the existing command-source test. Its
# assertions are unchanged and the child inherits the network/credential guard.
ConsumerCapacityTest.prepend(Module.new do
  def test_public_command_source_uses_argv_and_rejects_failed_response
    Fo24aOfflineGuard.permit([RbConfig.ruby, "-e", "puts ARGV.first", JSON.generate(@demand)]) do
      Fo24aOfflineGuard.permit([RbConfig.ruby, "-e", "exit 1"]) { super }
    end
  end

  def test_fo16_public_demand_ready_dispatch_pause_bound_work_expiry_and_retirement
    super
    retained = JSON.parse(File.binread(File.join(@fixture.root, "consumer-output/runs/one/metadata.json")))
    assert_equal "complete", retained.fetch("status")
    assert_equal 1, retained.fetch("attempt")
    assert_equal 0, retained.fetch("exit_status")
    assert_equal @fixture.initial_worker.fetch("worker_id"), retained.dig("worker_execution_identity", "worker_id")
    assert_equal @fixture.initial_worker.fetch("generation_id"), retained.dig("worker_execution_identity", "generation_id")
    assert_equal "READY", retained.dig("worker_snapshot", "state")
    assert_operator Time.iso8601(retained.fetch("completed_at")), :>=, Time.iso8601(retained.fetch("started_at"))
    # The provider has since drained/retired this exact generation; WLO still
    # retains its original READY snapshot and successful attempt identity.
    assert_equal "retired", @fixture.worker.dig("lifecycle", "phase")
  end
end)

class AvailabilityFallbackTest
  def test_fo24b_ambiguous_create_registry_consequence_and_execution_retry_are_independent
    provider = Provider.new([gpu(A40, 0.49), gpu(BLACKWELL, 2.09)])
    provider.behaviors[A40] = :ambiguous
    subject = runtime(provider: provider)
    assert_raises(RuntimeError) { subject.ensure_workers!(**ensure_arguments) }
    before = @binding.parent_budget.status.fetch("reservations")
    assert_equal ["pending"], before.values.map { |row| row.fetch("status") }
    assert_equal "blocked", subject.status.dig("availability_fallback", "state")
    snapshot = fo24b_registry
    assert_empty snapshot.fetch("workers")
    fo24b_consume(snapshot, retry_failure: true)
    assert_raises(RunpodOllamaFleet::CampaignRunpodRuntime::Error) do
      runtime(provider: provider).ensure_workers!(**ensure_arguments)
    end
    assert_equal before, @binding.parent_budget.status.fetch("reservations")
    assert_equal 1, provider.create_bodies.length
    assert_empty provider.deleted_ids
  end

  def test_fo24b_capacity_exhaustion_publishes_no_ready_worker_and_low_remains_usable
    provider = Provider.new([gpu(A40, 0.49, availability: "NONE"), gpu(BLACKWELL, 2.09, availability: "NONE")])
    subject = runtime(provider: provider)
    assert_raises(RunpodOllamaFleet::CampaignRunpodRuntime::Error) { subject.ensure_workers!(**ensure_arguments) }
    assert_equal "exhausted", subject.status.dig("availability_fallback", "state")
    snapshot = fo24b_registry
    assert_empty snapshot.fetch("workers")
    fo24b_consume(snapshot, retry_failure: false)
    assert_empty provider.create_bodies
    assert_empty provider.deleted_ids
  end

  private

  def fo24b_registry
    # Real publisher observes the runtime's own fleet state. With no known
    # generation/capability readiness it cannot invent a WLO READY record.
    state = LocalModelEvaluation::RunpodFleetState.new(root: File.join(@tmp, "fleets", "qwen35"))
    RunpodOllamaFleet::DynamicWorkerRegistry.new(state_root: @tmp, repo_root: @tmp,
      clock: -> { @now }, id_generator: -> { "authority-registry" },
      fleet_sources: [{ "fleet_key" => "qwen35", "state" => state }]).snapshot
  end

  def fo24b_consume(remote, retry_failure:)
    w = WorkloadOrchestrator
    local = Marshal.load(Marshal.dump(FO24B_PUBLIC.fetch("providers").fetch("low").fetch("valid")))
    local["published_at"] = @now.iso8601
    local["expires_at"] = (@now + 60).iso8601
    plan = w::Plan.new(JSON.generate(
      "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "authority-consequence",
      "failure_policy" => { "max_consecutive_failures" => 3, "max_total_failures" => 3 },
      "pools" => [{ "pool_id" => "generic", "requirements" => { "ollama" => FO24B_PUBLIC.fetch("request").fetch("ollama") } }],
      "jobs" => [{ "job_id" => "neutral", "pool_id" => "generic", "argv" => ["scripted-local-only"] }]
    ))
    sources = w::WorkerSourceSet.new([local, remote].each_with_index.map do |doc, index|
      w::WorkerSourceSet::Entry.new(name: "source-#{index}", policy: "optional", source: w::StaticWorkerSource.new(JSON.generate(doc)))
    end)
    calls = 0
    command = lambda do |*|
      calls += 1
      code = retry_failure && calls == 1 ? 7 : 0
      result = Struct.new(:exitstatus) { def success? = exitstatus.zero? }.new(code)
      ["scripted", "", result]
    end
    runner = w::Runner.new(plan: plan, workers: w::WorkerSet.new({}), workdir: @tmp,
      output_dir: File.join(@tmp, "wlo"), out: StringIO.new, worker_source: sources,
      worker_registry_clock: -> { @now }, worker_poll_interval: 0.001, command_executor: command)
    assert_equal(retry_failure ? "workload_failed" : "completed", Timeout.timeout(5) { runner.run })
    original = runner.store.metadata_for(plan.jobs.first)
    assert_equal local.fetch("registry_id"), original.dig("worker_execution_identity", "registry_id")
    assert_equal "generation-1", original.dig("worker_execution_identity", "generation_id")
    assert_equal(retry_failure ? 7 : 0, original.fetch("exit_status"))
    if retry_failure
      runner.store.retry_failed!(all: true, reason: "reviewed execution failure; provider authority unchanged")
      assert_equal "completed", Timeout.timeout(5) { runner.run(resume: true) }
      assert_equal 2, runner.store.metadata_for(plan.jobs.first).fetch("attempt")
      assert_equal original, JSON.parse(File.binread(File.join(@tmp, "wlo/attempts/neutral/attempt-1/metadata.json")))
    end
    assert_equal(retry_failure ? 2 : 1, calls)
  end
end

Minitest.after_run do
  Fo24aOfflineGuard.assert_clean!
end
