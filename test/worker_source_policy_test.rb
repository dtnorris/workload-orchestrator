# frozen_string_literal: true

require_relative "test_helper"

class WorkerSourcePolicyTest < Minitest::Test
  include WloTestSupport

  START = Time.iso8601("2030-01-01T00:00:30Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(*values)
      super()
      @values = values
      @calls = 0
    end

    def latest_snapshot
      value = @values.fetch([@calls, @values.length - 1].min)
      @calls += 1
      raise value if value.is_a?(Exception)

      value
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-source-policy-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    @now = START
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_v02_requires_explicit_policy_and_v01_remains_required
    legacy = load_config(
      WorkloadOrchestrator::WorkerSourceConfiguration::LEGACY_CONTRACT_VERSION,
      [config_row("legacy", policy: nil)]
    )
    current = load_config(
      WorkloadOrchestrator::WorkerSourceConfiguration::CONTRACT_VERSION,
      [config_row("required", policy: "required"), config_row("optional", policy: "optional")]
    )

    assert_equal ["required"], legacy.entries.map(&:policy)
    assert_equal %w[required optional], current.entries.map(&:policy)
    error = assert_raises(WorkloadOrchestrator::Error) do
      load_config(
        WorkloadOrchestrator::WorkerSourceConfiguration::CONTRACT_VERSION,
        [config_row("missing", policy: nil)]
      )
    end
    assert_includes error.message, "fields are invalid"
  end

  def test_two_healthy_sources_contribute_registry_qualified_workers
    set = registry_set(
      "required" => ["required", source(snapshot("required-registry", 1, "shared", "model-a", 11_441))],
      "optional" => ["optional", source(snapshot("optional-registry", 1, "shared", "model-b", 11_442))]
    ).poll_once

    assert_equal %w[optional-registry required-registry], set.current_workers.map(&:registry_id).sort
    assert_equal %w[shared shared], set.current_workers.map(&:worker_id)
    refute set.dispatch_blocked?
    assert_equal 2, set.accepted_checkpoints.length
  end

  def test_optional_poll_failure_retains_fresh_snapshot_without_blocking
    optional = source(
      snapshot("optional-registry", 7, "optional-worker", "model-b", 11_442),
      WorkloadOrchestrator::Error.new("optional publisher failed")
    )
    set = registry_set(
      "required" => ["required", source(snapshot("required-registry", 1, "required-worker", "model-a", 11_441))],
      "optional" => ["optional", optional]
    ).poll_once
    digest = set.source_checkpoints.fetch("optional").fetch("snapshot_sha256")

    set.poll_once

    assert_equal %w[optional-worker required-worker], set.ready_workers.map(&:worker_id).sort
    health = set.source_health.fetch("optional")
    assert_equal "failure", health.fetch("last_poll_result")
    assert_equal "fresh", health.fetch("state")
    assert health.fetch("usable")
    refute health.fetch("blocking")
    assert_equal digest, set.source_checkpoints.fetch("optional").fetch("snapshot_sha256")
  end

  def test_optional_expiry_removes_only_its_capacity_and_recovery_restores_it
    optional = source(
      snapshot("optional-registry", 7, "optional-worker", "model-b", 11_442,
               expires_at: "2030-01-01T00:01:00Z"),
      WorkloadOrchestrator::Error.new("optional publisher failed"),
      WorkloadOrchestrator::Error.new("optional publisher still failed"),
      snapshot("optional-registry", 8, "optional-worker", "model-b", 11_442,
               published_at: "2030-01-01T00:01:01Z", expires_at: "2030-01-01T00:03:00Z")
    )
    set = registry_set(
      "required" => ["required", source(snapshot("required-registry", 1, "required-worker", "model-a", 11_441))],
      "optional" => ["optional", optional]
    ).poll_once

    @now = Time.iso8601("2030-01-01T00:00:45Z")
    set.poll_once
    assert_equal %w[optional-worker required-worker], set.current_workers.map(&:worker_id).sort

    @now = Time.iso8601("2030-01-01T00:01:01Z")
    set.poll_once
    assert_equal ["required-worker"], set.current_workers.map(&:worker_id)
    assert_equal "stale", set.source_health.dig("optional", "state")
    refute set.dispatch_blocked?

    @now = Time.iso8601("2030-01-01T00:01:02Z")
    set.poll_once
    assert_equal %w[optional-worker required-worker], set.current_workers.map(&:worker_id).sort
    assert_equal "success", set.source_health.dig("optional", "last_poll_result")
  end

  def test_required_source_blocks_only_after_expiry_and_recovers_automatically
    required = source(
      snapshot("required-registry", 1, "required-worker", "model-a", 11_441,
               expires_at: "2030-01-01T00:01:00Z"),
      WorkloadOrchestrator::Error.new("required publisher failed"),
      WorkloadOrchestrator::Error.new("required publisher still failed"),
      snapshot("required-registry", 2, "required-worker", "model-a", 11_441,
               published_at: "2030-01-01T00:01:01Z", expires_at: "2030-01-01T00:03:00Z")
    )
    set = registry_set("required" => ["required", required]).poll_once

    @now = Time.iso8601("2030-01-01T00:00:45Z")
    set.poll_once
    refute set.dispatch_blocked?
    assert_equal ["required-worker"], set.ready_workers.map(&:worker_id)

    @now = Time.iso8601("2030-01-01T00:01:01Z")
    set.poll_once
    assert set.dispatch_blocked?
    assert_empty set.ready_workers
    assert_equal ["required"], set.blocking_sources.map { |row| row.fetch("source_name") }

    @now = Time.iso8601("2030-01-01T00:01:02Z")
    set.poll_once
    refute set.dispatch_blocked?
    assert_equal ["required-worker"], set.ready_workers.map(&:worker_id)
  end

  def test_required_source_recovery_resumes_runner_dispatch_without_restart
    plan = plan_for(["job"])
    required = source(
      WorkloadOrchestrator::Error.new("required publisher unavailable"),
      snapshot("required-registry", 1, "required-worker", "model-a", 11_441)
    )
    sources = WorkloadOrchestrator::WorkerSourceSet.new([
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name: "required", source: required, policy: "required"
      )
    ])
    runner = WorkloadOrchestrator::Runner.new(
      plan:,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: sources,
      worker_registry_clock: -> { @now },
      worker_registry_sleeper: ->(*) { Thread.pass },
      worker_poll_interval: 0.001,
      command_executor: ->(*) { command_result }
    )

    assert_equal "completed", runner.run
    assert_operator required.calls, :>=, 2
    assert_equal "required-registry",
                 runner.store.metadata_for(plan.jobs.first).dig("worker_execution_identity", "registry_id")
    assert_equal "fresh", runner.worker_registry_poller.source_health.dig("required", "state")
    refute runner.worker_registry_poller.dispatch_blocked?
  end

  def test_set_level_registry_collision_remains_a_fatal_runner_error
    plan = plan_for(["job"])
    sources = WorkloadOrchestrator::WorkerSourceSet.new([
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name: "one", source: source(snapshot("shared-registry", 1, "one", "model-a", 11_441)),
        policy: "required"
      ),
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name: "two", source: source(snapshot("shared-registry", 1, "two", "model-a", 11_442)),
        policy: "optional"
      )
    ])
    runner = WorkloadOrchestrator::Runner.new(
      plan:,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: sources,
      worker_registry_clock: -> { @now },
      worker_registry_sleeper: ->(*) { raise "unexpected sleep" },
      command_executor: ->(*) { raise "unexpected dispatch" }
    )

    error = assert_raises(WorkloadOrchestrator::Error) { runner.run }

    assert_includes error.message, "registry_id"
    assert_equal "worker_registry", runner.store.dispatch_halt.fetch("kind")
    assert_includes runner.store.dispatch_halt.fetch("error"), "shared-registry"
  end

  def test_set_level_error_during_interrupt_records_the_interruption
    plan = plan_for(["job"])
    runner = nil
    interrupted_snapshot = snapshot("shared-registry", 1, "two", "model-a", 11_442)
    interrupted_source = WorkloadOrchestrator::WorkerSource.new
    interrupted_source.define_singleton_method(:latest_snapshot) do
      runner.instance_variable_set(:@interrupt_signal, "INT")
      interrupted_snapshot
    end
    sources = WorkloadOrchestrator::WorkerSourceSet.new([
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name: "one", source: source(snapshot("shared-registry", 1, "one", "model-a", 11_441)),
        policy: "required"
      ),
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name: "two", source: interrupted_source, policy: "optional"
      )
    ])
    runner = WorkloadOrchestrator::Runner.new(
      plan:,
      workers: WorkloadOrchestrator::WorkerSet.new({}),
      workdir: @workdir,
      output_dir: @output,
      out: StringIO.new,
      worker_source: sources,
      worker_registry_clock: -> { @now },
      worker_registry_sleeper: ->(*) { raise "unexpected sleep" }
    )

    assert_raises(WorkloadOrchestrator::Error) { runner.run }
    assert_equal "interrupted", runner.store.status
    assert_nil runner.store.dispatch_halt
  end

  def test_invalid_optional_replacements_do_not_mutate_accepted_state_or_healthy_source
    changed = worker("optional-worker", "changed-model", 11_442)
    optional = source(
      snapshot("optional-registry", 7, "optional-worker", "model-b", 11_442),
      "{",
      snapshot("optional-registry", 6, "optional-worker", "model-b", 11_442,
               published_at: "2030-01-01T00:00:20Z"),
      registry_document("optional-registry", 7, [changed], published_at: "2030-01-01T00:00:00Z")
    )
    required = source(
      snapshot("required-registry", 1, "required-worker", "model-a", 11_441),
      snapshot("required-registry", 2, "required-worker", "model-a", 11_441,
               published_at: "2030-01-01T00:00:10Z"),
      snapshot("required-registry", 3, "required-worker", "model-a", 11_441,
               published_at: "2030-01-01T00:00:20Z"),
      snapshot("required-registry", 4, "required-worker", "model-a", 11_441,
               published_at: "2030-01-01T00:00:40Z")
    )
    set = registry_set(
      "required" => ["required", required], "optional" => ["optional", optional]
    ).poll_once
    optional_checkpoint = File.binread(checkpoint_path("optional"))
    @now = Time.iso8601("2030-01-01T00:01:00Z")

    3.times do |index|
      set.poll_once
      assert_equal optional_checkpoint, File.binread(checkpoint_path("optional")), index
      assert_equal index + 2, set.source_checkpoints.dig("required", "revision"), index
      assert_equal "failure", set.source_health.dig("optional", "last_poll_result"), index
    end
    assert_includes set.source_health.dig("optional", "failure_reason"), "changed contents"
  end

  def test_expired_source_marks_only_its_registry_bound_attempt_in_doubt
    plan = plan_for(%w[required-job optional-job])
    store = prepared_store(plan)
    shared_required = snapshot("required-registry", 1, "shared", "model-a", 11_441)
    shared_optional = snapshot(
      "optional-registry", 1, "shared", "model-a", 11_442,
      expires_at: "2030-01-01T00:01:00Z"
    )
    set = registry_set(
      "required" => ["required", source(shared_required)],
      "optional" => ["optional", source(
        shared_optional,
        WorkloadOrchestrator::Error.new("optional publisher failed"),
        WorkloadOrchestrator::Error.new("optional publisher failed")
      )]
    ).poll_once
    by_registry = set.current_workers.to_h { |entry| [entry.registry_id, entry] }
    store.record_dynamic_running!(
      job: plan.jobs.fetch(0), worker: by_registry.fetch("required-registry"), environment_keys: []
    )
    store.record_dynamic_running!(
      job: plan.jobs.fetch(1), worker: by_registry.fetch("optional-registry"), environment_keys: []
    )
    reconciler = WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store:)

    @now = Time.iso8601("2030-01-01T00:00:45Z")
    set.poll_once
    assert_empty reconciler.reconcile!(set)

    @now = Time.iso8601("2030-01-01T00:01:01Z")
    set.poll_once
    events = reconciler.reconcile!(set)

    assert_equal ["optional-job"], events.map { |event| event.fetch("job_id") }
    assert_equal "worker_source_expired", events.first.fetch("reason")
    assert_equal "running", store.metadata_for(plan.jobs.fetch(0)).fetch("status")
    assert_equal "failed", store.metadata_for(plan.jobs.fetch(1)).fetch("status")
  end

  def test_status_exposes_policy_health_and_required_blocking_reason
    plan = plan_for(["pending-job"])
    prepared_store(plan)
    required = source(
      snapshot("required-registry", 1, "required-worker", "model-a", 11_441,
               expires_at: "2030-01-01T00:01:00Z"),
      WorkloadOrchestrator::Error.new("required publisher failed")
    )
    optional = source(snapshot("optional-registry", 1, "optional-worker", "model-a", 11_442))
    set = registry_set(
      "required" => ["required", required], "optional" => ["optional", optional]
    ).poll_once

    @now = Time.iso8601("2030-01-01T00:01:01Z")
    set.poll_once
    report = WorkloadOrchestrator::ExecutionReport.new(
      plan:, output: @output, clock: -> { @now }
    )
    document = report.document
    required_health = document.fetch("worker_sources").find do |row|
      row.fetch("source_name") == "required"
    end
    out = StringIO.new
    report.print(out, verbose: true)

    assert_equal "required", required_health.fetch("policy")
    assert_equal "required-registry", required_health.fetch("registry_id")
    assert_equal 1, required_health.fetch("last_accepted_revision")
    assert_equal "stale", required_health.fetch("state")
    assert required_health.fetch("blocking")
    assert_equal "REQUIRED_WORKER_SOURCE_UNAVAILABLE",
                 document.dig("pool_status", 0, "reason")
    assert_includes out.string, "required policy=required state=stale"
  end

  def test_healthy_source_dispatches_and_completes_while_optional_source_is_unavailable
    plan = plan_for(["job"])
    store = prepared_store(plan)
    set = registry_set(
      "required" => ["required", source(snapshot("required-registry", 1, "worker", "model-a", 11_441))],
      "optional" => ["optional", source(WorkloadOrchestrator::Error.new("optional unavailable"))]
    ).poll_once
    scheduler = WorkloadOrchestrator::DynamicScheduler.new(plan:, store:)

    refute set.dispatch_blocked?
    assignment = scheduler.assignments(
      workers: set.ready_workers, current_workers: set.current_workers
    ).fetch(0)
    attempt = store.record_dynamic_running!(
      job: assignment.job, worker: assignment.worker, environment_keys: []
    )
    store.record_dynamic_terminal!(attempt:, status: "complete", exit_status: 0)

    assert_equal "required-registry", assignment.worker.registry_id
    assert_equal "complete", store.metadata_for(plan.jobs.first).fetch("status")
    assert_equal "unavailable", set.source_health.dig("optional", "state")
    refute set.source_health.dig("optional", "blocking")
  end

  private

  def source(*values)
    SequenceSource.new(*values)
  end

  def registry_set(sources)
    entries = sources.map do |name, (policy, source_value)|
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(
        name:, source: source_value, policy:
      )
    end
    WorkloadOrchestrator::WorkerRegistrySet.new(
      sources: WorkloadOrchestrator::WorkerSourceSet.new(entries),
      checkpoint_root: File.join(@output, "dynamic-workers"),
      clock: -> { @now }, interval_seconds: 0.001, sleeper: ->(*) {}
    )
  end

  def checkpoint_path(source_name)
    File.join(@output, "dynamic-workers", "sources", source_name, "checkpoint.json")
  end

  def config_row(name, policy:)
    row = {
      "name" => name,
      "command" => RbConfig.ruby,
      "args" => ["-e", "print '{}'"],
      "environment" => {},
      "workdir" => @workdir
    }
    row["policy"] = policy if policy
    row
  end

  def load_config(version, rows)
    path = File.join(@tmp, "config-#{Digest::SHA256.hexdigest(JSON.generate(rows))}.yml")
    File.write(path, YAML.dump("contract_version" => version, "sources" => rows))
    WorkloadOrchestrator::WorkerSourceConfiguration.load(path)
  end

  def snapshot(registry_id, revision, worker_id, model, port,
               published_at: "2030-01-01T00:00:00Z", expires_at: "2030-01-01T00:05:00Z")
    registry_document(
      registry_id, revision, [worker(worker_id, model, port)],
      published_at:, expires_at:
    )
  end

  def registry_document(registry_id, revision, workers,
                        published_at:, expires_at: "2030-01-01T00:05:00Z")
    JSON.generate(
      "contract_version" => WorkloadOrchestrator::DynamicWorkerRegistry::CONTRACT_VERSION,
      "registry_id" => registry_id,
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => expires_at,
      "workers" => workers
    )
  end

  def worker(worker_id, model, port)
    row = {
      "worker_id" => worker_id,
      "generation_id" => "generation-1",
      "endpoint" => "http://127.0.0.1:#{port}",
      "state" => "READY",
      "labels" => ["inference"],
      "capabilities" => {
        "gpu_id" => "fixture-gpu",
        "ollama" => {
          "models" => [{
            "model" => model,
            "digest" => Digest::SHA256.hexdigest(model),
            "context_length" => 131_072,
            "fully_gpu_resident" => true
          }]
        }
      }
    }
    row["capability_fingerprint"] = capability_fingerprint(row)
    row
  end

  def capability_fingerprint(row)
    capabilities = row.fetch("capabilities")
    models = capabilities.dig("ollama", "models").map do |model|
      model.slice("context_length", "digest", "fully_gpu_resident", "model")
    end
    Digest::SHA256.hexdigest(JSON.generate(
                               "gpu_id" => capabilities.fetch("gpu_id"),
                               "labels" => row.fetch("labels"),
                               "ollama_models" => models
                             ))
  end

  def plan_for(job_ids)
    path = File.join(@tmp, "plan-#{Digest::SHA256.hexdigest(JSON.generate(job_ids))}.json")
    File.write(path, JSON.generate(
                       "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                       "plan_id" => "source-policy-fixture",
                       "failure_policy" => {
                         "max_consecutive_failures" => 10,
                         "max_total_failures" => 10
                       },
                       "pools" => [{
                         "pool_id" => "model-a",
                         "required_labels" => ["inference"],
                         "requirements" => {
                           "ollama" => {
                             "model" => "model-a",
                             "expected_digest" => Digest::SHA256.hexdigest("model-a"),
                             "required_context_length" => 131_072,
                             "require_fully_gpu_resident" => true,
                             "required_gpu_id" => "fixture-gpu"
                           }
                         }
                       }],
                       "jobs" => job_ids.map do |job_id|
                         { "job_id" => job_id, "pool_id" => "model-a", "argv" => ["fixture", job_id] }
                       end
                     ))
    WorkloadOrchestrator::Plan.load(path)
  end

  def prepared_store(plan)
    WorkloadOrchestrator::ExecutionStore.new(
      output_dir: @output, plan:, workdir: @workdir
    ).tap do |store|
      store.prepare!
      store.start!
    end
  end
end
