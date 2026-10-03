# frozen_string_literal: true

require_relative "test_helper"

class MultiSourceWorkerTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")

  class SequenceSource < WorkloadOrchestrator::WorkerSource
    attr_reader :calls

    def initialize(*snapshots)
      super()
      @snapshots = snapshots
      @calls = 0
    end

    def latest_snapshot
      value = @snapshots.fetch([calls, @snapshots.length - 1].min)
      @calls += 1
      value
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-multi-source-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_arbitrary_source_names_do_not_create_provider_semantics
    %w[alpha archive-zone].each do |name|
      set = load_config([source_row(name)])
      assert_equal [name], set.entries.map(&:name)
    end

    mixed = load_config(%w[alpha archive-zone].map { |name| source_row(name) })
    assert_equal %w[alpha archive-zone], mixed.entries.map(&:name)
    assert_equal 3, load_config(%w[a b c].map { |name| source_row(name) }).entries.length
    assert(mixed.entries.all? { |entry| entry.source.is_a?(WorkloadOrchestrator::CommandWorkerSource) })
  end

  def test_union_preserves_namespaces_duplicate_worker_ids_and_independent_checkpoints
    set = registry_set(
      "local" => SequenceSource.new(snapshot("local-registry", 1, [worker("worker-1", "local", 11_441)])),
      "remote" => SequenceSource.new(snapshot("remote-registry", 7, [worker("worker-1", "remote", 11_442)]))
    ).poll_once

    assert_equal %w[local-registry remote-registry], set.current_workers.map(&:registry_id)
    assert_equal %w[worker-1 worker-1], set.current_workers.map(&:worker_id)
    assert_equal %w[local-registry remote-registry], set.accepted_checkpoints.keys.sort
    refute_path_exists File.join(@output, "dynamic-workers", "checkpoint.json")
    assert_path_exists File.join(@output, "dynamic-workers", "sources", "local", "checkpoint.json")
    assert_path_exists File.join(@output, "dynamic-workers", "sources", "remote", "checkpoint.json")
  end

  def test_scheduler_selection_persists_exact_source_revision_and_snapshot_evidence
    plan = plan_for([pool("remote")], [plan_job("job", "remote")])
    store = prepared_store(plan)
    set = registry_set(
      "local" => SequenceSource.new(snapshot("local-registry", 1, [worker("worker-1", "local", 11_441)])),
      "remote" => SequenceSource.new(snapshot("remote-registry", 7, [worker("worker-1", "remote", 11_442)]))
    ).poll_once

    assignment = WorkloadOrchestrator::DynamicScheduler.new(plan:, store:).assignments(
      workers: set.ready_workers, current_workers: set.current_workers
    ).fetch(0)
    attempt = store.record_dynamic_running!(job: assignment.job, worker: assignment.worker, environment_keys: [])
    metadata = store.metadata_for(plan.jobs.first)

    assert_equal "remote-registry", attempt.worker_binding.execution_identity.fetch("registry_id")
    assert_equal 7, metadata.dig("worker_registry_binding", "registry_revision")
    assert_equal set.accepted_checkpoints.fetch("remote-registry").fetch("snapshot_sha256"),
                 metadata.dig("worker_registry_binding", "registry_snapshot_sha256")
  end

  def test_worker_disappearance_is_reconciled_only_inside_its_registry_namespace
    original = worker("worker-1", "shared", 11_441)
    peer = worker("worker-1", "shared", 11_442)
    plan = plan_for([pool("shared")], [plan_job("local-job", "shared"), plan_job("remote-job", "shared")])
    store = prepared_store(plan)
    set = registry_set(
      "local" => SequenceSource.new(
        snapshot("local-registry", 1, [original]),
        snapshot("local-registry", 2, [], published_at: "2030-01-01T00:00:20Z")
      ),
      "remote" => SequenceSource.new(snapshot("remote-registry", 1, [peer]))
    ).poll_once
    by_registry = set.current_workers.to_h { |worker| [worker.registry_id, worker] }
    store.record_dynamic_running!(
      job: plan.jobs.fetch(0), worker: by_registry.fetch("local-registry"), environment_keys: []
    )
    store.record_dynamic_running!(
      job: plan.jobs.fetch(1), worker: by_registry.fetch("remote-registry"), environment_keys: []
    )

    set.poll_once
    events = WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store:).reconcile!(set)

    assert_equal ["local-job"], events.map { |event| event.fetch("job_id") }
    assert_equal "failed", store.metadata_for(plan.jobs.fetch(0)).fetch("status")
    assert_equal "running", store.metadata_for(plan.jobs.fetch(1)).fetch("status")
  end

  def test_generation_endpoint_and_capability_changes_are_scoped_to_the_correct_source
    original = worker("worker-1", "shared", 11_441)
    generation = original.merge("generation_id" => "generation-2")
    changed = worker("worker-1", "changed", 11_443).merge("generation_id" => "generation-2")
    peer = worker("worker-1", "shared", 11_442)
    set = registry_set(
      "local" => SequenceSource.new(
        snapshot("local-registry", 1, [original]),
        snapshot("local-registry", 2, [generation], published_at: "2030-01-01T00:00:20Z"),
        snapshot("local-registry", 3, [changed], published_at: "2030-01-01T00:00:40Z")
      ),
      "remote" => SequenceSource.new(snapshot("remote-registry", 1, [peer]))
    ).poll_once

    set.poll_once
    assert_equal ["generation"], set.pollers.fetch("local").last_reconciliation.dig("changed", 0, "kinds")
    assert_empty set.pollers.fetch("remote").last_reconciliation.fetch("changed")

    set.poll_once
    assert_equal %w[endpoint capability], set.pollers.fetch("local").last_reconciliation.dig("changed", 0, "kinds")
    assert_empty set.pollers.fetch("remote").last_reconciliation.fetch("changed")
  end

  def test_running_attempt_keeps_original_source_snapshot_after_replacement
    original = worker("worker-1", "shared", 11_441)
    replacement = original.merge("generation_id" => "generation-2")
    plan = plan_for([pool("shared")], [plan_job("job", "shared")])
    store = prepared_store(plan)
    set = registry_set(
      "local" => SequenceSource.new(
        snapshot("local-registry", 1, [original]),
        snapshot("local-registry", 2, [replacement], published_at: "2030-01-01T00:00:20Z")
      )
    ).poll_once
    attempt = store.record_dynamic_running!(job: plan.jobs.first, worker: set.current_workers.first,
                                            environment_keys: [])
    before = Marshal.load(Marshal.dump(store.metadata_for(plan.jobs.first)))

    set.poll_once
    WorkloadOrchestrator::DynamicWorkerLossReconciler.new(store:).reconcile!(set)
    after = store.metadata_for(plan.jobs.first)

    assert_equal before.fetch("worker_execution_identity"), after.fetch("worker_execution_identity")
    assert_equal before.fetch("worker_registry_binding"), after.fetch("worker_registry_binding")
    assert_equal before.fetch("worker_snapshot"), after.fetch("worker_snapshot")
    assert_equal attempt.worker_binding.worker_snapshot, after.fetch("worker_snapshot")
  end

  def test_duplicate_publisher_registry_identity_fails_without_a_synthetic_union
    set = registry_set(
      "one" => SequenceSource.new(snapshot("duplicate", 1, [worker("one", "model", 11_441)])),
      "two" => SequenceSource.new(snapshot("duplicate", 1, [worker("two", "model", 11_442)]))
    )

    error = assert_raises(WorkloadOrchestrator::Error) { set.poll_once }
    assert_includes error.message, "duplicate registry_id"
    refute_path_exists File.join(@output, "dynamic-workers", "checkpoint.json")
  end

  def test_v03_cli_runs_local_remote_and_mixed_fake_publishers_without_adventure_finder_code
    now = Time.now.utc
    local_snapshot = snapshot(
      "local-registry", 1, [worker("worker-1", "local", 11_441)],
      published_at: (now - 2).iso8601, expires_at: (now + 300).iso8601
    )
    remote_snapshot = snapshot(
      "remote-registry", 1, [worker("worker-1", "remote", 11_442)],
      published_at: (now - 2).iso8601, expires_at: (now + 300).iso8601
    )
    local_path = write_snapshot("local.json", local_snapshot)
    remote_path = write_snapshot("remote.json", remote_snapshot)
    publisher = File.join(@tmp, "publisher.rb")
    File.write(publisher, "print File.binread(ARGV.fetch(0))\n")
    rows = {
      "local" => source_row("local", args: [publisher, local_path]),
      "remote" => source_row("remote", args: [publisher, remote_path])
    }
    cases = {
      "local-only" => [%w[local], ["local-registry"]],
      "remote-only" => [%w[remote], ["remote-registry"]],
      "mixed" => [%w[local remote], %w[local-registry remote-registry]]
    }
    cases.each do |name, (models, expected_registries)|
      config = write_source_config(models.map { |model| rows.fetch(model) })
      plan = plan_for(
        models.map { |model| pool(model) },
        models.map { |model| plan_job("#{model}-job", model) },
        executable_jobs: true
      )
      output = File.join(@tmp, "output-#{name}")
      out = StringIO.new
      err = StringIO.new
      status = WorkloadOrchestrator::CLI.new(
        ["run", plan.path, "--workdir", @workdir, "--output", output,
         "--worker-sources-config", config], out:, err:, worker_poll_interval: 0.001
      ).run

      assert_equal 0, status, "#{name}: #{err.string}"
      identities = plan.jobs.map do |job|
        path = File.join(output, "runs", job.id, "metadata.json")
        JSON.parse(File.read(path)).dig("worker_execution_identity", "registry_id")
      end
      assert_equal expected_registries, identities.sort
    end
    refute($LOADED_FEATURES.any? { |path| File.basename(path).start_with?("adventure_finder") })
  end

  private

  def registry_set(sources)
    entries = sources.map do |name, source|
      WorkloadOrchestrator::WorkerSourceSet::Entry.new(name:, source:)
    end
    WorkloadOrchestrator::WorkerRegistrySet.new(
      sources: WorkloadOrchestrator::WorkerSourceSet.new(entries),
      checkpoint_root: File.join(@output, "dynamic-workers"), clock: -> { NOW }, interval_seconds: 0.001,
      sleeper: ->(*) {}
    )
  end

  def load_config(rows)
    WorkloadOrchestrator::WorkerSourceConfiguration.load(write_source_config(rows))
  end

  def write_source_config(rows)
    path = File.join(@tmp, "sources-#{Digest::SHA256.hexdigest(JSON.generate(rows))[0, 8]}.yml")
    File.write(path, YAML.dump(
                       "contract_version" => WorkloadOrchestrator::WorkerSourceConfiguration::CONTRACT_VERSION,
                       "sources" => rows
                     ))
    path
  end

  def source_row(name, args: ["-e", "print '{}'"])
    {
      "name" => name,
      "policy" => "required",
      "command" => RbConfig.ruby,
      "args" => args,
      "environment" => {},
      "workdir" => @workdir
    }
  end

  def snapshot(registry_id, revision, workers, published_at: "2030-01-01T00:00:00Z",
               expires_at: "2030-01-01T00:05:00Z")
    JSON.generate(
      "contract_version" => WorkloadOrchestrator::DynamicWorkerRegistry::CONTRACT_VERSION,
      "registry_id" => registry_id,
      "revision" => revision,
      "published_at" => published_at,
      "expires_at" => expires_at,
      "workers" => workers
    )
  end

  def write_snapshot(name, bytes)
    File.join(@tmp, name).tap { |path| File.write(path, bytes) }
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

  def pool(model)
    {
      "pool_id" => model,
      "required_labels" => ["inference"],
      "requirements" => {
        "ollama" => {
          "model" => model,
          "expected_digest" => Digest::SHA256.hexdigest(model),
          "required_context_length" => 131_072,
          "require_fully_gpu_resident" => true,
          "required_gpu_id" => "fixture-gpu"
        }
      }
    }
  end

  def plan_job(id, pool_id)
    { "job_id" => id, "pool_id" => pool_id, "argv" => ["fixture-command", id] }
  end

  def plan_for(pools, jobs, executable_jobs: false)
    rows = jobs.map do |job|
      next job unless executable_jobs

      job.merge("argv" => [RbConfig.ruby, "-e", "exit 0"])
    end
    path = File.join(@tmp, "plan-#{Digest::SHA256.hexdigest(JSON.generate(rows))[0, 8]}.json")
    File.write(path, JSON.generate(
                       "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                       "plan_id" => "multi-source-fixture",
                       "failure_policy" => { "max_consecutive_failures" => 10, "max_total_failures" => 10 },
                       "pools" => pools,
                       "jobs" => rows
                     ))
    WorkloadOrchestrator::Plan.load(path)
  end

  def prepared_store(plan)
    WorkloadOrchestrator::ExecutionStore.new(output_dir: @output, plan:, workdir: @workdir).tap do |store|
      store.prepare!
      store.start!
    end
  end

  def metadata_path(job)
    File.join(@output, "runs", job.id, "metadata.json")
  end
end
