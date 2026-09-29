# frozen_string_literal: true

require_relative "test_helper"

class DynamicSchedulerTest < Minitest::Test
  include WloTestSupport

  class FakeStore
    def initialize(metadata = {})
      @metadata = metadata
    end

    def metadata_for(job)
      @metadata[job.id]
    end
  end

  class SpyMatcher
    attr_reader :calls

    def initialize(allowed_worker_id)
      @allowed_worker_id = allowed_worker_id
      @calls = []
    end

    def match_job(job:, worker:)
      @calls << [job.id, worker.worker_id]
      reasons = worker.worker_id == @allowed_worker_id ? [] : ["model_mismatch"]
      WorkloadOrchestrator::CapabilityMatcher::Result.new(reasons)
    end
  end

  def test_maximum_cardinality_avoids_naive_first_fit
    plan = build_plan(
      pools: [pool("qwen35", "qwen:35b"), pool("qwen27", "qwen:27b")],
      jobs: [job("qwen35-job", "qwen35"), job("qwen27-job", "qwen27")]
    )
    flexible = registry_worker("worker-a", models: [model("qwen:35b"), model("qwen:27b")])
    constrained = registry_worker("worker-b", models: [model("qwen:35b")])

    result = assignments(plan, workers: [flexible, constrained])

    assert_equal [%w[qwen35-job worker-b], %w[qwen27-job worker-a]], pairs(result)
  end

  def test_priority_wins_when_cardinality_is_constrained
    plan = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [job("earlier", "model"), job("later", "model")]
    )

    result = assignments(plan, workers: [registry_worker("worker", models: [model("same:model")])])

    assert_equal [%w[earlier worker]], pairs(result)
  end

  def test_unrunnable_earlier_job_does_not_block_later_job
    plan = build_plan(
      pools: [pool("missing", "missing:model"), pool("available", "available:model")],
      jobs: [job("earlier-unrunnable", "missing"), job("later-runnable", "available")]
    )
    worker = registry_worker("worker", models: [model("available:model")])

    assert_equal [%w[later-runnable worker]], pairs(assignments(plan, workers: [worker]))
  end

  def test_matching_is_deterministic_across_worker_input_order
    plan = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [job("first", "model"), job("second", "model")]
    )
    worker_b = registry_worker(
      "worker-b", registry_id: "registry-a", models: [model("same:model")]
    )
    worker_a = registry_worker(
      "worker-a", registry_id: "registry-z", models: [model("same:model")]
    )

    forward = assignments(plan, workers: [worker_a, worker_b])
    reverse = assignments(plan, workers: [worker_b, worker_a])

    assert_equal [%w[first worker-b], %w[second worker-a]], pairs(forward)
    assert_equal pairs(forward), pairs(reverse)
  end

  def test_exact_capability_mismatches_never_create_edges
    plan = build_plan(
      pools: [pool("qualified", "qualified:model", gpu_id: "GPU-A")],
      jobs: [job("qualified-job", "qualified")]
    )
    workers = [
      registry_worker("wrong-model", models: [model("other:model")]),
      registry_worker("wrong-digest", models: [model("qualified:model", digest: "b" * 64)]),
      registry_worker("wrong-context", models: [model("qualified:model", context: 65_536)]),
      registry_worker("wrong-residency", models: [model("qualified:model", resident: false)]),
      registry_worker("wrong-gpu", gpu_id: "GPU-B", models: [model("qualified:model")]),
      registry_worker("qualified", gpu_id: "GPU-A", models: [model("qualified:model")])
    ]

    assert_equal [%w[qualified-job qualified]], pairs(assignments(plan, workers: workers))
  end

  def test_scheduler_uses_injected_dw12_matcher
    plan = build_plan(pools: [pool("model", "same:model")], jobs: [job("one", "model")])
    denied = registry_worker("denied", models: [model("same:model")])
    allowed = registry_worker("allowed", models: [model("other:model")])
    matcher = SpyMatcher.new("allowed")

    result = scheduler(plan, matcher: matcher).assignments(workers: [denied, allowed])

    assert_equal [%w[one allowed]], pairs(result)
    assert_equal [%w[one allowed], %w[one denied]], matcher.calls.sort
  end

  def test_dependencies_require_terminal_state_and_failed_is_terminal
    plan = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [
        job("prerequisite", "model"),
        job("dependent", "model", depends_on: ["prerequisite"]),
        job("independent", "model")
      ]
    )
    workers = %w[worker-a worker-b].map do |id|
      registry_worker(id, models: [model("same:model")])
    end

    pending = assignments(plan, workers: workers)
    complete = assignments(
      plan, workers: workers,
            metadata: { "prerequisite" => { "status" => "complete" } }
    )
    failed = assignments(
      plan, workers: workers,
            metadata: { "prerequisite" => { "status" => "failed" } }
    )
    running = assignments(
      plan, workers: workers,
            metadata: { "prerequisite" => running_metadata(workers.first) }
    )

    assert_equal %w[independent prerequisite], pairs(pending).map(&:first).sort
    assert_equal %w[dependent independent], pairs(complete).map(&:first).sort
    assert_equal %w[dependent independent], pairs(failed).map(&:first).sort
    assert_equal [%w[independent worker-b]], pairs(running)
  end

  def test_running_binding_makes_worker_busy_and_generation_change_fails_closed
    plan = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [job("running", "model"), job("pending", "model")]
    )
    busy = registry_worker("worker-a", models: [model("same:model")])
    idle = registry_worker("worker-b", models: [model("same:model")])
    metadata = { "running" => running_metadata(busy) }

    assert_equal [%w[pending worker-b]], pairs(assignments(plan, workers: [busy, idle], metadata: metadata))

    replacement = registry_worker(
      "worker-a", generation_id: "replacement-generation", models: [model("same:model")]
    )
    error = assert_raises(WorkloadOrchestrator::Error) do
      assignments(plan, workers: [replacement, idle], metadata: metadata)
    end
    assert_includes error.message, "absent or replaced"
  end

  def test_not_ready_bound_worker_stays_busy_without_remaining_eligible
    plan = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [job("running", "model"), job("pending", "model")]
    )
    busy = registry_worker("worker-a", models: [model("same:model")])
    not_ready = registry_worker(
      "worker-a", state: "NOT_READY", models: [model("same:model")]
    )
    idle = registry_worker("worker-b", models: [model("same:model")])

    result = assignments(
      plan,
      workers: [idle],
      current_workers: [not_ready, idle],
      metadata: { "running" => running_metadata(busy) }
    )

    assert_equal [%w[pending worker-b]], pairs(result)
  end

  def test_bound_pool_concurrency_counts_running_attempts
    base = build_plan(
      pools: [pool("model", "same:model")],
      jobs: [job("running", "model"), job("pending", "model")]
    )
    limited_pool = base.pools.first.dup
    limited_pool.max_concurrency = 1
    limited_pool.freeze
    plan = WorkloadOrchestrator::ExecutionProfile::BoundPlan.new(base, Object.new, [limited_pool])
    busy = registry_worker("worker-a", models: [model("same:model")])
    idle = registry_worker("worker-b", models: [model("same:model")])

    result = assignments(
      plan, workers: [busy, idle], metadata: { "running" => running_metadata(busy) }
    )

    assert_empty result
  end

  private

  def assignments(plan, workers:, metadata: {}, current_workers: workers)
    scheduler(plan, metadata: metadata).assignments(
      workers: workers, current_workers: current_workers
    )
  end

  def scheduler(plan, metadata: {}, matcher: nil)
    options = { plan: plan, store: FakeStore.new(metadata) }
    options[:matcher] = matcher if matcher
    WorkloadOrchestrator::DynamicScheduler.new(**options)
  end

  def pairs(rows)
    rows.map { |row| [row.job.id, row.worker.worker_id] }
  end

  def build_plan(pools:, jobs:)
    WorkloadOrchestrator::Plan.new(JSON.generate(
                                     "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
                                     "plan_id" => "dynamic-scheduler-fixture",
                                     "failure_policy" => {
                                       "max_consecutive_failures" => 10,
                                       "max_total_failures" => 10
                                     },
                                     "pools" => pools,
                                     "jobs" => jobs
                                   ))
  end

  def pool(id, model_name, gpu_id: nil)
    requirement = {
      "model" => model_name,
      "expected_digest" => "a" * 64,
      "required_context_length" => 131_072,
      "require_fully_gpu_resident" => true
    }
    requirement["required_gpu_id"] = gpu_id if gpu_id
    { "pool_id" => id, "requirements" => { "ollama" => requirement } }
  end

  def job(id, pool_id, depends_on: [])
    {
      "job_id" => id,
      "pool_id" => pool_id,
      "depends_on_job_ids" => depends_on,
      "argv" => ["fixture-command", id]
    }
  end

  def model(name, digest: "a" * 64, context: 131_072, resident: true)
    {
      "model" => name,
      "digest" => digest,
      "context_length" => context,
      "fully_gpu_resident" => resident
    }
  end

  def registry_worker(worker_id, models:, registry_id: "registry", generation_id: "generation",
                      state: "READY", gpu_id: "GPU-A")
    WorkloadOrchestrator::RegistryWorker.new(
      registry: {
        registry_id: registry_id,
        revision: 1,
        published_at: Time.iso8601("2030-01-01T00:00:00Z"),
        expires_at: Time.iso8601("2030-01-01T00:05:00Z"),
        sha256: "f" * 64
      },
      record: {
        "worker_id" => worker_id,
        "generation_id" => generation_id,
        "endpoint" => "http://127.0.0.1:#{11_440 + (worker_id.bytes.sum % 100)}",
        "state" => state,
        "labels" => ["inference"],
        "capabilities" => { "gpu_id" => gpu_id, "ollama" => { "models" => models } },
        "capability_fingerprint" => Digest::SHA256.hexdigest([worker_id, generation_id, models].inspect)
      }
    )
  end

  def running_metadata(worker)
    { "status" => "running" }.merge(
      WorkloadOrchestrator::DynamicWorkerBinding.from_worker(worker).metadata
    )
  end
end
