# frozen_string_literal: true

require_relative "test_helper"

class PlanTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-plan-")
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_loads_minimal_generic_plan_and_freezes_identity
    path = write_plan(@tmp, jobs: [job("job-1", code: "exit 0")])
    plan = WorkloadOrchestrator::Plan.load(path)

    assert_equal "fixture-plan", plan.id
    assert_match(/\A[0-9a-f]{64}\z/, plan.sha256)
    assert_equal ["local-pool"], plan.pools.map(&:id)
    assert_equal ["job-1"], plan.jobs.map(&:id)
    assert_equal 2, plan.failure_policy.fetch("max_consecutive_failures")
    assert_equal [], plan.failure_policy.fetch("non_operational_exit_statuses")
  end

  def test_failure_classification_exit_statuses_are_strict_and_nonzero
    jobs = [job("one", code: "exit 42")]
    base = { "max_consecutive_failures" => 2, "max_total_failures" => 3 }
    [0, -1, 256, "42", nil, 42.0].each do |invalid|
      path = write_plan(@tmp, jobs: jobs, failure_policy: base.merge("non_operational_exit_statuses" => [invalid]))
      assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.load(path) }
    end
    path = write_plan(@tmp, jobs: jobs, failure_policy: base.merge("non_operational_exit_statuses" => [42, 42]))
    assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.load(path) }
    path = write_plan(@tmp, jobs: jobs, failure_policy: base.merge("non_operational_exit_statuses" => [42]))
    assert_equal [42], WorkloadOrchestrator::Plan.load(path).failure_policy.fetch("non_operational_exit_statuses")
  end

  def test_rejects_unknown_fields_and_duplicate_job_ids
    document = JSON.parse(File.read(write_plan(@tmp, jobs: [job("job-1", code: "exit 0")])))
    document["domain_hint"] = "not allowed"
    File.write(File.join(@tmp, "unknown.json"), JSON.dump(document))

    error = assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::Plan.load(File.join(@tmp, "unknown.json"))
    end
    assert_includes error.message, "unknown field"

    duplicate = write_plan(
      @tmp,
      jobs: [job("same", code: "exit 0"), job("same", code: "exit 0")],
      name: "duplicate.json"
    )
    error = assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.load(duplicate) }
    assert_includes error.message, "duplicate job_id"
  end

  def test_group_id_enables_grouped_scheduling_and_mixed_grouping_is_rejected
    grouped = write_plan(
      @tmp,
      jobs: [
        job("a-1", code: "exit 0", group_id: "adventure-a"),
        job("a-2", code: "exit 0", group_id: "adventure-a")
      ],
      name: "grouped.json"
    )
    plan = WorkloadOrchestrator::Plan.load(grouped)
    assert plan.grouped_jobs?
    assert_equal([%w[a-1 a-2]], plan.job_groups.map { |rows| rows.map(&:id) })

    mixed = write_plan(
      @tmp,
      jobs: [
        job("a-1", code: "exit 0", group_id: "adventure-a"),
        job("ungrouped", code: "exit 0")
      ],
      name: "mixed.json"
    )
    error = assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.load(mixed) }
    assert_includes error.message, "all define group_id or all omit it"
  end

  def test_environment_values_are_strings_or_null
    path = write_plan(
      @tmp,
      jobs: [job("job-1", code: "exit 0", env: { "KEEP" => "yes", "REMOVE" => nil })]
    )
    plan = WorkloadOrchestrator::Plan.load(path)

    assert_equal({ "KEEP" => "yes", "REMOVE" => nil }, plan.jobs.first.env)
  end

  def test_ollama_requirement_requires_exact_digest
    pool = ollama_pool
    pool.fetch("requirements").fetch("ollama")["expected_digest"] = "short"
    path = write_plan(@tmp, pools: [pool], jobs: [job("job-1", code: "exit 0", pool_id: "ollama-pool")])

    error = assert_raises(WorkloadOrchestrator::Error) { WorkloadOrchestrator::Plan.load(path) }
    assert_includes error.message, "64-hex"
  end
end
