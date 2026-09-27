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
