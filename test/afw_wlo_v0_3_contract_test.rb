# frozen_string_literal: true

require_relative "test_helper"
require "digest"

class AfwWloV03ContractTest < Minitest::Test
  FIXTURE_ROOT = File.expand_path("fixtures/afw-wlo-v0.3", __dir__)
  CHECKSUM_MANIFEST_SHA256 = "04a958bd00135c866679eda12e39c6590fcba3382726525a932e6b2bece75b14"
  LEGACY_V01_FIXTURE = File.expand_path("../examples/hello-plan.json", __dir__)
  LEGACY_V01_SHA256 = "b6045754210463e8f7dc19fe334b8e515673cff3958eab2104438ae6c38234a6"
  LEGACY_V02_FIXTURE = File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
  LEGACY_V02_SHA256 = "03cd53956ef828dd61d46a4433eba948d0cba6017a25e97e360d3cc5a61952f5"

  def test_canonical_fixture_bytes_match_frozen_hashes
    manifest = File.join(FIXTURE_ROOT, "SHA256SUMS")
    assert_equal CHECKSUM_MANIFEST_SHA256, Digest::SHA256.file(manifest).hexdigest

    checksum_rows.each do |relative_path, expected|
      assert_equal expected, Digest::SHA256.file(File.join(FIXTURE_ROOT, relative_path)).hexdigest, relative_path
    end
  end

  def test_grouped_plan_uses_priority_only_groups_and_normalizes_dependencies
    plan = load_fixture("valid/grouped-no-dependencies.json")

    assert_equal WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION, plan.contract_version
    assert_equal WorkloadOrchestrator::Plan::WORK_CONSERVING_PRIORITY_SCHEDULING, plan.scheduling_semantics
    assert_equal WorkloadOrchestrator::Plan::PRIORITY_ONLY_GROUPS, plan.group_semantics
    assert_equal [[], [], []], plan.jobs.map(&:depends_on_job_ids)
    assert_equal(
      [[0, 0, "group-a-1"], [1, 0, "group-b-1"], [0, 1, "group-a-2"]],
      plan.jobs.map(&:priority_key)
    )
    assert_equal(
      %w[group-a-1 group-a-2 group-b-1],
      plan.jobs.sort_by { |job| plan.job_priority_key(job) }.map(&:id)
    )
  end

  def test_dependency_chain_accepts_only_earlier_jobs
    plan = load_fixture("valid/dependency-chain.json")

    assert_equal [[], ["step-1"], ["step-2"]], plan.jobs.map(&:depends_on_job_ids)
  end

  def test_ungrouped_plan_has_absolute_position_priority
    plan = load_fixture("valid/ungrouped.json")

    assert_equal WorkloadOrchestrator::Plan::NO_GROUP_SEMANTICS, plan.group_semantics
    assert_equal [[0, 0, "zeta"], [0, 1, "alpha"]], plan.jobs.map(&:priority_key)
  end

  def test_invalid_canonical_fixtures_fail_closed
    expected_messages = {
      "invalid/bad-version.json" => "unsupported execution plan contract",
      "invalid/duplicate-dependency.json" => "contains duplicate job_id",
      "invalid/forward-dependency.json" => "must appear earlier",
      "invalid/self-dependency.json" => "cannot depend on itself",
      "invalid/unknown-dependency.json" => "unknown dependency",
      "invalid/unknown-field.json" => "unknown field"
    }
    expected_messages.each do |relative_path, expected_message|
      error = assert_raises(WorkloadOrchestrator::Error, relative_path) { load_fixture(relative_path) }
      assert_includes error.message, expected_message, relative_path
    end
  end

  def test_legacy_fixture_bytes_and_scheduling_semantics_remain_frozen
    assert_equal LEGACY_V01_SHA256, Digest::SHA256.file(LEGACY_V01_FIXTURE).hexdigest
    assert_equal LEGACY_V02_SHA256, Digest::SHA256.file(LEGACY_V02_FIXTURE).hexdigest

    v01 = WorkloadOrchestrator::Plan.load(LEGACY_V01_FIXTURE)
    v02 = WorkloadOrchestrator::Plan.load(LEGACY_V02_FIXTURE)
    assert_equal WorkloadOrchestrator::Plan::LEGACY_SCHEDULING, v01.scheduling_semantics
    assert_equal WorkloadOrchestrator::Plan::LEGACY_SCHEDULING, v02.scheduling_semantics
    assert_equal WorkloadOrchestrator::Plan::HARD_GROUP_BARRIER, v02.group_semantics
    assert_raises(WorkloadOrchestrator::Error) { v02.job_priority_key(v02.jobs.first) }
  end

  def test_v02_does_not_accept_v03_dependency_fields
    document = JSON.parse(File.read(LEGACY_V02_FIXTURE))
    document.fetch("jobs").first["depends_on_job_ids"] = []

    error = assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::Plan.new(JSON.generate(document))
    end
    assert_includes error.message, "unknown field"
  end

  def test_v03_static_execution_still_fails_closed_without_a_dynamic_worker_source
    plan = load_fixture("valid/grouped-no-dependencies.json")
    Dir.mktmpdir("wlo-v03-run-") do |workdir|
      output = File.join(workdir, "output")
      runner = WorkloadOrchestrator::Runner.new(
        plan: plan, workers: nil, workdir: workdir, output_dir: output
      )

      error = assert_raises(WorkloadOrchestrator::Error) { runner.run }
      assert_includes error.message, "requires a dynamic worker source"
      refute Dir.exist?(output)
    end
  end

  private

  def load_fixture(relative_path)
    WorkloadOrchestrator::Plan.load(File.join(FIXTURE_ROOT, relative_path))
  end

  def checksum_rows
    File.readlines(File.join(FIXTURE_ROOT, "SHA256SUMS"), chomp: true).to_h do |line|
      checksum, relative_path = line.split(/\s+/, 2)
      [relative_path, checksum]
    end
  end
end
