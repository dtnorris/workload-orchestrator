# frozen_string_literal: true

require_relative "test_helper"
require "digest"

class AfwWloContractTest < Minitest::Test
  FIXTURE = File.expand_path("fixtures/afw-wlo-v0.2.json", __dir__)
  CANONICAL_SHA256 = "03cd53956ef828dd61d46a4433eba948d0cba6017a25e97e360d3cc5a61952f5"

  def test_canonical_afw_plan_has_frozen_bytes_and_group_order
    bytes = File.binread(FIXTURE)
    assert_equal CANONICAL_SHA256, Digest::SHA256.hexdigest(bytes)
    plan = WorkloadOrchestrator::Plan.new(bytes)

    assert_equal WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION, plan.contract_version
    assert_equal "afw-contract-fixture", plan.id
    assert_equal %w[case-1 case-2 case-3], plan.jobs.map(&:id)
    assert_equal [%w[case-1 case-2], ["case-3"]], plan.job_groups.map { |rows| rows.map(&:id) }
    assert_equal [42], plan.failure_policy.fetch("non_operational_exit_statuses")
    assert_equal({}, plan.jobs.last.env)
    assert_equal bytes, plan.bytes
  end

  def test_incompatible_versions_shapes_and_scalar_types_fail_closed
    base = JSON.parse(File.read(FIXTURE))
    variants = {
      "version" => ->(row) { row["contract_version"] = "wlo-execution-plan/v0.3" },
      "missing plan ID" => ->(row) { row.delete("plan_id") },
      "null plan ID" => ->(row) { row["plan_id"] = nil },
      "numeric plan ID" => ->(row) { row["plan_id"] = 7 },
      "numeric job ID" => ->(row) { row.fetch("jobs").first["job_id"] = 7 },
      "numeric argv" => ->(row) { row.fetch("jobs").first["argv"] = ["echo", 7] },
      "null group" => ->(row) { row.fetch("jobs").first["group_id"] = nil },
      "unknown pool field" => ->(row) { row.fetch("pools").first["backend"] = "rpof" },
      "string failure limit" => ->(row) { row.fetch("failure_policy")["max_total_failures"] = "3" },
      "fractional context" => ->(row) { row.dig("pools", 0, "requirements", "ollama")["required_context_length"] = 2.5 },
      "null environment" => ->(row) { row.fetch("jobs").first["env"] = nil },
      "unknown job field" => ->(row) { row.fetch("jobs").first["provider"] = "runpod" }
    }
    variants.each do |name, change|
      document = Marshal.load(Marshal.dump(base))
      change.call(document)
      assert_raises(WorkloadOrchestrator::Error, name) do
        WorkloadOrchestrator::Plan.new(JSON.generate(document))
      end
    end
  end

  def test_profile_maps_logical_pool_without_changing_afw_plan_bytes
    plan = WorkloadOrchestrator::Plan.load(FIXTURE)
    profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{ "pool_id" => "qwen", "backend" => "local", "worker_names" => ["mac"],
                    "max_concurrency" => 1 }]
    ))
    bound = profile.bind(plan)

    assert_equal plan.sha256, bound.sha256
    assert_equal plan.bytes, bound.bytes
    assert_same plan.jobs, bound.jobs
    assert_equal "qwen", bound.pools.first.id
  end

  def test_terminal_import_of_fixture_requires_exact_plan_profile_workers_and_workdir
    plan = WorkloadOrchestrator::Plan.load(FIXTURE)
    profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{ "pool_id" => "qwen", "backend" => "local", "worker_names" => ["mac"],
                    "max_concurrency" => 1 }]
    ))
    bound = profile.bind(plan)
    Dir.mktmpdir("afw-wlo-contract-") do |workdir|
      evidence = File.join(workdir, "prior.json")
      File.write(evidence, "completed\n")
      workers_sha = Digest::SHA256.hexdigest("selected workers")
      document = {
        "contract_version" => WorkloadOrchestrator::TerminalImport::CONTRACT_VERSION,
        "plan_id" => bound.id, "plan_sha256" => bound.sha256,
        "execution_profile_sha256" => profile.sha256, "workers_sha256" => workers_sha,
        "workdir" => workdir,
        "jobs" => [{ "job_id" => "case-1", "status" => "complete", "exit_status" => 0,
                     "failure_class" => nil, "source_path" => "prior.json",
                     "source_sha256" => Digest::SHA256.file(evidence).hexdigest }]
      }
      imported = WorkloadOrchestrator::TerminalImport.new(
        bytes: JSON.generate(document), plan: bound, workdir: workdir, workers_sha256: workers_sha
      )
      assert_equal ["case-1"], imported.rows.keys
      %w[plan_id plan_sha256 execution_profile_sha256 workers_sha256 workdir].each do |key|
        changed = Marshal.load(Marshal.dump(document))
        changed[key] = "different"
        assert_raises(WorkloadOrchestrator::Error, key) do
          WorkloadOrchestrator::TerminalImport.new(
            bytes: JSON.generate(changed), plan: bound, workdir: workdir, workers_sha256: workers_sha
          )
        end
      end
    end
  end
end
