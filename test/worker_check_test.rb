# frozen_string_literal: true

require_relative "test_helper"

class WorkerCheckTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-worker-check-")
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_verifies_exact_ollama_model_digest
    plan = WorkloadOrchestrator::Plan.load(
      write_plan(
        @tmp,
        pools: [ollama_pool],
        jobs: [job("job-1", code: "exit 0", pool_id: "ollama-pool")]
      )
    )
    workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp, include_ollama: true))
    fetch_json = lambda do |_base_url, path|
      if path == "/api/version"
        { "version" => "fixture" }
      else
        { "models" => [{ "name" => "fixture-model:latest", "digest" => DIGEST }] }
      end
    end

    results = WorkloadOrchestrator::WorkerCheck.new(fetch_json: fetch_json).check_plan!(plan, workers)

    assert_equal 1, results.length
    assert results.first.ok
    assert_equal DIGEST, results.first.model_digest
  end

  def test_rejects_digest_mismatch
    plan = WorkloadOrchestrator::Plan.load(
      write_plan(
        @tmp,
        pools: [ollama_pool],
        jobs: [job("job-1", code: "exit 0", pool_id: "ollama-pool")]
      )
    )
    workers = WorkloadOrchestrator::WorkerSet.load(write_workers(@tmp, include_ollama: true))
    fetch_json = lambda do |_base_url, path|
      if path == "/api/version"
        { "version" => "fixture" }
      else
        { "models" => [{ "name" => "fixture-model:latest", "digest" => "b" * 64 }] }
      end
    end

    error = assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::WorkerCheck.new(fetch_json: fetch_json).check_plan!(plan, workers)
    end
    assert_includes error.message, "model digest mismatch"
  end
end
