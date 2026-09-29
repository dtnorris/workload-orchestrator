# frozen_string_literal: true

require "minitest/autorun"
require "fileutils"
require "json"
require "rbconfig"
require "stringio"
require "tmpdir"
require "yaml"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "workload_orchestrator"

module WloTestSupport
  DIGEST = ("a" * 64).freeze

  FakeCommandStatus = Struct.new(:exitstatus) do
    def success?
      exitstatus.zero?
    end
  end

  def command_result(exit_status: 0, stdout: "", stderr: "")
    [stdout, stderr, FakeCommandStatus.new(exit_status)]
  end

  def fixture_job(id, pool_id: "local-pool", group_id: nil)
    row = { "job_id" => id, "pool_id" => pool_id, "argv" => ["fixture-command", id] }
    row["group_id"] = group_id if group_id
    row
  end

  def write_plan(root, jobs:, pools: nil, failure_policy: nil, name: "plan.json")
    pools ||= [command_pool]
    failure_policy ||= { "max_consecutive_failures" => 2, "max_total_failures" => 3 }
    path = File.join(root, name)
    File.write(
      path,
      "#{JSON.pretty_generate(
        'contract_version' => WorkloadOrchestrator::Plan::CONTRACT_VERSION,
        'plan_id' => 'fixture-plan',
        'failure_policy' => failure_policy,
        'pools' => pools,
        'jobs' => jobs
      )}\n"
    )
    path
  end

  def command_pool(worker_names: ["local"], max_concurrency: 1)
    {
      "pool_id" => "local-pool",
      "worker_names" => worker_names,
      "required_labels" => ["local"],
      "max_concurrency" => max_concurrency
    }
  end

  def ollama_pool
    {
      "pool_id" => "ollama-pool",
      "worker_names" => ["ollama"],
      "required_labels" => ["local"],
      "requirements" => {
        "ollama" => {
          "model" => "fixture-model:latest",
          "expected_digest" => DIGEST
        }
      },
      "max_concurrency" => 1
    }
  end

  def job(id, code:, pool_id: "local-pool", group_id: nil, env: nil)
    row = {
      "job_id" => id,
      "pool_id" => pool_id,
      "argv" => [RbConfig.ruby, "-e", code]
    }
    row["group_id"] = group_id if group_id
    row["env"] = env if env
    row
  end

  def write_workers(root, rate: 0.0, include_ollama: false, extra_local: false)
    rows = {
      "local" => {
        "type" => "command",
        "labels" => ["local"],
        "hourly_rate_usd" => rate,
        "job_env" => {}
      }
    }
    if extra_local
      rows["local2"] = {
        "type" => "command",
        "labels" => ["local"],
        "hourly_rate_usd" => 0.0,
        "job_env" => {}
      }
    end
    if include_ollama
      rows["ollama"] = {
        "type" => "ollama",
        "base_url" => "http://127.0.0.1:19999",
        "labels" => ["local"],
        "hourly_rate_usd" => 0.0,
        "job_env" => {}
      }
    end
    path = File.join(root, "workers.yml")
    File.write(path, YAML.dump("workers" => rows))
    path
  end
end
