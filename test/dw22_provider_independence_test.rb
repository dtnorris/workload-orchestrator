# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class Dw22ProviderIndependenceTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)

  def test_dynamic_runtime_waits_schedules_and_completes_without_loading_legacy_provider_lifecycle
    script = <<~'RUBY'
      require "digest"
      require "fileutils"
      require "json"
      require "stringio"
      require "tmpdir"
      require "workload_orchestrator"

      legacy_pattern = %r{/(?:rpof_(?:client|budget_client|capacity_client)|paid_budget(?:_lifecycle)?|pool_fulfillment|execution_pool_plan|worker_admission_policy|legacy_rpof_runner)\.rb\z}
      raise "legacy provider lifecycle loaded by core loader" unless $LOADED_FEATURES.grep(legacy_pattern).empty?

      model = ->(name) { Digest::SHA256.hexdigest(name) }
      worker = lambda do |id, name, port|
        capabilities = {
          "gpu_id" => "NVIDIA A40",
          "ollama" => { "models" => [{
            "model" => name, "digest" => model.call(name), "context_length" => 131_072,
            "fully_gpu_resident" => true
          }] }
        }
        fingerprint_models = capabilities.dig("ollama", "models").map do |entry|
          {
            "context_length" => entry.fetch("context_length"), "digest" => entry.fetch("digest"),
            "fully_gpu_resident" => entry.fetch("fully_gpu_resident"), "model" => entry.fetch("model")
          }
        end
        fingerprint = Digest::SHA256.hexdigest(JSON.generate(
          "gpu_id" => capabilities.fetch("gpu_id"), "labels" => ["inference"],
          "ollama_models" => fingerprint_models
        ))
        {
          "worker_id" => id, "generation_id" => "generation-1",
          "endpoint" => "http://127.0.0.1:#{port}", "state" => "READY",
          "labels" => ["inference"], "capabilities" => capabilities,
          "capability_fingerprint" => fingerprint
        }
      end
      snapshot = lambda do |revision, workers|
        JSON.generate(
          "contract_version" => "dynamic-worker-registry/v0.1", "registry_id" => "dw22-registry",
          "revision" => revision,
          "published_at" => "2030-01-01T00:00:#{format('%02d', revision)}Z",
          "expires_at" => "2030-01-01T00:05:00Z", "workers" => workers
        )
      end
      source_class = Class.new(WorkloadOrchestrator::WorkerSource) do
        attr_reader :calls

        def initialize(rows)
          @rows = rows
          @calls = 0
        end

        def latest_snapshot
          value = @rows.fetch([calls, @rows.length - 1].min)
          @calls += 1
          value
        end
      end
      pools = %w[model-a model-b].map do |name|
        {
          "pool_id" => name, "required_labels" => ["inference"],
          "requirements" => { "ollama" => {
            "model" => name, "expected_digest" => model.call(name),
            "required_context_length" => 131_072, "require_fully_gpu_resident" => true,
            "required_gpu_id" => "NVIDIA A40"
          } }
        }
      end
      jobs = %w[model-a model-b].map do |name|
        {
          "job_id" => "job-#{name}", "pool_id" => name, "group_id" => "one-workload",
          "depends_on_job_ids" => [], "argv" => ["fixture-command", "job-#{name}"],
          "env" => { "KEEP" => name }
        }
      end
      plan = WorkloadOrchestrator::Plan.new(JSON.generate(
        "contract_version" => "wlo-execution-plan/v0.3", "plan_id" => "dw22-fixture",
        "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 2 },
        "pools" => pools, "jobs" => jobs
      ))
      workers = [worker.call("worker-a", "model-a", 11_441), worker.call("worker-b", "model-b", 11_442)]
      source = source_class.new([snapshot.call(1, []), snapshot.call(2, workers)])
      observed = {}

      Dir.mktmpdir("dw22-provider-independent-") do |root|
        work = File.join(root, "work")
        output = File.join(root, "output")
        FileUtils.mkdir_p(work)
        runner = WorkloadOrchestrator::Runner.new(
          plan: plan, workers: WorkloadOrchestrator::WorkerSet.new({}),
          workdir: work, output_dir: output, out: StringIO.new, worker_source: source,
          worker_registry_clock: -> { Time.iso8601("2030-01-01T00:01:00Z") },
          worker_registry_sleeper: ->(*) { Thread.pass }, worker_poll_interval: 0.001,
          command_executor: lambda do |environment, *argv, **_options|
            observed[argv.last] = environment
            ["", "", Struct.new(:exitstatus) { def success? = exitstatus.zero? }.new(0)]
          end
        )
        raise "dynamic run failed" unless runner.run == "completed"
        rows = JSON.parse(File.read(File.join(output, "jobs.json"))).fetch("jobs")
        raise "execution was partitioned" unless rows.length == 2 && rows.all? { |row| row["status"] == "complete" }
        raise "attempt worker evidence missing" unless rows.all? do |row|
          metadata = JSON.parse(File.read(File.join(output, "runs", row.fetch("job_id"), "metadata.json")))
          metadata.dig("worker_execution_identity", "endpoint") ==
            observed.fetch(row.fetch("job_id")).fetch("AF_OLLAMA_BASE_URL") && metadata.key?("worker_snapshot")
        end
      end

      raise "zero-capacity poll was skipped" unless source.calls >= 2
      raise "legacy provider lifecycle loaded by dynamic execution" unless $LOADED_FEATURES.grep(legacy_pattern).empty?
      puts JSON.generate("calls" => source.calls, "jobs" => observed.keys.sort)
    RUBY

    out, err, status = Open3.capture3(
      { "RUBYOPT" => nil }, RbConfig.ruby, "-I#{File.join(ROOT, 'lib')}", "-e", script
    )

    assert status.success?, err
    result = JSON.parse(out)
    assert_operator result.fetch("calls"), :>=, 2
    assert_equal %w[job-model-a job-model-b], result.fetch("jobs")
  end
end
