# frozen_string_literal: true

require_relative "test_helper"
require "open3"

class DynamicLocalDispatchTest < Minitest::Test
  include WloTestSupport

  NOW = Time.iso8601("2030-01-01T00:01:00Z")
  FIXTURE = File.expand_path("fixtures/dynamic-worker-registry-v0.1.json", __dir__)
  AFW_CONTROL_NAMES = %w[
    AF_CATALOG_ROLE AF_CATALOG_WORKBOOK AF_INVESTIGATION_GUARDRAIL_PROFILE
    AF_LETHALITY_GUARDRAIL_PROFILE AF_LLM_MAX_TOKENS AF_LLM_PROVIDER
    AF_NOS_TWO_STAGE_PROFILE AF_OPENAI_MODEL AF_OPENAI_REASONING_EFFORT
    AF_OUTPUT_DIR AF_PROJECT_STATE AF_PUZZLE_GUARDRAIL_PROFILE
    AF_SERIOUSNESS_GUARDRAIL_PROFILE AF_SOCIAL_INTERACTION_GUARDRAIL_PROFILE
    AF_SOURCE_REGISTRY AF_SOURCE_ROOT AF_SPEC_ROOT AF_XLSX_ROOT
  ].freeze

  class ForbiddenProvider
    def method_missing(name, *_args, **_options)
      raise "dynamic execution attempted legacy provider call: #{name}"
    end

    def respond_to_missing?(*_args)
      true
    end
  end

  class Source < WorkloadOrchestrator::WorkerSource
    def initialize(&callback)
      super()
      @callback = callback
    end

    def latest_snapshot
      @callback.call
    end
  end

  def setup
    @tmp = Dir.mktmpdir("wlo-local-dispatch-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @records = [worker("worker-a", "model-a"), worker("worker-b", "model-b")]
    @plan = WorkloadOrchestrator::Plan.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "local-dispatch-fixture",
      "failure_policy" => { "max_consecutive_failures" => 10, "max_total_failures" => 10 },
      "pools" => %w[model-a model-b].map do |model|
        { "pool_id" => model, "required_labels" => ["inference"],
          "requirements" => { "ollama" => { "model" => model, "expected_digest" => DIGEST } } }
      end,
      "jobs" => [fixture_job("success", pool_id: "model-a"), fixture_job("failure", pool_id: "model-b"),
                 fixture_job("exception", pool_id: "model-a")]
    ))
  end

  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_heterogeneous_commands_and_all_results_stay_local_with_exact_evidence
    mutex = Mutex.new
    condition = ConditionVariable.new
    observed = {}
    executor = lambda do |environment, *argv, chdir:|
      id = argv.last
      metadata = read_metadata(id)
      mutex.synchronize do
        observed[id] = { environment: environment, argv: argv, workdir: chdir, metadata: metadata }
        condition.broadcast
        if id != "exception"
          deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
          until observed.length >= 2
            remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
            raise "timeout waiting for concurrent local commands" unless remaining.positive?

            condition.wait(mutex, remaining)
          end
        end
      end
      raise "local exception" if id == "exception"

      command_result(exit_status: id == "failure" ? 7 : 0, stdout: "local stdout #{id}", stderr: "local stderr #{id}")
    end
    runner = build_runner(Source.new { snapshot(@records) }, executor)

    assert_equal "workload_failed", runner.run
    assert_equal %w[exception failure success], observed.keys.sort
    assert_equal({ "complete" => 1, "failed" => 2 }, runner.store.counts)
    assert_equal %w[execution.json jobs.json plan.json], Dir.children(@output).grep(/\.json\z/).sort
    refute runner.respond_to?(:remote_request, true)
    refute runner.respond_to?(:record_remote_result, true)
    observed.each do |id, data|
      job = @plan.jobs.find { |entry| entry.id == id }
      record = @records.find { |entry| entry.dig("capabilities", "ollama", "models", 0, "model") == job.pool_id }
      metadata = data.fetch(:metadata)
      assert_equal job.argv, data.fetch(:argv)
      assert_equal @workdir, data.fetch(:workdir)
      assert_equal "running", metadata.fetch("status")
      assert_equal record.fetch("endpoint"), data.dig(:environment, "AF_OLLAMA_BASE_URL")
      assert_equal record.fetch("endpoint"), metadata.dig("worker_execution_identity", "endpoint")
      assert_equal record.fetch("generation_id"), metadata.dig("worker_snapshot", "generation_id")
      assert_equal record.fetch("capabilities"), metadata.dig("worker_snapshot", "capabilities")
      assert_equal record.fetch("capability_fingerprint"), metadata.dig("worker_snapshot", "capability_fingerprint")
      assert_equal 1, metadata.dig("worker_snapshot", "registry_revision")
      refute File.exist?(File.join(@output, "runs", id, "provider-attempt-1"))
      refute read_metadata(id).key?("remote_request")
      refute read_metadata(id).key?("provider_job_id")
      refute read_metadata(id).key?("evidence")
    end
    assert_equal 2, observed.values.map { |data| data.dig(:environment, "AF_OLLAMA_BASE_URL") }.uniq.length
    assert_equal 0, read_metadata("success").fetch("exit_status")
    assert_equal 7, read_metadata("failure").fetch("exit_status")
    assert_nil read_metadata("exception").fetch("exit_status")
    assert_equal "local exception", read_metadata("exception").fetch("error")
    %w[success failure].each do |id|
      assert_equal "local stdout #{id}", File.read(File.join(@output, "runs", id, "stdout.log"))
      assert_equal "local stderr #{id}", File.read(File.join(@output, "runs", id, "stderr.log"))
    end
    assert_includes File.read(File.join(@output, "runs", "exception", "stderr.log")), "local exception"
  end

  def test_no_workers_means_polling_without_provider_fallback
    runner = nil
    calls = 0
    sleeper = lambda do |*_args|
      calls += 1
      assert_equal "running", runner.store.status
      runner.store.pause! if calls == 2
    end
    runner = build_runner(Source.new { snapshot([]) }, ->(*) { raise "unexpected local execution" }, sleeper)

    assert_equal "paused", runner.run
    assert_equal 2, calls
    assert_equal({ "pending" => 3 }, runner.store.counts)
    refute Dir.exist?(File.join(@output, "runs"))
    refute runner.respond_to?(:remote_request, true)
  end

  def test_afw_environment_is_identical_for_local_and_remote_endpoints_except_runtime_binding
    job_environment = AFW_CONTROL_NAMES.to_h { |name| [name, nil] }
    job_environment["AF_LLM_PROVIDER"] = "ollama"
    job_environment["AF_SOCIAL_INTERACTION_GUARDRAIL_PROFILE"] = "phase6-v0.3"
    document = JSON.parse(@plan.bytes)
    document["jobs"] = [fixture_job("success", pool_id: "model-a").merge(
      "argv" => [RbConfig.ruby, "-rjson", "-e", "puts JSON.generate(ENV.to_h.select { |key, _| key.start_with?('AF_') })"],
      "env" => job_environment
    )]
    @plan = WorkloadOrchestrator::Plan.new(JSON.generate(document))
    previous = AFW_CONTROL_NAMES.to_h { |name| [name, ENV[name]] }
    previous["AF_OLLAMA_BASE_URL"] = ENV["AF_OLLAMA_BASE_URL"]
    AFW_CONTROL_NAMES.each { |name| ENV[name] = "stale-#{name}" }
    ENV["AF_OLLAMA_BASE_URL"] = "http://wrong.example:11434"

    observed = ["http://127.0.0.1:11441", "http://remote.example:11434"].map.with_index do |endpoint, index|
      @output = File.join(@tmp, "output-#{index}")
      record = worker("worker-a", "model-a").merge("endpoint" => endpoint)
      @records = [record]
      result = build_runner(Source.new { snapshot(@records) }, Open3.method(:capture3)).run
      stderr = File.read(File.join(@output, "runs/success/stderr.log"))
      assert_equal "completed", result, stderr
      JSON.parse(File.read(File.join(@output, "runs/success/stdout.log")))
    end

    observed.each do |environment|
      assert_equal "ollama", environment.fetch("AF_LLM_PROVIDER")
      assert_equal "phase6-v0.3", environment.fetch("AF_SOCIAL_INTERACTION_GUARDRAIL_PROFILE")
      refute environment.key?("AF_INVESTIGATION_GUARDRAIL_PROFILE")
    end
    assert_equal "http://127.0.0.1:11441", observed[0].fetch("AF_OLLAMA_BASE_URL")
    assert_equal "http://remote.example:11434", observed[1].fetch("AF_OLLAMA_BASE_URL")
    assert_equal observed[0].reject { |name, _value| name == "AF_OLLAMA_BASE_URL" },
                 observed[1].reject { |name, _value| name == "AF_OLLAMA_BASE_URL" }
  ensure
    previous&.each { |name, value| value.nil? ? ENV.delete(name) : ENV[name] = value }
  end

  def test_runner_factory_rejects_dynamic_plans_and_sources_with_legacy_profiles
    profile = legacy_profile
    v02 = JSON.parse(@plan.bytes).merge("contract_version" => WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION)
    static_plan = WorkloadOrchestrator::Plan.new(JSON.generate(v02))
    source = Source.new { snapshot(@records) }
    [[@plan, nil], [@plan, source], [static_plan, source]].each do |plan, worker_source|
      error = assert_raises(WorkloadOrchestrator::Error) do
        WorkloadOrchestrator::Runner.new(plan: profile.bind(plan), worker_source: worker_source)
      end
      assert_includes error.message, "cannot execute dynamic attempts"
    end
    refute Dir.exist?(@output)
  end

  def test_explicit_legacy_runner_cannot_dispatch_dynamic_work
    require_relative "../lib/workload_orchestrator/legacy_rpof_runner"
    error = assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::LegacyRpofRunner.new(
        plan: @plan, worker_source: Source.new { snapshot(@records) },
        rpof_client: ForbiddenProvider.new, capacity_session: ForbiddenProvider.new
      )
    end
    assert_includes error.message, "cannot execute dynamic attempts"
    refute Dir.exist?(@output)
  end

  def test_generation_replacement_halts_without_provider_replay
    document = JSON.parse(@plan.bytes)
    document["jobs"] = [fixture_job("success", pool_id: "model-a")]
    @plan = WorkloadOrchestrator::Plan.new(JSON.generate(document))
    replacement = @records.first.merge("generation_id" => "replacement-generation")
    polls = 0
    started = Queue.new
    runner = nil
    source = Source.new do
      polls += 1
      registry = JSON.parse(snapshot(polls == 1 ? [@records.first] : [replacement]))
      registry["revision"] = polls
      registry["published_at"] = (Time.iso8601("2030-01-01T00:00:00Z") + polls).iso8601
      JSON.generate(registry)
    end
    calls = []
    executor = lambda do |environment, *_argv, **_options|
      calls << environment.fetch("AF_OLLAMA_BASE_URL")
      started << true
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      until runner.store.dispatch_halted?
        raise "timeout waiting for replacement halt" if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

        Thread.pass
      end
      command_result
    end
    runner = build_runner(source, executor, ->(*) { started.pop if polls == 1 })

    assert_equal "infrastructure_failed", runner.run
    assert_equal [@records.first.fetch("endpoint")], calls
    metadata = read_metadata("success")
    assert_equal "dynamic_worker_loss_in_doubt", metadata.dig("evidence", "kind")
    assert_equal "worker_generation_replaced", metadata.dig("evidence", "reason")
    assert_equal @records.first.fetch("generation_id"), metadata.dig("worker_snapshot", "generation_id")
    assert_equal "failed", metadata.fetch("status")
    assert_equal "complete", metadata.dig("late_evidence", 0, "status")
    refute metadata.key?("remote_request")
    refute runner.respond_to?(:remote_request, true)
    refute File.exist?(File.join(@output, "runs/success/provider-attempt-1"))
  end

  def test_primary_loader_and_real_local_execution_never_load_dispatch_implementation
    plan_path = File.join(@tmp, "plan.json")
    data = JSON.parse(@plan.bytes)
    data["jobs"] = [fixture_job("success", pool_id: "model-a")]
    data["jobs"][0]["argv"] = [RbConfig.ruby, "-e", "puts ENV.fetch('AF_OLLAMA_BASE_URL')"]
    File.write(plan_path, JSON.generate(data))
    registry_path = File.join(@tmp, "registry.json")
    File.write(registry_path, snapshot(@records))
    program = <<~'CODE'
      require "workload_orchestrator"
      require "stringio"
      # Any accidental constructor use fails rather than invoking a provider.
      trap = Class.new do
        def self.new(*)
          raise "RPOF client construction forbidden"
        end
      end
      %i[RpofClient RpofBudgetClient RpofCapacityClient PoolFulfillment].each do |name|
        WorkloadOrchestrator.send(:remove_const, name)
        WorkloadOrchestrator.const_set(name, trap)
      end
      # Loading a legacy profile schema alone must not pull in dispatch validators.
      WorkloadOrchestrator::RpofContract
      plan_path, registry_path, workdir, output = ARGV
      source = Class.new(WorkloadOrchestrator::WorkerSource) do
        def initialize(bytes)
          super()
          @bytes = bytes
        end
        def latest_snapshot
          @bytes
        end
      end.new(File.binread(registry_path))
      plan = WorkloadOrchestrator::Plan.load(plan_path)
      profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(
        "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
        "budget" => { "max_hourly_rate_usd" => 2, "max_total_cost_usd" => 10, "max_runtime_seconds" => 60 },
        "pools" => plan.pools.map do |pool|
          { "pool_id" => pool.id, "backend" => "rpof", "max_concurrency" => 1,
            "min_workers" => 1, "desired_workers" => 1, "max_hourly_rate_usd" => 1 }
        end
      ))
      begin
        WorkloadOrchestrator::Runner.new(plan: profile.bind(plan), worker_source: source)
        raise "dynamic plan routed to legacy runner"
      rescue WorkloadOrchestrator::Error => error
        raise unless error.message.include?("cannot execute dynamic attempts")
      end
      runner = WorkloadOrchestrator::Runner.new(
        plan: WorkloadOrchestrator::Plan.load(plan_path), workers: WorkloadOrchestrator::WorkerSet.new({}),
        worker_source: source, workdir: workdir, output_dir: output, out: StringIO.new,
        worker_registry_clock: -> { Time.iso8601("2030-01-01T00:01:00Z") }
      )
      result = runner.run
      forbidden = $LOADED_FEATURES.grep(%r{/workload_orchestrator/(legacy/|legacy_rpof_runner|rpof_client|rpof_dispatch_validation|rpof_budget_client|rpof_capacity_client|pool_fulfillment)})
      puts JSON.generate("status" => result, "forbidden_features" => forbidden,
                         "remote_request_available" => runner.respond_to?(:remote_request, true))
    CODE
    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby, "-I", File.expand_path("../lib", __dir__), "-e", program,
      plan_path, registry_path, @workdir, @output
    )

    assert status.success?, stderr
    report = JSON.parse(stdout)
    assert_equal "completed", report.fetch("status")
    assert_empty report.fetch("forbidden_features")
    assert_equal false, report.fetch("remote_request_available")
    assert_equal "#{@records.first.fetch('endpoint')}\n", File.read(File.join(@output, "runs/success/stdout.log"))
  end

  private

  def legacy_profile
    WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "budget" => { "max_hourly_rate_usd" => 2, "max_total_cost_usd" => 10, "max_runtime_seconds" => 60 },
      "pools" => @plan.pools.map do |pool|
        { "pool_id" => pool.id, "backend" => "rpof", "max_concurrency" => 1,
          "min_workers" => 1, "desired_workers" => 1, "max_hourly_rate_usd" => 1 }
      end
    ))
  end

  def build_runner(source, executor, sleeper = nil)
    WorkloadOrchestrator::Runner.new(
      plan: @plan, workers: WorkloadOrchestrator::WorkerSet.new({}), workdir: @workdir,
      output_dir: @output, worker_source: source, command_executor: executor, out: StringIO.new,
      worker_check: ForbiddenProvider.new,
      worker_registry_clock: -> { NOW }, worker_registry_sleeper: sleeper, worker_poll_interval: 0.001
    )
  end

  def read_metadata(id)
    JSON.parse(File.read(File.join(@output, "runs", id, "metadata.json")))
  end

  def worker(id, model)
    record = JSON.parse(File.read(FIXTURE)).fetch("workers").first
    record["worker_id"] = id
    record["endpoint"] = id == "worker-a" ? "http://127.0.0.1:11441" : "http://127.0.0.1:11442"
    record["capabilities"]["ollama"]["models"][0]["model"] = model
    record["capability_fingerprint"] = fingerprint(record)
    record
  end

  def fingerprint(record)
    models = record.dig("capabilities", "ollama", "models").map do |model|
      model.slice("context_length", "digest", "fully_gpu_resident", "model")
    end
    Digest::SHA256.hexdigest(JSON.generate("gpu_id" => record.dig("capabilities", "gpu_id"),
                                           "labels" => record.fetch("labels"), "ollama_models" => models))
  end

  def snapshot(workers)
    document = JSON.parse(File.read(FIXTURE))
    JSON.generate(document.merge("workers" => workers, "revision" => 1))
  end
end
