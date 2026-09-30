# frozen_string_literal: true

require_relative "test_helper"
require "digest"

class TerminalImportTest < Minitest::Test
  include WloTestSupport

  def setup
    @tmp = Dir.mktmpdir("wlo-terminal-import-")
    @workdir = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@workdir)
    @workers = write_workers(@tmp)
    @plan_path = write_plan(@tmp, jobs: [
      job("old-complete", code: 'File.write("unexpected", "complete")'),
      job("old-failed", code: 'File.write("unexpected", "failed")'),
      job("new", code: 'File.write("new", "ran")')
    ], failure_policy: { "max_consecutive_failures" => 2, "max_total_failures" => 4,
                         "non_operational_exit_statuses" => [42] })
    @plan = WorkloadOrchestrator::Plan.load(@plan_path)
    File.write(File.join(@workdir, "complete.json"), '{"status":"complete"}')
    File.write(File.join(@workdir, "failed.json"), '{"status":"failed"}')
    @handoff = {
      "contract_version" => WorkloadOrchestrator::TerminalImport::CONTRACT_VERSION,
      "plan_id" => @plan.id, "plan_sha256" => @plan.sha256,
      "workdir" => File.expand_path(@workdir), "execution_profile_sha256" => nil, "workers_sha256" => nil,
      "jobs" => [source_row("old-complete", "complete.json", "complete", 0, nil),
                 source_row("old-failed", "failed.json", "failed", 42, "non_operational")]
    }
    write_handoff
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_import_preserves_terminal_jobs_and_resume_only_runs_pending_work
    assert_equal 0, import_cli.first
    assert_equal 0, import_cli.first
    assert_equal({ "complete" => 1, "failed" => 1, "pending" => 1 }, compact_counts)
    state = JSON.parse(File.read(File.join(@output, "execution.json")))
    assert_equal Digest::SHA256.hexdigest(File.binread(@handoff_path)), state.dig("terminal_import", "sha256")
    assert_equal "complete", state.dig("terminal_import", "phase")
    assert_equal 0, state.dig("circuit_breaker", "total_failures")
    assert_equal "non_operational", metadata("old-failed").fetch("failure_class")
    assert_equal File.binread(File.join(@workdir, "failed.json")), File.binread(File.join(@output, "runs/old-failed/import-source"))

    assert_equal "workload_failed", runner.run(resume: true)
    refute File.exist?(File.join(@workdir, "unexpected"))
    assert_equal "ran", File.read(File.join(@workdir, "new"))
    assert_equal({ "complete" => 2, "failed" => 1 }, compact_counts)
    assert_equal "workload_failed", runner.run(resume: true)
  end

  def test_import_binds_expanded_workdir_even_when_real_path_differs
    alias_dir = File.join(@tmp, "linked-work")
    File.symlink(@workdir, alias_dir)
    @handoff["workdir"] = File.expand_path(alias_dir)
    write_handoff
    subject = WorkloadOrchestrator::Runner.new(plan: @plan,
      workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: alias_dir, output_dir: @output, out: StringIO.new)

    refute_equal File.realpath(alias_dir), File.expand_path(alias_dir)
    assert_equal 2, subject.store.import_terminal!(bytes: File.binread(@handoff_path))
    assert_equal "workload_failed", subject.run(resume: true)
    assert_equal "ran", File.read(File.join(@workdir, "new"))
  end

  def test_non_operational_exit_is_failed_and_resets_consecutive_but_counts_total
    jobs = [job("first", code: "exit 7"), job("classified", code: "exit 42"),
            job("third", code: "exit 7"), job("later", code: 'File.write("later", "yes")')]
    plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, name: "classified-plan.json", jobs: jobs,
      failure_policy: { "max_consecutive_failures" => 2, "max_total_failures" => 4,
                        "non_operational_exit_statuses" => [42] }))
    output = File.join(@tmp, "classified-output")
    subject = WorkloadOrchestrator::Runner.new(plan: plan, workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: @workdir, output_dir: output, out: StringIO.new)
    assert_equal "workload_failed", subject.run
    assert_equal "yes", File.read(File.join(@workdir, "later"))
    state = JSON.parse(File.read(File.join(output, "execution.json")))
    assert_equal 3, state.dig("circuit_breaker", "total_failures")
    assert_equal 0, state.dig("circuit_breaker", "consecutive_failures")
    assert_equal "non_operational", JSON.parse(File.read(File.join(output, "runs/classified/metadata.json"))).fetch("failure_class")
    assert_equal "operational", JSON.parse(File.read(File.join(output, "runs/third/metadata.json"))).fetch("failure_class")
    assert_equal 3, JSON.parse(File.read(File.join(output, "jobs.json"))).fetch("jobs").count { |row| row["status"] == "failed" }
  end

  def test_ordinary_failures_still_trip_consecutive_breaker
    plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, name: "ordinary-plan.json", jobs: [
      job("one", code: "exit 7"), job("two", code: "exit 7"),
      job("later", code: 'File.write("later", "yes")')
    ], failure_policy: { "max_consecutive_failures" => 2, "max_total_failures" => 4,
                         "non_operational_exit_statuses" => [42] }))
    subject = WorkloadOrchestrator::Runner.new(plan: plan, workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: @workdir, output_dir: File.join(@tmp, "ordinary-output"), out: StringIO.new)
    assert_equal "circuit_broken", subject.run
    refute File.exist?(File.join(@workdir, "later"))
  end

  def test_infrastructure_failure_with_reserved_exit_status_remains_operational
    subject = runner.store
    subject.prepare!
    started = subject.record_running!(job: @plan.jobs.last,
      worker: WorkloadOrchestrator::WorkerSet.load(@workers).fetch("local"), environment_keys: [])
    subject.record_terminal!(job: @plan.jobs.last, status: "failed", started_at: started,
      exit_status: 42, failure_class: "operational", evidence: { "kind" => "infrastructure" })
    assert_equal "operational", metadata("new").fetch("failure_class")
    assert_equal 1, JSON.parse(File.read(File.join(@output, "execution.json"))).dig("circuit_breaker", "consecutive_failures")
    assert_raises(WorkloadOrchestrator::Error) do
      subject.record_terminal!(job: @plan.jobs.last, status: "complete", started_at: started,
        exit_status: 0, failure_class: "non_operational")
    end
  end

  def test_command_launch_error_remains_an_ordinary_failed_attempt
    plan = WorkloadOrchestrator::Plan.load(write_plan(@tmp, name: "launch-error-plan.json",
      jobs: [{ "job_id" => "broken", "pool_id" => "local-pool", "argv" => [File.join(@tmp, "missing-executable")] }],
      failure_policy: { "max_consecutive_failures" => 2, "max_total_failures" => 3,
                        "non_operational_exit_statuses" => [42] }))
    output = File.join(@tmp, "launch-error-output")
    subject = WorkloadOrchestrator::Runner.new(plan: plan, workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: @workdir, output_dir: output, out: StringIO.new)
    assert_equal "workload_failed", subject.run
    result = JSON.parse(File.read(File.join(output, "runs/broken/metadata.json")))
    assert_equal "failed", result.fetch("status")
    assert_equal "operational", result.fetch("failure_class")
    assert_nil result.fetch("exit_status")
  end

  def test_runner_rejects_missing_workdir_before_creating_state
    subject = WorkloadOrchestrator::Runner.new(plan: @plan,
      workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: File.join(@tmp, "missing-workdir"), output_dir: @output, out: StringIO.new)
    assert_raises(WorkloadOrchestrator::Error) { subject.run }
    refute Dir.exist?(@output)
  end

  def test_bad_identity_or_changed_source_fails_before_creating_output
    @handoff["plan_sha256"] = "0" * 64
    write_handoff
    assert_equal 1, import_cli.first
    refute Dir.exist?(@output)
    @handoff["plan_sha256"] = @plan.sha256
    File.write(File.join(@workdir, "failed.json"), "modified")
    write_handoff
    assert_equal 1, import_cli.first
    refute Dir.exist?(@output)
  end

  def test_logical_profile_and_worker_binding_are_part_of_import_identity
    document = JSON.parse(File.read(@plan_path))
    document["contract_version"] = WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION
    document["pools"] = [{ "pool_id" => "local-pool" }]
    File.write(@plan_path, JSON.pretty_generate(document) + "\n")
    profile_path = File.join(@tmp, "profile.json")
    File.write(profile_path, JSON.pretty_generate(
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                    "worker_names" => ["local"], "max_concurrency" => 1 }]
    ) + "\n")
    bound = WorkloadOrchestrator::ExecutionProfile.load(profile_path).bind(WorkloadOrchestrator::Plan.load(@plan_path))
    @handoff["plan_sha256"] = bound.sha256
    @handoff["execution_profile_sha256"] = bound.execution_profile.sha256
    @handoff["workers_sha256"] = WorkloadOrchestrator::WorkerSet.load(@workers).execution_sha256(bound)
    write_handoff
    out, err = StringIO.new, StringIO.new
    args = ["import-terminal", @plan_path, @handoff_path, "--workdir", @workdir, "--output", @output,
            "--workers-config", @workers, "--execution-profile", profile_path]
    assert_equal 0, WorkloadOrchestrator::CLI.new(args, out: out, err: err).run, err.string
    workers = YAML.safe_load_file(@workers)
    workers.fetch("workers").fetch("local")["job_env"] = { "CHANGED" => "yes" }
    File.write(@workers, YAML.dump(workers))
    assert_equal 1, WorkloadOrchestrator::CLI.new(args, out: StringIO.new, err: StringIO.new).run
  end

  def test_unbound_v0_3_import_starts_one_fresh_execution_with_26_complete_and_166_pending
    pool_ids = %w[qwen27 gptoss gemma qwen]
    jobs = (943..958).flat_map do |adventure|
      12.times.map do |dimension|
        id = format("production-batch-039-rerun-fixture-adv%04d-d%02d", adventure, dimension)
        {
          "job_id" => id,
          "pool_id" => pool_ids.fetch(dimension % pool_ids.length),
          "group_id" => format("ADV-%04d", adventure),
          "argv" => ["bin/afw-score-job", "build/cases/#{id}.yml"],
          "env" => {},
          "depends_on_job_ids" => []
        }
      end
    end
    pools = pool_ids.map do |id|
      {
        "pool_id" => id,
        "requirements" => {
          "ollama" => {
            "model" => "#{id}:fixture", "expected_digest" => "a" * 64,
            "required_context_length" => 131_072, "require_fully_gpu_resident" => true
          }
        }
      }
    end
    document = {
      "contract_version" => WorkloadOrchestrator::Plan::PRIORITY_CONTRACT_VERSION,
      "plan_id" => "production-batch-039-rerun-fixture",
      "failure_policy" => { "max_consecutive_failures" => 2, "max_total_failures" => 3 },
      "pools" => pools,
      "jobs" => jobs
    }
    plan_path = File.join(@tmp, "batch039-v0.3.json")
    File.write(plan_path, JSON.pretty_generate(document) + "\n")
    plan = WorkloadOrchestrator::Plan.load(plan_path)
    evidence_root = File.join(@workdir, "terminal-import-evidence")
    FileUtils.mkdir_p(evidence_root)
    imported = jobs.first(26).map do |job_row|
      relative = File.join("terminal-import-evidence", "#{job_row.fetch('job_id')}.json")
      File.write(File.join(@workdir, relative), JSON.generate("job_id" => job_row.fetch("job_id")))
      source_row(job_row.fetch("job_id"), relative, "complete", 0, nil)
    end
    handoff = {
      "contract_version" => WorkloadOrchestrator::TerminalImport::CONTRACT_VERSION,
      "plan_id" => plan.id,
      "plan_sha256" => plan.sha256,
      "workdir" => File.expand_path(@workdir),
      "execution_profile_sha256" => nil,
      "workers_sha256" => nil,
      "jobs" => imported
    }
    handoff_path = File.join(@tmp, "batch039-terminal-import.json")
    File.write(handoff_path, JSON.pretty_generate(handoff) + "\n")
    output = File.join(@tmp, "batch039-output")
    args = ["import-terminal", plan_path, handoff_path, "--workdir", @workdir, "--output", output]

    assert_equal 0, WorkloadOrchestrator::CLI.new(args, out: StringIO.new, err: StringIO.new).run
    before = Dir.glob(File.join(output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
      .to_h { |path| [path.delete_prefix("#{output}/"), File.binread(path)] }
    assert_equal 0, WorkloadOrchestrator::CLI.new(args, out: StringIO.new, err: StringIO.new).run
    assert_equal before, Dir.glob(File.join(output, "**", "*"), File::FNM_DOTMATCH).select { |path| File.file?(path) }
      .to_h { |path| [path.delete_prefix("#{output}/"), File.binread(path)] }

    rows = JSON.parse(File.read(File.join(output, "jobs.json"))).fetch("jobs")
    assert_equal({ "complete" => 26, "pending" => 166 }, rows.group_by { |row| row.fetch("status") }.transform_values(&:length))
    assert_equal 192, rows.length
    state = JSON.parse(File.read(File.join(output, "execution.json")))
    assert_equal "pending", state.fetch("status")
    assert_equal({ "sha256" => Digest::SHA256.file(handoff_path).hexdigest,
                   "phase" => "complete", "jobs" => 26, "completed_at" => state.dig("terminal_import", "completed_at") },
                 state.fetch("terminal_import"))
    %w[retry_history dispatch_halt worker_registry checkpoint pause].each { |field| refute state.key?(field) }
    assert_equal ["execution.json"], Dir.glob(File.join(output, "**", "execution.json")).map { |path| File.basename(path) }
    assert_equal ["jobs.json"], Dir.glob(File.join(output, "**", "jobs.json")).map { |path| File.basename(path) }
    assert_equal 26, Dir.children(File.join(output, "runs")).length
  end

  def test_all_terminal_remote_import_does_not_acquire_paid_capacity
    document = JSON.parse(File.read(@plan_path))
    document["contract_version"] = WorkloadOrchestrator::Plan::LOGICAL_CONTRACT_VERSION
    document["pools"] = [{ "pool_id" => "local-pool" }]
    document["jobs"] = document.fetch("jobs").first(2)
    File.write(@plan_path, JSON.pretty_generate(document) + "\n")
    profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.pretty_generate(
      "contract_version" => WorkloadOrchestrator::ExecutionProfile::CONTRACT_VERSION,
      "pools" => [{ "pool_id" => "local-pool", "backend" => "rpof", "max_concurrency" => 1,
                    "min_workers" => 1, "desired_workers" => 1, "max_hourly_rate_usd" => 1 }],
      "budget" => { "max_hourly_rate_usd" => 1, "max_total_cost_usd" => 2,
                    "max_runtime_seconds" => 3600 }
    ))
    bound = profile.bind(WorkloadOrchestrator::Plan.load(@plan_path))
    workers = WorkloadOrchestrator::WorkerSet.new({})
    @handoff["plan_sha256"] = bound.sha256
    @handoff["execution_profile_sha256"] = profile.sha256
    @handoff["workers_sha256"] = workers.execution_sha256(bound)
    subject = WorkloadOrchestrator::ExecutionStore.new(
      plan: bound, workdir: @workdir, output_dir: @output,
      workers_sha256: workers.execution_sha256(bound)
    )
    assert_equal 2, subject.import_terminal!(bytes: JSON.pretty_generate(@handoff) + "\n")
    report = WorkloadOrchestrator::ExecutionReport.new(plan: bound, output: @output).document
    assert_equal({ "complete" => 1, "failed" => 1, "running" => 0, "pending" => 0 }, report.fetch("counts"))
    before = File.binread(File.join(@output, "execution.json"))
    assert_raises(WorkloadOrchestrator::Error) do
      WorkloadOrchestrator::Runner.new(
        plan: bound, workers: workers, workdir: @workdir, output_dir: @output
      )
    end
    assert_equal before, File.binread(File.join(@output, "execution.json"))
    refute Dir.exist?(File.join(@output, "capacity"))
  end

  def test_conflicting_handoff_and_tampered_import_evidence_fail_closed
    assert_equal 0, import_cli.first
    @handoff["jobs"][1]["failure_class"] = "operational"
    write_handoff
    assert_equal 1, import_cli.first
    File.write(File.join(@output, "runs/old-failed/import-source"), "tampered")
    assert_raises(WorkloadOrchestrator::Error) { runner.run }
    refute File.exist?(File.join(@workdir, "new"))
  end

  def test_import_refuses_an_execution_that_has_already_started
    assert_equal "completed", runner.run
    before = File.binread(File.join(@output, "execution.json"))
    assert_equal 1, import_cli.first
    assert_equal before, File.binread(File.join(@output, "execution.json"))
  end

  def test_incomplete_import_blocks_execution_and_recovers_with_identical_snapshot
    subject = runner.store
    subject.define_singleton_method(:apply_terminal_import!) do |_handoff, _sources|
      raise IOError, "simulated interruption"
    end
    assert_raises(IOError) { subject.import_terminal!(bytes: File.binread(@handoff_path)) }
    assert_raises(WorkloadOrchestrator::Error) { runner.run }
    refute File.exist?(File.join(@workdir, "new"))
    subject.singleton_class.remove_method(:apply_terminal_import!)
    assert_equal 2, subject.import_terminal!(bytes: File.binread(@handoff_path))
    assert_equal "workload_failed", runner.run
  end

  def test_explicit_retry_archives_imported_failure_without_ordinary_replay
    assert_equal 0, import_cli.first
    assert_equal "workload_failed", runner.run
    out, err = StringIO.new, StringIO.new
    code = WorkloadOrchestrator::CLI.new(["retry-failed", @plan_path, "--workdir", @workdir,
      "--output", @output, "--job", "old-failed", "--reason", "reviewed"], out: out, err: err).run
    assert_equal 0, code, err.string
    archive = File.join(@output, "attempts/old-failed/attempt-1")
    assert_equal "failed", JSON.parse(File.read(File.join(archive, "metadata.json"))).fetch("status")
    assert File.file?(File.join(archive, "import-source"))
    assert_equal "paused", runner.run
  end

  private

  def source_row(id, path, status, exit_status, classification)
    { "job_id" => id, "status" => status, "exit_status" => exit_status,
      "failure_class" => classification, "source_path" => path,
      "source_sha256" => Digest::SHA256.file(File.join(@workdir, path)).hexdigest }
  end

  def write_handoff
    @handoff_path = File.join(@tmp, "handoff.json")
    File.write(@handoff_path, JSON.pretty_generate(@handoff) + "\n")
  end

  def import_cli
    out, err = StringIO.new, StringIO.new
    code = WorkloadOrchestrator::CLI.new(["import-terminal", @plan_path, @handoff_path,
      "--workdir", @workdir, "--output", @output, "--workers-config", @workers], out: out, err: err).run
    [code, out.string, err.string]
  end

  def runner
    WorkloadOrchestrator::Runner.new(plan: @plan, workers: WorkloadOrchestrator::WorkerSet.load(@workers),
      workdir: @workdir, output_dir: @output, out: StringIO.new)
  end

  def metadata(id)
    JSON.parse(File.read(File.join(@output, "runs", id, "metadata.json")))
  end

  def compact_counts
    runner.store.counts.reject { |_key, value| value.zero? }
  end
end
