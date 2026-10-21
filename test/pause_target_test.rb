# frozen_string_literal: true

require_relative "test_helper"
require "find"
require "minitest/mock"
require "open3"
require "timeout"

# Public pause discovery against disposable retained executions only.
class PauseTargetTest < Minitest::Test
  include WloTestSupport
  TOKEN = "a" * 64
  OTHER = "b" * 64

  # Retain two executions of the same plan with distinct public consumer IDs.
  def setup
    @tmp = File.realpath(Dir.mktmpdir("wlo-pause-target-"))
    @repo = File.join(@tmp, "wlo")
    @root = File.join(@repo, "output")
    @output = File.join(@root, "retained-execution-with-an-unrelated-directory-name")
    @unrelated = File.join(@root, "another-execution-of-the-same-plan")
    @work = File.join(@tmp, "work")
    FileUtils.mkdir_p(@work)
    @path = write_plan(@tmp, jobs: [job("one", code: "puts :ok")])
    @plan = WorkloadOrchestrator::Plan.load(@path)
    @workers = write_workers(@tmp)
    [@output, @unrelated].each { |output| store(output).prepare! }
    @identifier = report(@output).dig("pause_control", "consumer_id")
  end

  # Remove only this test's synthetic files, after any owned process is reaped.
  def teardown
    FileUtils.remove_entry(@tmp)
  end

  # Run unchanged public bin/lib bytes from a disposable WLO invocation root.
  # This proves default-root derivation and the real manual control handoff.
  def test_public_cli_default_root_selects_exact_execution_from_any_working_directory
    FileUtils.cp_r(File.expand_path("../lib", __dir__), @repo)
    FileUtils.cp_r(File.expand_path("../bin", __dir__), @repo)
    before = tree(@unrelated)
    original = tree(@output)
    2.times do |index|
      cwd = index.zero? ? @repo : @work
      stdout, stderr, status = Open3.capture3(RbConfig.ruby, File.join(@repo, "bin/wlo"),
        "pause", "--execution", @identifier, chdir: cwd)
      assert status.success?, "#{stdout}\n#{stderr}"
      assert_includes stdout, "Pause requested: #{@output}"
      assert_equal "requested\n", File.binread(File.join(@output, "control/pause"))
      assert_equal before, tree(@unrelated)
      assert_equal original, tree(@output).reject { |name, _| ["control", "control/pause"].include?(name) }
    end
    assert_operator "bin/wlo pause --execution #{@identifier}".size, :<,
      "bin/wlo pause --output #{@output}".size
    refute_equal @identifier, report(@unrelated).dig("pause_control", "consumer_id")
  end

  # Explicit roots work from other directories and never fall back elsewhere.
  def test_public_cli_root_override_and_missing_root_refusal
    before = tree(@unrelated)
    stdout, stderr, status = public_cli("pause", "--execution", @identifier, "--output-root", @root)
    assert status.success?, "#{stdout}\n#{stderr}"
    assert_includes stdout, @output
    assert_equal "requested\n", File.binread(File.join(@output, "control/pause"))
    assert_equal before, tree(@unrelated)
    [File.join(@tmp, "missing"), @work].each do |root|
      assert_refusal("pause", "--execution", @identifier, "--output-root", root)
    end
    assert_equal 0, cli("pause", "--execution", @identifier, "--output-root", "output", cwd: @repo).first
  end

  # Discovery reads current dynamic plans and frozen legacy profiles without
  # workers, registries or execution preparation, and refuses profile drift.
  def test_dynamic_and_profiled_plan_identity_is_read_only_and_byte_bound
    document = JSON.parse(File.binread(@path))
    document["pools"] = [{ "pool_id" => "local-pool" }]
    %w[v0.2 v0.3 v0.4].each do |version|
      document["contract_version"] = "wlo-execution-plan/#{version}"
      plan = WorkloadOrchestrator::Plan.new(JSON.generate(document))
      if version == "v0.2"
        profile = WorkloadOrchestrator::ExecutionProfile.new(JSON.generate(
          "contract_version" => "wlo-execution-profile/v0.1",
          "pools" => [{ "pool_id" => "local-pool", "backend" => "local",
                        "worker_names" => ["local"], "max_concurrency" => 1 }]
        ))
        plan = profile.bind(plan)
      end
      output = File.join(@root, version)
      WorkloadOrchestrator::ExecutionStore.new(plan: plan, workdir: @work, output_dir: output,
        workers_sha256: version == "v0.2" ? "c" * 64 : nil).prepare!
      identifier = WorkloadOrchestrator::PauseRequests.new(output).snapshot.fetch("consumer_id")
      before = tree(output)
      assert_equal 0, cli("pause", "--execution", identifier).first
      assert_equal before, tree(output).reject { |name, _| ["control", "control/pause"].include?(name) }
    end
    profile_path = File.join(@root, "v0.2", "execution-profile.json")
    File.write(profile_path, File.binread(profile_path) + " ")
    assert_refusal("pause", "--execution", @identifier, message: "different execution profile")
  end

  # Both locators preserve independent holds, idempotence and manual authority.
  def test_both_selectors_preserve_owned_and_external_manual_holds
    [["--output", @output], ["--execution", @identifier]].each do |selector|
      assert_equal 0, cli("pause", *selector).first
      marker = File.binread(File.join(@output, "control/pause"))
      [TOKEN, OTHER, TOKEN].each do |token|
        before = tree(@output)
        code, stdout, stderr = cli("pause", *selector, "--owner-token", token)
        assert_equal 0, code, stderr
        assert_includes stdout, Digest::SHA256.hexdigest(token)
        refute_includes stdout + stderr, token
        assert_equal before, tree(@output) if token == TOKEN && report(@output).dig("pause_control", "request_ids").size == 2
      end
      assert_equal [TOKEN, OTHER].map { |token| Digest::SHA256.hexdigest(token) }.sort,
        report(@output).dig("pause_control", "request_ids")
      resume_args = [@path, "--workdir", @work, "--output", @output, "--workers-config", @workers]
      assert_equal 0, cli("resume", *resume_args, "--preserve-pauses").first
      [TOKEN, OTHER].each do |token|
        assert_equal 0, cli("resume", *resume_args, "--release-pause", token).first
        assert_equal marker, File.binread(File.join(@output, "control/pause"))
        assert report(@output).fetch("paused")
      end
      assert_refusal("resume", *resume_args, "--release-pause", TOKEN)
      assert_equal "pending", report(@output).fetch("jobs").first.fetch("status")
      WorkloadOrchestrator::PauseRequests.new(@output).clear_all!
    end
  end

  # Unusual layouts still use the unchanged explicit-output path and errors.
  def test_output_compatibility_and_invalid_owner_tokens
    unusual = File.join(@work, "unusual", "nested execution")
    store(unusual).prepare!
    assert_equal 0, cli("pause", "--output", unusual).first
    assert_equal "requested\n", File.binread(File.join(unusual, "control/pause"))
    assert_refusal("pause", "--output", File.join(@tmp, "absent"), message: "execution state not found")
    assert_refusal("pause", message: "missing argument: --output DIR")
    [["--output", @output], ["--execution", @identifier]].each do |selector|
      ["bad", Digest::SHA256.hexdigest(TOKEN).upcase].each do |token|
        assert_refusal("pause", *selector, "--owner-token", token, message: "pause owner token")
      end
    end
  end

  # Neither IDs nor options can reinterpret paths or silently replace selectors.
  def test_invalid_missing_abbreviated_and_conflicting_selectors_write_nothing
    ["", "39b", @plan.id, @identifier[0, 12], @identifier.upcase, "0" * 64,
     ".", "..", "../#{@identifier}", "/#{@identifier}", "a/b", 'a\\b', "a\0b"].each do |identifier|
      assert_refusal("pause", "--execution", identifier)
    end
    [
      ["--execution", @identifier, "--output", @output],
      ["--output", @output, "--execution", @identifier],
      ["--execution", @identifier, "--execution", "0" * 64],
      ["--execution", @identifier, "--execution", @identifier],
      ["--output", @output, "--output", @unrelated],
      ["--execution", @identifier, "--output-root", @root, "--output-root", @work],
      ["--execution", @identifier, "--output-root", ""],
      ["--execution", @identifier, "--output-root", "bad\0root"],
      ["--output", @output, "--output-root", @root],
      ["--output-root", @root],
      ["--execution", @identifier, "--owner-token", TOKEN, "--owner-token", OTHER]
    ].each { |arguments| assert_refusal("pause", *arguments) }
  end

  # Validate frozen bytes and retained identity, never a matching directory name.
  def test_missing_corrupt_incomplete_and_wrongly_bound_metadata_write_nothing
    state_path = File.join(@output, "execution.json")
    original = File.binread(state_path)
    state = JSON.parse(original)
    changes = ["{", "[]", JSON.generate(state.except("workdir")), JSON.generate(state.except("status")),
      JSON.generate(state.merge("contract_version" => "unknown")),
      JSON.generate(state.merge("plan_id" => "other")),
      JSON.generate(state.merge("plan_sha256" => "b" * 64)),
      JSON.generate(state.merge("workdir" => @tmp)),
      JSON.generate(state.merge("workdir" => "relative")),
      JSON.generate(state.merge("workers_sha256" => "bad")),
      JSON.generate(state.merge("circuit_breaker" => nil)),
      JSON.generate(state.merge("terminal_import" => { "phase" => "copying" })),
      JSON.generate(state.merge("execution_profile_sha256" => "b" * 64))]
    changes.each do |bytes|
      File.binwrite(state_path, bytes)
      assert_refusal("pause", "--execution", @identifier)
    end
    File.binwrite(state_path, original)
    %w[execution.json plan.json].each do |name|
      path = File.join(@output, name)
      bytes = File.binread(path)
      File.delete(path)
      assert_refusal("pause", "--execution", @identifier)
      File.binwrite(path, bytes + " ")
      assert_refusal("pause", "--execution", @identifier) if name == "plan.json"
      File.binwrite(path, "{")
      assert_refusal("pause", "--execution", @identifier)
      File.binwrite(path, bytes)
    end
    FileUtils.mkdir_p(File.join(@root, "incomplete-execution"))
    assert_refusal("pause", "--execution", @identifier)
  end

  # A moved/copy's old consumer handle is stale; copied holds cannot be rebound.
  def test_copied_outputs_and_forged_or_corrupt_hold_bindings_refuse
    assert_equal 0, cli("pause", "--execution", @identifier, "--owner-token", TOKEN).first
    request = Dir.glob(File.join(@output, "control/pause-requests/*.json")).fetch(0)
    original = File.binread(request)
    ["{", JSON.generate(JSON.parse(original).merge("consumer_id" => "b" * 64)),
      JSON.generate(JSON.parse(original).merge("request_id" => "b" * 64))].each do |bytes|
      File.binwrite(request, bytes)
      assert_refusal("pause", "--execution", @identifier)
    end
    File.binwrite(request, original)
    copy = File.join(@root, "copied")
    FileUtils.cp_r(@output, copy)
    assert_refusal("pause", "--execution", @identifier, message: "does not belong to this execution")
    FileUtils.remove_entry(copy)
    FileUtils.remove_entry(File.join(@output, "control"))
    File.rename(@output, copy)
    assert_refusal("pause", "--execution", @identifier, message: "not found")
  end

  # Reject root aliases, child aliases and every metadata/control symlink seam.
  def test_symlinks_and_unsafe_control_destinations_refuse_without_writes
    alias_root = File.join(@tmp, "alias")
    File.symlink(@root, alias_root)
    [alias_root, File.join(alias_root, "..", "alias")].each do |root|
      assert_refusal("pause", "--execution", @identifier, "--output-root", root)
    end
    File.symlink(@repo, File.join(@tmp, "ancestor-alias"))
    assert_refusal("pause", "--execution", @identifier, "--output-root", File.join(@tmp, "ancestor-alias/output"))
    %w[execution.json plan.json control control/pause control/pause-requests control/pause-requests/hold.json].each do |name|
      path = File.join(@output, name)
      backup = File.join(@tmp, "saved")
      FileUtils.mkdir_p(File.dirname(path))
      File.rename(path, backup) if File.exist?(path)
      File.symlink(File.join(@tmp, "outside-or-missing"), path)
      assert_refusal("pause", "--execution", @identifier)
      File.delete(path)
      File.rename(backup, path) if File.exist?(backup)
    end
    [@unrelated, @work, File.join(@tmp, "missing")].each do |destination|
      path = File.join(@root, "child-alias")
      File.symlink(destination, path)
      assert_refusal("pause", "--execution", @identifier)
      File.delete(path)
    end
    marker = File.join(@output, "control/pause")
    outside = File.join(@work, "external-hold")
    File.write(outside, "external\n")
    File.link(outside, marker)
    assert_refusal("pause", "--execution", @identifier)
    File.delete(marker)
    FileUtils.mkdir_p(marker)
    assert_refusal("pause", "--execution", @identifier)
  end

  # Inject replacements after the scan and before the owning-control handoff.
  # Revalidation detects root/output replacement and late unsafe control paths.
  def test_lookup_rechecks_directory_identity_and_controls_before_handoff
    [:output, :root, :control].each do |boundary|
      output = @output
      root = @root
      saved = File.join(@tmp, "saved-#{boundary}")
      after_replacement = nil
      snapshot = -> { after_replacement = tree }
      resolver_class = Class.new(WorkloadOrchestrator::PauseTarget) do
        # Run real lookup, then simulate a filesystem replacement before return.
        define_method(:find_output) do |identifier|
          selected = super(identifier)
          path = boundary == :root ? root : output
          if boundary == :control
            File.symlink(saved, File.join(output, "control"))
          else
            File.rename(path, saved)
            FileUtils.cp_r(saved, path)
          end
          snapshot.call
          selected
        end
      end
      error = assert_raises(WorkloadOrchestrator::Error) do
        resolver_class.new(@root).with_output(@identifier) { flunk "unsafe control handoff" }
      end
      assert_match(/changed during lookup|canonical directory/, error.message)
      assert_equal after_replacement, tree
      if boundary == :control
        File.delete(File.join(output, "control"))
      else
        path = boundary == :root ? root : output
        FileUtils.remove_entry(path)
        File.rename(saved, path)
      end
    end
  end

  # Preparation can rewrite retained evidence even if resulting bytes match.
  # Guard the actual owning store and Runner constructors during real lookup.
  def test_lookup_never_prepares_an_execution_or_constructs_a_runner
    constructor = WorkloadOrchestrator::ExecutionStore.method(:new)
    factory = lambda do |**arguments|
      execution = constructor.call(**arguments)
      # Fail specifically if lookup tries to prepare this retained store.
      execution.define_singleton_method(:prepare!) { raise "lookup attempted execution preparation" }
      execution
    end
    unexpected_runner = ->(*) { flunk "lookup constructed a runner" }
    WorkloadOrchestrator::Runner.stub(:new, unexpected_runner) do
      WorkloadOrchestrator::ExecutionStore.stub(:new, factory) do
        code, _, stderr = cli("pause", "--execution", @identifier)
        assert_equal 0, code, stderr
        assert_equal "requested\n", File.binread(File.join(@output, "control/pause"))
      end
    end
  end

  # A real CLI runner finishes active work normally and retains pending work.
  def test_public_cli_pause_drains_running_work_and_preserves_terminal_evidence
    drain_journey
  end

  # Deliberately bypass the manual-control writer in a test-owned CLI process;
  # the same composed proof must fail specifically at the handoff assertion.
  def test_drain_journey_detects_bypassed_control_handoff
    error = assert_raises(Minitest::Assertion) { drain_journey(bypass: true) }
    assert_includes error.message, "pause handoff did not publish control/pause"
  end

  private

  # Exercise actual public run/pause and subprocess completion in disposable
  # roots. A file gate orders the request before running-work completion; all
  # waits and test-owned processes are bounded and reaped, including on failure.
  def drain_journey(bypass: false)
    gate = File.join(@work, "release")
    ready = File.join(@work, "ready")
    running_code = <<~RUBY
      trap("TERM") { File.write("signalled", "TERM"); exit 91 }
      File.write("ready", "running")
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
      until File.exist?("release")
        abort "fixture gate timed out" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        sleep 0.005
      end
      STDOUT.binmode
      STDOUT.write("finished\\x00\\xff")
    RUBY
    path = write_plan(@tmp, name: "drain.json", jobs: [
      job("complete", code: 'STDOUT.write("complete\\x00\\xff")'),
      job("failed", code: 'STDERR.write("failure\\x00\\xff"); exit 7'),
      job("running", code: running_code),
      job("pending", code: 'File.write("unwanted-dispatch", "called")')
    ])
    plan = WorkloadOrchestrator::Plan.load(path)
    output = File.join(@tmp, "drain-root", "execution")
    FileUtils.mkdir_p(File.dirname(output))
    executable = File.expand_path("../bin/wlo", __dir__)
    arguments = [RbConfig.ruby, executable, "run", path, "--workdir", @work,
      "--output", output, "--workers-config", @workers]
    Open3.popen3(*arguments) do |stdin, stdout, stderr, waiter|
      stdin.close
      begin
        Timeout.timeout(5) { sleep 0.005 until File.file?(ready) }
        retained = tree(File.join(output, "runs")).select do |name, _|
          name.start_with?("complete/", "failed/")
        end
        document = WorkloadOrchestrator::ExecutionReport.new(plan: plan, output: output).document
        assert_equal({ "complete" => 1, "failed" => 1, "running" => 1, "pending" => 1 }, document.fetch("counts"))
        identifier = document.dig("pause_control", "consumer_id")
        pause_arguments = ["pause", "--execution", identifier, "--output-root", File.dirname(output)]
        if bypass
          code = <<~RUBY
            class WorkloadOrchestrator::CLI
              # Negative control: acknowledge without writing the manual marker.
              def pause_existing_output(_output); end
            end
            exit WorkloadOrchestrator::CLI.new(ARGV).run
          RUBY
          pause_out, pause_err, status = Open3.capture3(RbConfig.ruby, "-I", File.expand_path("../lib", __dir__),
            "-rworkload_orchestrator", "-e", code, "--", *pause_arguments, chdir: @work)
        else
          pause_out, pause_err, status = public_cli(*pause_arguments)
        end
        assert status.success?, "#{pause_out}\n#{pause_err}"
        assert_includes pause_out, output
        marker = File.join(output, "control/pause")
        assert File.file?(marker), "pause handoff did not publish control/pause"
        assert_equal "requested\n", File.binread(marker)
        File.write(gate, "finish")
        result = Timeout.timeout(8) { waiter.value }
        assert_equal 0, result.exitstatus, "#{stdout.read}\n#{stderr.read}"
        final = WorkloadOrchestrator::ExecutionReport.new(plan: plan, output: output).document
        assert_equal "paused", final.fetch("status")
        assert_equal({ "complete" => 2, "failed" => 1, "running" => 0, "pending" => 1 }, final.fetch("counts"))
        assert_equal retained, tree(File.join(output, "runs")).select { |name, _| name.start_with?("complete/", "failed/") }
        running = final.fetch("jobs").find { |row| row.fetch("job_id") == "running" }
        assert_equal 0, running.fetch("exit_status")
        assert_equal 1, running.fetch("attempt")
        assert_equal "finished\x00\xff".b, File.binread(File.join(output, "runs/running/stdout.log"))
        refute File.exist?(File.join(@work, "signalled"))
        refute File.exist?(File.join(@work, "unwanted-dispatch"))
        refute File.exist?(File.join(output, "runs/pending"))
      ensure
        File.write(gate, "finish")
        unless waiter.join(8)
          Process.kill("TERM", waiter.pid)
          Timeout.timeout(5) { waiter.value }
        end
      end
    end
  end

  # Return an inert store for the frozen synthetic plan and supplied output.
  def store(output)
    WorkloadOrchestrator::ExecutionStore.new(plan: @plan, workdir: @work, output_dir: output)
  end

  # Observe public identity and pause state without writing execution evidence.
  def report(output)
    WorkloadOrchestrator::ExecutionReport.new(plan: @plan, output: output).document
  end

  # Invoke the public CLI in process with a disposable checkout's default root.
  # Returns exit code/stdout/stderr, preserving error diagnostics for assertions.
  def cli(*arguments, cwd: @work)
    stdout = StringIO.new
    stderr = StringIO.new
    code = Dir.chdir(cwd) do
      WorkloadOrchestrator::CLI.new(arguments, root: @repo, out: stdout, err: stderr).run
    end
    [code, stdout.string, stderr.string]
  end

  # Invoke the actual repository executable from another working directory.
  # Only an explicit synthetic output root can be passed to pause in this helper.
  def public_cli(*arguments)
    Open3.capture3(RbConfig.ruby, File.expand_path("../bin/wlo", __dir__), *arguments, chdir: @work)
  end

  # Refusals must preserve every fixture path, file byte, symlink and mode.
  def assert_refusal(*arguments, message: nil)
    before = tree
    code, stdout, stderr = cli(*arguments)
    assert_equal 1, code, "#{stdout}\n#{stderr}"
    assert_empty stdout
    assert_includes stderr, message if message
    assert_equal before, tree
  end

  # Snapshot without following symlinks, including empty directories and binary
  # files, to catch accidental creation or writes outside the selected execution.
  def tree(root = @tmp)
    Find.find(root).to_h do |path|
      stat = File.lstat(path)
      bytes = stat.symlink? ? File.readlink(path) : (stat.file? ? File.binread(path) : nil)
      [path.delete_prefix("#{root}/"), [stat.ftype, stat.mode, bytes]]
    end.reject { |path, _| path == root }
  end
end
