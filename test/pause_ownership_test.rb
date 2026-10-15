# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

# Exercises durable public pause ownership with one synthetic command worker.
class PauseOwnershipTest < Minitest::Test
  include WloTestSupport
  TOKEN = "a" * 64
  OTHER = "b" * 64

  # Build one pending execution; no worker or network service is started.
  def setup
    @tmp = Dir.mktmpdir("pause-ownership-")
    @work = File.join(@tmp, "work")
    @output = File.join(@tmp, "output")
    FileUtils.mkdir_p(@work)
    @path = write_plan(@tmp, jobs: [job("one", code: 'File.open("calls", "a") { |io| io.puts "one" }')])
    @plan = WorkloadOrchestrator::Plan.load(@path)
    @workers_path = write_workers(@tmp)
    @workers = WorkloadOrchestrator::WorkerSet.load(@workers_path)
    @calls = []
    runner.store.prepare!
  end

  # Remove only this test's disposable execution tree.
  def teardown
    FileUtils.remove_entry(@tmp)
  end

  # Both request orderings retain external authority after scoped continuation.
  def test_external_pause_before_and_after_owned_request_survives_scoped_resume
    [:before, :after].each do |ordering|
      @output = File.join(@tmp, ordering.to_s)
      runner.store.prepare!
      cli("pause", "--output", @output) if ordering == :before
      owned_pause
      cli("pause", "--output", @output) if ordering == :after
      marker = File.binread(File.join(@output, "control/pause"))
      assert_equal "paused", runner.run(resume: true, release_pause: TOKEN)
      assert_empty @calls
      assert_equal marker, File.binread(File.join(@output, "control/pause"))
      assert_equal true, report.fetch("pause_control").fetch("operator_pause")
      assert_equal "completed", runner.run(resume: true)
      assert_equal ["one"], @calls
      @calls.clear
    end
  end

  # A caller's token cannot release a different owner's hold or a missing hold.
  def test_other_owner_forged_stale_and_missing_tokens_fail_closed
    owned_pause
    owned_pause(OTHER)
    assert_equal "paused", runner.run(resume: true, release_pause: TOKEN)
    assert_equal [Digest::SHA256.hexdigest(OTHER)], report.dig("pause_control", "request_ids")
    before = request_bytes
    [TOKEN, Digest::SHA256.hexdigest(OTHER), "c" * 64, "not-a-token"].each do |token|
      assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true, release_pause: token) }
      assert_equal before, request_bytes
      assert_empty @calls
    end
    assert_equal "completed", runner.run(resume: true, release_pause: OTHER)
    assert_equal ["one"], @calls
    assert_equal "completed", runner.run(resume: true, preserve_pauses: true)
    assert_equal ["one"], @calls
  end

  # Corrupt and execution-copied authority cannot be released by scoped resume.
  def test_corrupt_and_wrong_execution_ownership_is_not_authority
    owned_pause
    path = Dir.glob(File.join(@output, "control/pause-requests/*.json")).fetch(0)
    original = File.binread(path)
    ["broken", JSON.generate(JSON.parse(original).merge("consumer_id" => "0" * 64))].each do |bytes|
      File.binwrite(path, bytes)
      assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true, release_pause: TOKEN) }
      assert_equal bytes, File.binread(path)
      assert_empty @calls
    end
    File.binwrite(path, original)
    assert_equal "completed", runner.run(resume: true, release_pause: TOKEN)
  end

  # A legacy manual marker is never released by an automatic, unscoped restart.
  def test_preserve_pauses_and_legacy_marker_require_manual_resume
    cli("pause", "--output", @output)
    before = File.binread(File.join(@output, "control/pause"))
    assert_equal "paused", runner.run(resume: true, preserve_pauses: true)
    assert_equal before, File.binread(File.join(@output, "control/pause"))
    assert_empty @calls
    assert_equal "completed", runner.run(resume: true)
  end

  # Pipes force an operator request across either side of the scoped unlink;
  # no timing assumption or competing worker is needed to prove independence.
  def test_external_request_at_release_boundary_cannot_be_erased
    [:before_unlink, :after_release].each do |boundary|
      3.times do
        owned_pause
        ready_reader, ready_writer = IO.pipe
        go_reader, go_writer = IO.pipe
        pid = fork do
          ready_reader.close
          go_writer.close
          if boundary == :before_unlink
            hook = Module.new do
              define_method(:delete) do |*paths|
                if paths.any? { |path| path.include?("pause-requests/") }
                  ready_writer.write("ready")
                  go_reader.read(1)
                end
                super(*paths)
              end
            end
            File.singleton_class.prepend(hook)
          else
            hook = Module.new do
              define_method(:release!) do |*args|
                result = super(*args)
                ready_writer.write("ready")
                go_reader.read(1)
                result
              end
            end
            WorkloadOrchestrator::PauseRequests.prepend(hook)
          end
          exit! runner.run(resume: true, release_pause: TOKEN) == "paused" ? 0 : 9
        end
        ready_writer.close
        go_reader.close
        Timeout.timeout(10) { assert_equal "ready", ready_reader.read(5) }
        cli("pause", "--output", @output)
        go_writer.write("g")
        go_writer.close
        assert_equal 0, Timeout.timeout(10) { Process.waitpid2(pid).last.exitstatus }
        pid = nil
        assert report.fetch("paused")
        assert report.dig("pause_control", "operator_pause")
        assert_empty @calls
        runner.store.clear_pause!
      ensure
        if pid
          Process.kill("KILL", pid) rescue Errno::ESRCH
          Process.waitpid(pid) rescue Errno::ECHILD
        end
        ready_reader&.close unless ready_reader&.closed?
        go_writer&.close unless go_writer&.closed?
      end
    end
  end

  # A broken request-directory link is corrupt evidence, never an empty hold set.
  def test_corrupt_request_directory_cannot_authorize_preserving_resume
    control = File.join(@output, "control")
    FileUtils.mkdir_p(control)
    File.symlink(File.join(@tmp, "missing"), File.join(control, "pause-requests"))
    assert_raises(WorkloadOrchestrator::Error) { runner.run(resume: true, preserve_pauses: true) }
    assert_empty @calls
  end

  # Public fresh-process scoped/manual resumes preserve the one-attempt contract.
  def test_public_cli_scoped_resume_and_manual_continuation
    owned_pause
    cli("pause", "--output", @output)
    arguments = [@path, "--workdir", @work, "--output", @output, "--workers-config", @workers_path]
    stdout, stderr, status = Open3.capture3(RbConfig.ruby, File.expand_path("../bin/wlo", __dir__),
      "resume", *arguments, "--release-pause", TOKEN)
    assert status.success?, "#{stdout}\n#{stderr}"
    assert report.fetch("paused")
    refute File.exist?(File.join(@work, "calls"))
    2.times do
      stdout, stderr, status = Open3.capture3(RbConfig.ruby, File.expand_path("../bin/wlo", __dir__), "resume", *arguments)
      assert status.success?, "#{stdout}\n#{stderr}"
    end
    assert_equal ["one"], File.readlines(File.join(@work, "calls"), chomp: true)
    assert_equal 1, report.fetch("jobs").first.fetch("attempt")
  end

  # Scoping options cannot be silently ignored by run or detached start.
  def test_scope_options_are_only_accepted_for_foreground_resume
    before = request_bytes
    %w[run start].each do |command|
      err = StringIO.new
      code = WorkloadOrchestrator::CLI.new([command, @path, "--workdir", @work, "--output", @output,
        "--workers-config", @workers_path, "--preserve-pauses"], out: StringIO.new, err: err).run
      assert_equal 1, code
      assert_includes err.string, "invalid option"
      assert_equal before, request_bytes
      assert_empty @calls
    end
  end

  private

  # Return one runner with a synthetic executor recording dispatched job IDs.
  def runner
    WorkloadOrchestrator::Runner.new(plan: @plan, workers: @workers, workdir: @work,
      output_dir: @output, out: StringIO.new,
      command_executor: ->(_environment, *argv, chdir:) { @calls << "one"; command_result })
  end

  # Issue an owned pause through the public CLI; failures remain test failures.
  def owned_pause(token = TOKEN)
    cli("pause", "--output", @output, "--owner-token", token)
  end

  # Run the real CLI in process, asserting the public command succeeds.
  def cli(*args)
    err = StringIO.new
    assert_equal 0, WorkloadOrchestrator::CLI.new(args, out: StringIO.new, err: err).run, err.string
  end

  # Read ownership through the public execution-report contract.
  def report
    WorkloadOrchestrator::ExecutionReport.new(plan: @plan, output: @output).document
  end

  # Snapshot every owned hold's literal bytes to detect unauthorized clearing.
  def request_bytes
    Dir.glob(File.join(@output, "control/pause-requests/*")).sort.to_h { |path| [path, File.binread(path)] }
  end
end
