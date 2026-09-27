# frozen_string_literal: true

require_relative "test_helper"

class JobClaimTest < Minitest::Test
  def setup
    @tmp = Dir.mktmpdir("wlo-claim-")
  end

  def teardown
    FileUtils.rm_rf(@tmp)
  end

  def test_second_process_waits_for_same_job_claim
    ready = File.join(@tmp, "ready")
    acquired = File.join(@tmp, "acquired")
    claim = WorkloadOrchestrator::JobClaim.new(output_dir: @tmp)
    pid = nil

    claim.synchronize("shared-job") do
      pid = spawn_contender(ready, acquired)
      wait_for_file(ready)
      sleep 0.1
      refute File.exist?(acquired)
    end

    Process.wait(pid)
    assert File.file?(acquired)
  ensure
    Process.kill("TERM", pid) if pid && process_alive?(pid)
  end

  private

  def spawn_contender(ready, acquired)
    root = File.expand_path("..", __dir__)
    code = <<~RUBY
      $LOAD_PATH.unshift #{File.join(root, 'lib').inspect}
      require "workload_orchestrator"
      File.write(#{ready.inspect}, "ready")
      WorkloadOrchestrator::JobClaim.new(output_dir: #{@tmp.inspect}).synchronize("shared-job") do
        File.write(#{acquired.inspect}, "acquired")
      end
    RUBY
    Process.spawn(RbConfig.ruby, "-e", code, out: File::NULL, err: File::NULL)
  end

  def wait_for_file(path)
    100.times do
      return if File.file?(path)

      sleep 0.01
    end
    flunk "timed out waiting for contender"
  end

  def process_alive?(pid)
    Process.kill(0, pid)
    true
  rescue Errno::ESRCH
    false
  end
end
