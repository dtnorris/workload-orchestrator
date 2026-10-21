# frozen_string_literal: true

require_relative "test_helper"
require "digest"

# Verify correlation, binary retention and failure ordering at the public poller.
class SourceObservationTest < Minitest::Test
  include WloTestSupport

  # Construct a valid empty registry and a fake command with binary stderr.
  def setup
    @tmp = Dir.mktmpdir("wlo-source-observation-")
    @now = Time.utc(2030)
    @snapshot = JSON.generate("contract_version" => "dynamic-worker-registry/v0.1", "registry_id" => "registry",
                              "revision" => 1, "published_at" => @now.iso8601,
                              "expires_at" => (@now + 30).iso8601, "workers" => [])
    @stderr = "omission diagnostic\x00\xff".b
    executor = ->(*) { command_result(stdout: @snapshot, stderr: @stderr) }
    @source = WorkloadOrchestrator::CommandWorkerSource.new("fake-publisher", workdir: @tmp, command_executor: executor)
    @checkpoint = File.join(@tmp, "checkpoint.json")
    @poller = WorkloadOrchestrator::WorkerRegistryPoller.new(
      source: @source, checkpoint_path: @checkpoint, clock: -> { @now }
    )
  end

  # Remove the test-owned journal and checkpoint.
  def teardown
    FileUtils.remove_entry(@tmp)
  end

  def test_empty_observation_retains_exact_binary_channels_and_registry_identity
    @poller.poll_once
    receipt_path = Dir.glob(File.join(@tmp, "observations/*/receipt.json")).fetch(0)
    receipt = JSON.parse(File.binread(receipt_path))
    dir = File.dirname(receipt_path)
    assert_equal @snapshot, File.binread(File.join(dir, "stdout.bin"))
    assert_equal @stderr, File.binread(File.join(dir, "stderr.bin"))
    assert_equal Digest::SHA256.hexdigest(@stderr), receipt.fetch("stderr_sha256")
    assert_equal @poller.accepted_checkpoint.fetch("snapshot_sha256"), receipt.fetch("snapshot_sha256")
    assert_equal "registry", receipt.fetch("registry_id")
    assert_equal 1, receipt.fetch("revision")
    assert_equal 0, receipt.fetch("worker_count")
    assert_equal "validated_before_checkpoint", receipt.fetch("phase")
  end

  def test_failed_retention_cannot_accept_a_new_checkpoint
    File.write(File.join(@tmp, "observations"), "blocked directory")
    assert_raises(WorkloadOrchestrator::Error) { @poller.poll_once }
    assert_nil @poller.accepted_checkpoint
    refute File.exist?(@checkpoint)
    assert_equal "failure", @poller.last_poll_result
  end

  def test_symlink_journal_is_rejected_without_acceptance
    dir = File.join(@tmp, "other")
    FileUtils.mkdir_p(dir)
    File.symlink(dir, File.join(@tmp, "observations"))
    assert_raises(WorkloadOrchestrator::Error) { @poller.poll_once }
    assert_nil @poller.accepted_checkpoint
    assert_empty Dir.children(dir)
  end
end
