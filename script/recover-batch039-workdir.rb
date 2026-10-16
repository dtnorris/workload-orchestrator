#!/usr/bin/env ruby
# frozen_string_literal: true

require "digest"
require "find"
require "json"
require "open3"
require "optparse"
require "time"
require_relative "../lib/workload_orchestrator"

# A one-time, offline metadata migration for the inspected Batch 039 snapshot.
# This is deliberately separate from WLO's routine identity checks and repair.
class Batch039WorkdirRecovery
  PRECONDITIONS = {
    "snapshot_sha256" => "3fc12311020429507851a2096e7f156f1401f524089fa72b6670c75be08a1969",
    "plan_sha256" => "0396dadb15bf0ca68238fd4f449ca99e58c3a4ee329dadc8a6e242f5da0ae5d1",
    "source_workdir" => "/Users/davidnorris/code/af-workloads-production",
    "target_workdir" => "/Users/davidnorris/code/adventure-finder-components/af-workloads-production",
    "afw_head" => "5b95e284dc13e6dcf8b64a2b7e327e770cc5e8b1"
  }.freeze

  # Bind an existing output directory. No files are created; invalid paths raise
  # WorkloadOrchestrator::Error or the underlying filesystem exception.
  def initialize(output:)
    @output = File.expand_path(output)
  end

  # Return the fixed reviewed provenance; callers cannot supply a new plan,
  # destination or snapshot hash through the command line. Returns a frozen Hash.
  def preconditions
    PRECONDITIONS
  end

  # Preview, or atomically apply, the exact reviewed workdir change. Inputs are
  # an explicit apply flag and audit reason. Returns before/after fingerprints;
  # raises on changed evidence, active locks, target drift or invalid WLO state.
  # Never prepares jobs, retries attempts, clears controls or contacts capacity.
  def recover(apply: false, reason: nil)
    reject!("--apply requires a nonblank --reason") if apply && reason.to_s.strip.empty?
    verify_snapshot!
    plan = WorkloadOrchestrator::Plan.load(File.join(@output, "plan.json"))
    verify_target!(plan)
    source_store = store(plan, preconditions.fetch("source_workdir"))
    check = source_store.action_check("recovery")
    reject!("ordinary retained validation failed: #{check}") unless check.fetch("disposition") == "execute"

    with_existing_lock(".execution.lock") do
      with_existing_lock(".state.lock") do
        verify_snapshot!
        before = File.binread(File.join(@output, "execution.json"))
        result = { "apply" => apply, "preconditions" => preconditions,
                   "execution_before_sha256" => Digest::SHA256.hexdigest(before) }
        return result unless apply

        verify_target!(plan)
        apply_migration!(before, result, reason)
      end
    end
  end

  private

  # Atomically write the audited metadata migration under the caller's locks.
  # Returns the updated provenance; parsing and filesystem errors propagate.
  def apply_migration!(before, result, reason)
    state = JSON.parse(before)
    state["workdir"] = preconditions.fetch("target_workdir")
    state["workdir_migration"] = result.merge(
      "contract_version" => "wlo-batch039-workdir-migration/v0.1",
      "at" => Time.now.utc.iso8601, "reason" => reason
    )
    after = "#{JSON.pretty_generate(state)}\n"
    atomic_replace_execution!(after)
    result.merge("execution_after_sha256" => Digest::SHA256.hexdigest(after))
  end

  # Fingerprint every regular file using sorted [relative path, size, SHA-256]
  # rows encoded as compact JSON. Reject links and special files, including an
  # aliased output root, rather than reading evidence outside the snapshot.
  def verify_snapshot!
    reject!("output root must be a real directory without aliases") unless File.realpath(@output) == @output
    rows = []
    Find.find(@output) do |path|
      info = File.lstat(path)
      next if info.directory?

      reject!("linked or special evidence: #{path}") unless info.file?
      bytes = File.binread(path)
      rows << [path.delete_prefix("#{@output}/"), bytes.bytesize, Digest::SHA256.hexdigest(bytes)]
    end
    digest = Digest::SHA256.hexdigest(JSON.generate(rows.sort))
    reject!("snapshot differs from the reviewed Batch 039 evidence") unless
      digest == preconditions.fetch("snapshot_sha256")
  end

  # Check the authoritative destination checkout and exact frozen plan without
  # invoking AFW, scoring, preflight or a provider. Raises on any checkout drift;
  # success proves only the destination for WLO metadata, not scoring readiness.
  def verify_target!(plan)
    target = preconditions.fetch("target_workdir")
    reject!("target workdir must be a real directory without aliases") unless File.realpath(target) == target
    head, _stderr, status = target_git("rev-parse", "HEAD")
    reject!("AFW target HEAD differs") unless status.success? && head.strip == preconditions.fetch("afw_head")
    changes, _stderr, status = target_git("status", "--porcelain", "--untracked-files=all")
    reject!("AFW target checkout is not clean") unless status.success? && changes.empty?
    path = File.join(target, "workloads", plan.id, "execution-plan.json")
    reject!("target frozen plan differs") unless File.realpath(path) == path && File.binread(path) == plan.bytes &&
                                                 plan.sha256 == preconditions.fetch("plan_sha256")
  end

  # Read Git metadata from the target only, without a shell. Returns stdout,
  # stderr and process status; process-launch errors propagate to the caller.
  def target_git(*)
    Open3.capture3("git", "-C", preconditions.fetch("target_workdir"), *)
  end

  # Construct a read-only validation handle for a specific workdir identity.
  # Returns an ExecutionStore without calling any of its mutation methods.
  def store(plan, workdir)
    WorkloadOrchestrator::ExecutionStore.new(plan: plan, workdir: workdir, output_dir: @output)
  end

  # Hold an existing lock exclusively without creating it. Returns the block's
  # value and raises if another executor/observer currently holds the lock.
  def with_existing_lock(name)
    File.open(File.join(@output, name), File::RDONLY) do |file|
      reject!("execution is active or state is busy") unless file.flock(File::LOCK_EX | File::LOCK_NB)
      yield
    end
  end

  # Replace only execution.json, with its audit embedded in the same atomic
  # rename. The flushed temporary file is exclusive; no attempt is rewritten.
  # Raises filesystem errors; a pre-rename crash leaves a detectable extra file.
  def atomic_replace_execution!(bytes)
    path = File.join(@output, "execution.json")
    temporary = "#{path}.workdir-migration.tmp"
    File.open(temporary, File::WRONLY | File::CREAT | File::EXCL, File.stat(path).mode & 0o777) do |file|
      file.write(bytes)
      file.flush
      file.fsync
    end
    File.rename(temporary, path)
  end

  # Raise a consistent public error for a failed precondition; never retries.
  def reject!(message)
    raise WorkloadOrchestrator::Error, message
  end
end

if $PROGRAM_NAME == __FILE__
  begin
    options = { apply: false }
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: ruby script/recover-batch039-workdir.rb --output DIR [--apply --reason TEXT]"
      opts.on("--output DIR") { |value| options[:output] = value }
      opts.on("--apply") { options[:apply] = true }
      opts.on("--reason TEXT") { |value| options[:reason] = value }
    end
    parser.parse!
    raise OptionParser::InvalidArgument, ARGV.join(" ") unless ARGV.empty?
    raise OptionParser::MissingArgument, "--output DIR" unless options[:output]

    recovery = Batch039WorkdirRecovery.new(output: options.delete(:output))
    puts JSON.pretty_generate(recovery.recover(**options))
  rescue WorkloadOrchestrator::Error, OptionParser::ParseError, SystemCallError => e
    warn "ERROR: #{e.message}"
    exit 1
  end
end
