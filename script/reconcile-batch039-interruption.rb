#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "recover-batch039-workdir"

# Bounded recovery of the reviewed execution-only TERM interruption. The general
# WLO repair contract remains unsupported; this never retries or runs a job.
class Batch039InterruptionRecovery < Batch039WorkdirRecovery
  CONTRACT = "wlo-batch039-interruption-reconciliation/v0.1"
  VERIFICATION_CONTRACT = "wlo-batch039-operator-verification/v0.1"
  MAX_VERIFICATION_AGE = 60

  # Bind retained output and an external, freshly reviewed operator record.
  # Construction is read-only; missing or unsafe paths fail during reconciliation.
  def initialize(output:, verification:)
    super(output: output)
    @verification = File.expand_path(verification)
  end

  # Preview or explicitly apply one reconciliation. Return an auditable result;
  # raise Error/filesystem errors on drift, ambiguous ownership or partial repair.
  # Host inspection is mandatory even for preview and repeated before commit.
  def recover(apply: false, reason: nil)
    reject!("a nonblank --reason is required") if reason.to_s.strip.empty?
    verify_snapshot!
    plan = WorkloadOrchestrator::Plan.load(File.join(@output, "plan.json"))
    verify_target!(plan)
    validate_resume!(plan)

    with_existing_lock(".execution.lock") do
      with_existing_lock(".state.lock") do
        verify_snapshot!
        before = File.binread(File.join(@output, "execution.json"))
        verification = read_verification
        liveness = verify_liveness!(verification)
        result = { "contract_version" => CONTRACT, "apply" => apply, "reason" => reason.strip,
                   "execution_before_sha256" => Digest::SHA256.hexdigest(before),
                   "operator_verification" => verification, "liveness" => liveness }
        return result unless apply

        verify_target!(plan)
        reject!("operator verification changed") unless read_verification == verification
        result["liveness"] = verify_liveness!(verification)
        verify_snapshot!
        commit!(before, result)
      end
    end
  end

  private

  # Keep WLO's ordinary public validation authoritative before taking locks.
  def validate_resume!(plan)
    check = store(plan, preconditions.fetch("target_workdir")).action_check("resume")
    reject!("expected public interrupted_attempts refusal: #{check}") unless
      check.values_at("disposition", "reason") == %w[blocked interrupted_attempts]
  end

  # Normalize ONLY the reviewed migration, then compare every retained file to
  # the original snapshot pin. This checks results, attempts, worker bindings,
  # consumed retries, breaker history, summaries and manager records together.
  def verify_snapshot!
    reject!("output must be a real directory without aliases") unless File.realpath(@output) == @output
    state = JSON.parse(File.binread(File.join(@output, "execution.json")))
    original = original_execution!(state)
    rows = evidence_rows(original: original)
    reject!("snapshot differs or reconciliation is partial/already applied") unless
      Digest::SHA256.hexdigest(JSON.generate(rows)) == preconditions.fetch("snapshot_sha256")
  end

  # Return sorted path/size/digest rows. Only caller-owned commit files may be
  # excluded during final race detection; links and hardlinks always fail.
  def evidence_rows(original: nil, excluding: [])
    rows = []
    Find.find(@output) do |path|
      info = File.lstat(path)
      next if info.directory?

      reject!("linked, shared or special evidence: #{path}") unless info.file? && info.nlink == 1
      relative = path.delete_prefix("#{@output}/")
      next if excluding.include?(relative)

      bytes = relative == "execution.json" && original ? original : File.binread(path)
      rows << [relative, bytes.bytesize, Digest::SHA256.hexdigest(bytes)]
    end
    rows.sort
  end

  # Recover the exact pre-migration serialization and validate its embedded
  # authority. No other state difference, including a repeated repair, is allowed.
  def original_execution!(state)
    migration = state.fetch("workdir_migration")
    valid = migration.keys.sort == %w[apply at contract_version execution_before_sha256 preconditions reason] &&
            migration["contract_version"] == "wlo-batch039-workdir-migration/v0.1" &&
            migration["apply"] == true && migration["preconditions"] == preconditions &&
            !migration["reason"].to_s.strip.empty? &&
            state["workdir"] == preconditions.fetch("target_workdir")
    reject!("reviewed workdir migration is missing or inconsistent") unless valid
    Time.iso8601(migration.fetch("at"))
    original = state.except("workdir_migration").merge("workdir" => preconditions.fetch("source_workdir"))
    bytes = "#{JSON.pretty_generate(original)}\n"
    reject!("migration before-evidence differs") unless
      Digest::SHA256.hexdigest(bytes) == migration.fetch("execution_before_sha256")
    bytes
  end

  # Read an operator-owned acceptance record outside the retained tree. It is
  # an explicit human assertion of source-host identity and remote absence,
  # never an inference from local reboot, manager exit or lock availability.
  def read_verification
    reject!("verification must be external and unaliased") if
      @verification.start_with?("#{@output}/") || File.realpath(@verification) != @verification
    reject!("verification must be a private regular file") unless
      File.lstat(@verification).file? && File.stat(@verification).mode.nobits?(0o077)
    validate_verification!(JSON.parse(File.binread(@verification)))
  end

  # Validate explicit human acceptance fields; return the unchanged record.
  def validate_verification!(document)
    valid = document["contract_version"] == VERIFICATION_CONTRACT &&
            document["snapshot_sha256"] == preconditions.fetch("snapshot_sha256") &&
            document["output"] == @output && document["source_host_confirmed"] == true &&
            document["remote_processes_absent"] == true &&
            %w[operator source_host_uuid remote_evidence].all? { |key| !document[key].to_s.strip.empty? }
    reject!("operator source-host/remote verification is incomplete") unless valid
    document
  end

  # Prove local termination by reboot on the explicitly confirmed source host.
  # Historical PID absence or terminal status alone cannot prove dead children.
  # Same-boot recovery intentionally remains unsupported with this old format.
  def verify_liveness!(verification)
    verify_freshness!(verification)
    host = verified_host!(verification)
    owners = manager_owners!
    boot_at = verify_reboot!(owners)
    verify_owner_pids!(owners)
    verify_freshness!(verification)
    { "source_host_uuid" => host, "boot_at" => boot_at.utc.iso8601,
      "checked_at" => Time.now.utc.iso8601, "owner_launch_ids" => owners.map { |owner| owner.fetch("launch_id") },
      "local_termination" => "source_host_reboot_after_interruption", "remote_absence" => "operator_verified" }
  end

  # Read macOS hardware identity, independently of mutable host names. Missing
  # or multiple platform UUIDs cannot establish the operator-confirmed origin.
  def verified_host!(verification)
    hardware = host_command("ioreg", "-rd1", "-c", "IOPlatformExpertDevice")
    identifiers = hardware.scan(/"IOPlatformUUID" = "([0-9A-Fa-f-]{36})"/).flatten
    valid = identifiers.length == 1 && identifiers.first == verification.fetch("source_host_uuid")
    reject!("source host is unknown or contradictory") unless valid
    identifiers.first
  end

  # Check freshness both before and after potentially slow host facilities.
  def verify_freshness!(verification)
    age = Time.now - Time.iso8601(verification.fetch("checked_at"))
    reject!("operator verification is stale or future-dated") unless age.between?(0, MAX_VERIFICATION_AGE)
  end

  # Return the verified boot time, requiring reboot after all retained owners
  # finished and no earlier than the interrupted execution's terminal boundary.
  def verify_reboot!(owners)
    boot = host_command("sysctl", "-n", "kern.boottime").match(/\A\{ sec = (\d+), usec = \d+ \}.*\n?\z/)
    reject!("host boot evidence is unavailable") unless boot
    boot_at = Time.at(Integer(boot[1]))
    state = JSON.parse(File.binread(File.join(@output, "execution.json")))
    boundary = Time.iso8601(state.fetch("interruption").fetch("at"))
    finished = owners.all? { |owner| Time.iso8601(owner.fetch("finished_at")) <= boundary }
    unless finished && boot_at > boundary && boot_at <= Time.now
      reject!("same-boot or incomplete child ownership evidence")
    end
    boot_at
  end

  # Reject unavailable inventories and every collision with a historical PID,
  # including reuse after reboot; no names or PID-only termination assumptions.
  def verify_owner_pids!(owners)
    pids = host_command("ps", "-axo", "pid=").lines.map do |line|
      reject!("invalid host process inventory") unless line.strip.match?(/\A[1-9]\d*\z/)
      Integer(line.strip)
    end
    reject!("empty or contradictory host process inventory") if pids.empty? || pids.uniq != pids
    reject!("live or reused historical owner PID; review required") if
      owners.map { |owner| owner.fetch("pid") }.intersect?(pids)
  end

  # Validate WLO's historical manager records, allowing only the stale running
  # pointer that DetachedManager deliberately retains after a terminal exit.
  def manager_owners!
    owners = Dir.glob(File.join(@output, "manager", "*.json")).map do |path|
      owner = JSON.parse(File.binread(path))
      validate_owner!(path, owner)
    end
    pointer = JSON.parse(File.binread(File.join(@output, "manager.json")))
    owner = owners.find { |record| record["launch_id"] == pointer["launch_id"] }
    expected = owner&.except("finished_at", "exit_status", "error")&.merge("status" => "running")
    reject!("contradictory manager pointer") unless expected && pointer == expected
    owners
  end

  # Validate one WLO manager launch's positive PID and terminal time interval.
  def validate_owner!(path, owner)
    valid = owner["pid"].is_a?(Integer) && owner["pid"].positive? &&
            File.basename(path) == "#{owner.fetch('launch_id')}.json" &&
            %w[interrupted circuit_broken paused error completed workload_failed].include?(owner["status"]) &&
            owner["exit_status"].is_a?(Integer) &&
            Time.iso8601(owner.fetch("started_at")) <= Time.iso8601(owner.fetch("finished_at"))
    reject!("unknown or incomplete manager ownership") unless valid
    owner
  end

  # Run only read-only host facilities, without shell or fallback inspection.
  # Unavailable/denied inspection raises Error; never bypasses the sandbox.
  def host_command(*arguments)
    stdout, stderr, status = Open3.capture3(*arguments)
    reject!("host inspection unavailable (#{arguments.join(' ')}): #{stderr.strip}") unless status.success?
    stdout
  rescue SystemCallError => e
    reject!("host inspection unavailable: #{e.message}")
  end

  # Publish a durable manual pause before the single atomic state replacement.
  # Crashes before rename retain interruption plus a detectable partial repair;
  # crashes after rename retain the complete embedded audit and pause together.
  def commit!(before, result)
    retained = evidence_rows
    state = reconciled_state(before, result)
    after = "#{JSON.pretty_generate(state)}\n"
    durable_write_new(File.join(@output, "control", "pause"), "#{state.fetch('updated_at')}\n")
    sync_directory(File.join(@output, "control"))
    temporary = File.join(@output, "execution.json.interruption-reconciliation.tmp")
    durable_write_new(temporary, after)
    # Detect non-cooperating evidence writers before committing; WLO writers
    # are excluded throughout by the existing execution and state locks.
    verify_commit!(retained, result, temporary, after)
    File.rename(temporary, File.join(@output, "execution.json"))
    sync_directory(@output)
    result.merge("execution_after_sha256" => Digest::SHA256.hexdigest(after), "status" => "paused")
  end

  # Recheck all external and retained authorities after durable staging. An
  # unsafe late change leaves original interruption and explicit partial files.
  def verify_commit!(retained, result, temporary, after)
    verification = result.fetch("operator_verification")
    reject!("operator verification changed") unless read_verification == verification
    verify_liveness!(verification)
    verify_target!(WorkloadOrchestrator::Plan.load(File.join(@output, "plan.json")))
    verify_freshness!(verification)
    reject!("operator verification changed") unless read_verification == verification
    reject!("retained evidence changed during reconciliation") unless
      evidence_rows(excluding: %w[control/pause execution.json.interruption-reconciliation.tmp]) == retained
    reject!("staged execution changed during reconciliation") unless File.binread(temporary) == after
  end

  # Build the single state commit, retaining exact prior bytes and interruption.
  # Return a new state; neither jobs nor original migration/history are edited.
  def reconciled_state(before, result)
    state = JSON.parse(before)
    interruption = state.delete("interruption")
    (state["interruption_history"] ||= []) << interruption
    state["status"] = "paused"
    state["updated_at"] = Time.now.utc.iso8601
    state["interruption_reconciliation"] = result.merge(
      "at" => state.fetch("updated_at"), "execution_before" => before,
      "original_interruption" => interruption, "resume_requirements" => ["explicit_resume"]
    )
    state
  end

  # Exclusively create, flush and fsync a private recovery file. Existing files
  # are never overwritten; failures deliberately leave diagnostic evidence.
  def durable_write_new(path, bytes)
    File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0o600) do |file|
      file.write(bytes)
      file.flush
      file.fsync
    end
  end

  # Make directory entry publication durable; unsupported fsync fails closed.
  def sync_directory(path)
    File.open(path, File::RDONLY, &:fsync)
  end
end

if $PROGRAM_NAME == __FILE__
  begin
    options = { apply: false }
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: ruby script/reconcile-batch039-interruption.rb " \
                    "--output DIR --verification FILE --reason TEXT [--apply]"
      opts.on("--output DIR") { |value| options[:output] = value }
      opts.on("--verification FILE") { |value| options[:verification] = value }
      opts.on("--reason TEXT") { |value| options[:reason] = value }
      opts.on("--apply") { options[:apply] = true }
    end
    parser.parse!
    raise OptionParser::InvalidArgument, ARGV.join(" ") unless ARGV.empty?

    %i[output verification].each do |key|
      raise OptionParser::MissingArgument, "--#{key}" unless options[key]
    end
    recovery = Batch039InterruptionRecovery.new(output: options.delete(:output),
                                                verification: options.delete(:verification))
    puts JSON.pretty_generate(recovery.recover(**options))
  rescue WorkloadOrchestrator::Error, OptionParser::ParseError, SystemCallError,
         JSON::ParserError, KeyError, TypeError, ArgumentError, IOError => e
    warn "ERROR: #{e.message}"
    exit 1
  end
end
