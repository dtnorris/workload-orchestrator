# frozen_string_literal: true

require "digest"
require "json"
require "securerandom"
require "time"

module WorkloadOrchestrator
  # Execution-local scheduling consent. Publisher readiness and running-attempt
  # reconciliation remain independent. All mutations and subprocess starts share
  # one file lock; malformed or missing adopted authority raises Error.
  class SourceOffers
    CONTRACT = "wlo-source-offers/v0.1"
    HEARTBEAT_SECONDS = 2
    MAX_DURATION_SECONDS = 604_800

    # Resolve an existing execution without creating it. Injectable clocks make
    # expiry/rollback tests deterministic. Unsafe/missing paths raise Error.
    def initialize(output, clock: -> { Time.now.utc }, monotonic: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) })
      expanded = File.expand_path(output)
      raise Error, "source offer output must not be a symlink" if File.symlink?(expanded)

      @output = File.realpath(expanded)
      @clock, @monotonic = clock, monotonic
      identity
    rescue SystemCallError => error
      raise Error, "invalid source offer output: #{error.message}"
    end

    # Register immutable configured-source authority under execution ownership.
    # Changed source names/commands/policies raise Error, without running them.
    def register!(sources, authoritative_spawn: true)
      configured = sources.entries.to_h do |entry|
        source = entry.source
        command = source.is_a?(CommandWorkerSource) ? [source.argv, source.environment.sort, source.workdir] : [source.class.name]
        [entry.name, {"policy" => entry.policy, "command_sha256" => Digest::SHA256.hexdigest(JSON.generate(command))}]
      end
      synchronize(create: true) do
        update_state do |state|
          expected = {"binding" => identity, "sources" => configured, "authoritative_spawn" => authoritative_spawn,
                      "lock_identity" => lock_identity}
          if state.key?("source_offer_authority")
            raise Error, "configured source authority changed" unless state["source_offer_authority"] == expected
          else
            raise Error, "missing managed source authority" if state.key?("offer_managed_sources") || File.exist?(record_path)

            state["source_offer_authority"] = expected
          end
        end
        load_record
      end
    end

    # Persist adopt/drain/offer for one optional named source. Adoption excludes
    # until offered. Identical offers never extend time; renewal is explicit.
    # Duration is a positive integer number of seconds (at most seven days).
    def request!(name, action:, duration: nil, renew: false)
      raise Error, "invalid source control action" unless %w[adopt drain offer].include?(action)
      if action == "offer"
        raise Error, "offer duration must be an integer from 1 to #{MAX_DURATION_SECONDS} seconds" unless
          duration.is_a?(String) && duration.match?(/\A[1-9][0-9]*\z/) && duration.to_i <= MAX_DURATION_SECONDS
      elsif duration || renew
        raise Error, "duration/renew applies only to offer"
      end
      synchronize do
        authority = authority!
        raise Error, "source controls require the native subprocess start boundary" unless authority.fetch("authoritative_spawn")
        source = authority.fetch("sources")[name]
        raise Error, "unknown configured source #{name.inspect}" unless source
        raise Error, "offer control requires an optional source; required policy is unchanged" unless source["policy"] == "optional"

        record = load_record
        current = record.fetch("sources")[name]
        if current.nil?
          raise Error, "adopt the source before offering it" unless action == "adopt"

          # Persist the tombstone first: interruption cannot restore legacy permission.
          update_state { |state| state["offer_managed_sources"] = (Array(state["offer_managed_sources"]) + [name]).uniq.sort }
        elsif action == "adopt" || (action == "drain" && current["intent"] == "drain")
          next
        elsif action == "offer" && current["intent"] == "offer" && !renew
          raise Error, "offer already recorded; use --renew to change duration" unless current["duration_seconds"] == duration.to_i
          next
        end

        now = @clock.call.utc
        request = {"revision" => (current ? current.fetch("revision") : 0) + 1,
                   "intent" => action == "offer" ? "offer" : "drain", "requested_at" => now.iso8601(6),
                   "duration_seconds" => action == "offer" ? duration.to_i : nil,
                   "expires_at" => action == "offer" ? (now + duration.to_i).iso8601(6) : nil,
                   "session" => action == "offer" ? record.dig("manager", "session") : nil,
                   "monotonic_expires_at" => action == "offer" ? @monotonic.call + duration.to_i : nil}
        record.fetch("sources")[name] = request
        write_record(record)
      end
      snapshot(name)
    end

    # Establish a new runner incarnation. Retained offers expire conservatively
    # on restart; re-offer with --renew is required, never inferred from READY.
    def start_manager!
      synchronize do
        record = load_record
        inherited = Plan.load(File.join(@output, "plan.json")).jobs.filter_map do |job|
          path = File.join(@output, "runs", job.id, "metadata.json")
          next unless File.exist?(path)

          row = read_json(path)
          [job.id, row.fetch("attempt")] if row["status"] == "running"
        end
        record["manager"] = {"session" => SecureRandom.hex(32), "heartbeat_at" => @clock.call.utc.iso8601(6),
                             "inherited_attempts" => inherited, "executor_token" => executor_token}
        write_record(record)
        record.fetch("manager").fetch("session")
      end
    end

    # Own an incarnation lock for the acknowledgment thread's entire lifetime.
    # This distinguishes a running manager from an unrelated read-only action
    # briefly probing/owning the execution lock. Startup errors release ownership.
    def with_manager_ownership
      path = File.join(@output, ".source-offers-manager.lock")
      regular!(path, missing: true)
      File.open(path, File::RDWR | File::CREAT, 0o600) do |lock|
        raise Error, "source manager is already active" unless lock.flock(File::LOCK_EX | File::LOCK_NB)

        lock.rewind
        lock.truncate(0)
        lock.write(JSON.generate("token" => SecureRandom.hex(32)) + "\n")
        lock.flush
        lock.fsync
        yield start_manager!
      end
    end

    # Acknowledge requests independently of publisher latency. The same lock is
    # held through actual Process.spawn, so selected but unstarted jobs cannot
    # escape a later exclusion. Returns nil; invalid control evidence raises.
    def acknowledge!(session)
      synchronize do
        record = load_record
        raise Error, "source control manager changed" unless record.dig("manager", "session") == session

        token = executor_token
        previous_token = record.dig("manager", "executor_token")
        raise Error, "source control executor incarnation changed" if previous_token && previous_token != token
        record["manager"]["executor_token"] = token

        now = @clock.call.utc.iso8601(6)
        record["manager"]["heartbeat_at"] = now
        record.fetch("sources").each_value do |request|
          request["acknowledgment"] = {"revision" => request.fetch("revision"), "session" => session,
                                       "eligible" => eligible?(request, record), "at" => now}
        end
        write_record(record)
      end
      nil
    end

    # Filter only assignment eligibility. The caller must keep its original
    # current-workers/checkpoints for loss reconciliation and running bindings.
    def eligible_workers(workers, source_registry_ids)
      synchronize do
        record = load_record
        workers.select do |worker|
          name = source_registry_ids.key(worker.registry_id)
          raise Error, "worker has no configured source authority" unless name

          request = record.fetch("sources")[name]
          request.nil? || eligible?(request, record)
        end
      end
    end

    # Recheck after the job claim, immediately before binding and subprocess
    # start. Yield an idempotent release callback called by the real executor
    # only after spawn. Executors without that callback hold the lock to return.
    def dispatch(name)
      synchronize do |release|
        record = load_record
        raise Error, "unknown dispatch source" unless authority!.fetch("sources").key?(name)
        request = record.fetch("sources")[name]
        yield release if request.nil? || eligible?(request, record)
      end
    end

    # Public status separates retained intent, live acknowledgment, publisher
    # health, and WLO-owned attempts. Dead/stale owners never establish quiescence.
    # A specific name returns one row; nil returns all registered source rows.
    def snapshot(name = nil)
      synchronize do
        authority = authority!(required: false)
        next(name ? (raise Error, "execution has no registered source authority") : []) unless authority

        sources = authority.fetch("sources")
        raise Error, "unknown configured source #{name.inspect}" if name && !sources.key?(name)

        record = load_record
        health = WorkerSourceHealth.load_root(File.join(@output, "dynamic-workers"), clock: @clock)
        validate_health!(health, sources)
        plan = Plan.load(File.join(@output, "plan.json"))
        known = read_json(File.join(@output, "jobs.json")).fetch("jobs").to_h { |row| [row.fetch("job_id"), row] }
        rows = plan.jobs.flat_map do |job|
          current = File.join(@output, "runs", job.id, "metadata.json")
          raise Error, "known attempt metadata is missing" if known.fetch(job.id)["attempt"] && !File.exist?(current)

          archived = Dir.glob(File.join(@output, "attempts", job.id, "attempt-*", "metadata.json"))
          ([current] + archived).filter_map do |path|
            next unless File.exist?(path)

            row = read_json(path)
            raise Error, "unknown attempt status" unless (ExecutionStore::TERMINAL_JOB_STATUSES + ["running"]).include?(row["status"])
            raise Error, "archived attempt cannot be running" if path != current && row["status"] == "running"
            DynamicWorkerBinding.from_metadata(row) if row["status"] == "running" || row.key?("worker_execution_identity")
            row
          end
        end
        selected = name ? [name] : sources.keys.sort
        result = selected.map { |source_name| status_row(source_name, sources.fetch(source_name), record, health, rows) }
        name ? result.first : result
      end
    end

    # Read-only display projection of consent, without taking the spawn lock
    # while a caller may hold the scheduler mutex. Concurrent adoption or damaged
    # evidence yields unknown/excluded display capacity, never permission. Only
    # snapshot provides the authoritative live acknowledgment boundary.
    def scheduling_view(health)
      record = load_record
      sources = authority!.fetch("sources").keys.to_h do |name|
        request = record.fetch("sources")[name]
        [name, {"mode" => request ? "managed" : "legacy/unmanaged",
                "scheduling_eligible" => request.nil? || eligible?(request, record),
                "offer_expires_at" => request && request["expires_at"]}]
      end
      excluded = health.filter_map do |row|
        row["registry_id"] if sources.dig(row["source_name"], "scheduling_eligible") == false
      end
      {"uncertain" => false, "excluded_registry_ids" => excluded, "sources" => sources}
    rescue Error, SystemCallError, JSON::ParserError, KeyError, TypeError, ArgumentError => error
      {"uncertain" => true, "excluded_registry_ids" => [], "sources" => {}, "error" => error.message}
    end

    # Check observed display consent without conflating it with qualification.
    # The real dispatcher always revalidates under the cross-process start lock.
    def self.scheduling_eligible?(report, worker)
      view = report["source_scheduling"]
      view.nil? || (!view.fetch("uncertain") && !view.fetch("excluded_registry_ids").include?(worker.registry_id))
    end

    private

    # Serialize independent CLI/runner processes and threads. Release can happen
    # at spawn without holding the lock during inference; ensure covers failures.
    def synchronize(create: false)
      path = File.join(@output, ".source-offers.lock")
      if create && !File.exist?(path) && read_json(File.join(@output, "execution.json")).key?("source_offer_authority")
        raise Error, "source synchronization authority is missing"
      end
      regular!(path, missing: create)
      File.open(File.join(@output, ".source-offers.lock"), File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)
        released = false
        release = lambda do
          unless released
            lock.flock(File::LOCK_UN)
            released = true
          end
        end
        begin
          yield release
        ensure
          release.call
        end
      end
    rescue SystemCallError, JSON::ParserError, KeyError, TypeError, ArgumentError => error
      raise Error, "invalid source offer evidence: #{error.message}"
    end

    # Bind to frozen plan, execution incarnation, real output inode and workdir.
    # A copied output, replaced directory or changed plan fails closed.
    def identity
      state = read_json(File.join(@output, "execution.json"))
      regular!(File.join(@output, "plan.json"))
      plan = Plan.load(File.join(@output, "plan.json"))
      raise Error, "invalid source offer execution identity" unless
        state["contract_version"] == ExecutionStore::CONTRACT_VERSION && state["plan_id"] == plan.id &&
        state["plan_sha256"] == plan.sha256 && plan.priority_scheduling? &&
        state["workdir"].is_a?(String) && state["workdir"].start_with?("/") && state["created_at"].is_a?(String)

      stat = File.stat(@output)
      Digest::SHA256.hexdigest(JSON.generate([@output, stat.dev, stat.ino, state.values_at("plan_sha256", "workdir", "created_at")]))
    end

    # Validate retained configured authority and its immutable output binding.
    def authority!(required: true)
      state = read_json(File.join(@output, "execution.json"))
      authority = state["source_offer_authority"]
      if authority.nil?
        raise Error, "missing source authority" if required || state.key?("offer_managed_sources") || File.exist?(record_path)
        return nil
      end
      raise Error, "foreign or corrupt source authority" unless authority.is_a?(Hash) &&
        authority.keys.sort == %w[authoritative_spawn binding lock_identity sources] && authority["binding"] == identity &&
        authority["lock_identity"] == lock_identity &&
        [true, false].include?(authority["authoritative_spawn"]) &&
        authority["sources"].is_a?(Hash) && !authority["sources"].empty? &&
        authority["sources"].all? do |name, row|
          name.is_a?(String) && WorkerSourceSet::NAME.match?(name) && row.is_a?(Hash) &&
            row.keys.sort == %w[command_sha256 policy] && WorkerSourceSet::POLICIES.include?(row["policy"]) &&
            row["command_sha256"].is_a?(String) && PauseRequests::TOKEN.match?(row["command_sha256"])
        end
      authority
    end

    # Read strict managed state. The execution tombstone makes a deleted record
    # distinguishable from genuine legacy/unmanaged scheduling.
    def load_record
      authority = authority!
      managed = read_json(File.join(@output, "execution.json")).fetch("offer_managed_sources", [])
      raise Error, "invalid managed source marker" unless managed.is_a?(Array) && managed == managed.uniq.sort &&
        managed.all? { |name| authority.fetch("sources").dig(name, "policy") == "optional" }
      unless File.exist?(record_path) || File.symlink?(record_path)
        raise Error, "managed source record is missing" unless managed.empty?
        return {"contract_version" => CONTRACT, "binding" => identity, "sources" => {}, "manager" => nil}
      end
      record = read_json(record_path)
      raise Error, "foreign or corrupt managed source record" unless record.is_a?(Hash) &&
        record.keys.sort == %w[binding contract_version manager sources] && record["contract_version"] == CONTRACT &&
        record["binding"] == identity && record["sources"].is_a?(Hash) && record["sources"].keys.sort == managed
      manager = record["manager"]
      raise Error, "invalid source manager evidence" unless manager.nil? ||
        (manager.is_a?(Hash) && manager.keys.sort == %w[executor_token heartbeat_at inherited_attempts session] &&
         manager["session"].is_a?(String) && PauseRequests::TOKEN.match?(manager["session"]) && timestamp?(manager["heartbeat_at"]) &&
         (manager["executor_token"].nil? || (manager["executor_token"].is_a?(String) && PauseRequests::TOKEN.match?(manager["executor_token"]))) &&
         manager["inherited_attempts"].is_a?(Array) && manager["inherited_attempts"].all? do |row|
           row.is_a?(Array) && row.length == 2 && row.first.is_a?(String) && row.last.is_a?(Integer) && row.last.positive?
         end)
      record.fetch("sources").each_value { |request| validate_request!(request) }
      record
    end

    # Reject unknown fields/states, malformed leases and forged acknowledgment
    # shapes. The public status never repairs ambiguous authority.
    def validate_request!(request)
      keys = %w[duration_seconds expires_at intent monotonic_expires_at requested_at revision session]
      raise Error, "invalid source request" unless request.is_a?(Hash) &&
        (request.keys - ["acknowledgment"]).sort == keys && request["revision"].is_a?(Integer) &&
        request["revision"].positive? && %w[offer drain].include?(request["intent"]) && timestamp?(request["requested_at"])
      if request["intent"] == "offer"
        duration = request["duration_seconds"]
        raise Error, "invalid source offer lease" unless duration.is_a?(Integer) && duration.between?(1, MAX_DURATION_SECONDS) &&
          timestamp?(request["expires_at"]) && Time.iso8601(request["expires_at"]) - Time.iso8601(request["requested_at"]) == duration &&
          request["monotonic_expires_at"].is_a?(Numeric) && request["monotonic_expires_at"].finite? &&
          (request["session"].nil? || (request["session"].is_a?(String) && PauseRequests::TOKEN.match?(request["session"])))
      else
        raise Error, "drain carries offer authority" unless request.values_at("duration_seconds", "expires_at", "monotonic_expires_at", "session").all?(&:nil?)
      end
      acknowledgment = request["acknowledgment"]
      return unless acknowledgment

      raise Error, "invalid source acknowledgment" unless acknowledgment.is_a?(Hash) &&
        acknowledgment.keys.sort == %w[at eligible revision session] && acknowledgment["revision"] == request["revision"] &&
        [true, false].include?(acknowledgment["eligible"]) && timestamp?(acknowledgment["at"]) &&
        acknowledgment["session"].is_a?(String) && PauseRequests::TOKEN.match?(acknowledgment["session"])
    end

    # Evaluate both clocks within one manager incarnation. A clock rollback or
    # restarted manager excludes capacity; observation never extends the lease.
    def eligible?(request, record)
      now = @clock.call.utc
      request["intent"] == "offer" && !request["session"].nil? && request["session"] == record.dig("manager", "session") &&
        now >= Time.iso8601(request.fetch("requested_at")) && now < Time.iso8601(request.fetch("expires_at")) &&
        @monotonic.call < request.fetch("monotonic_expires_at")
    end

    # Build a source-scoped, conservative public observation. Fully drained is
    # limited to WLO attempts and requires a live owner and healthy publication.
    def status_row(name, source, record, health, rows)
      runtime = health.find { |row| row["source_name"] == name }
      request = record.fetch("sources")[name]
      eligibility = request.nil? || eligible?(request, record)
      acknowledgment = request && request["acknowledgment"]
      heartbeat = record.dig("manager", "heartbeat_at")
      age = heartbeat && @clock.call.utc - Time.iso8601(heartbeat)
      token = executor_token
      live = executor_active? && token && token == record.dig("manager", "executor_token") &&
        token == executor_token && age && age.between?(0, HEARTBEAT_SECONDS)
      effective = !!(request && live && acknowledgment && acknowledgment["session"] == record.dig("manager", "session") &&
        acknowledgment["eligible"] == eligibility)
      registry_id = runtime && runtime["registry_id"]
      attempts = rows.select { |row| registry_id && row.dig("worker_execution_identity", "registry_id") == registry_id }
      active = attempts.count { |row| row["status"] == "running" }
      uncertain = attempts.any? do |row|
        row.dig("evidence", "kind") == "dynamic_worker_loss_in_doubt" ||
          (row["status"] == "running" && Array(record.dig("manager", "inherited_attempts")).include?([row["job_id"], row["attempt"]]))
      end
      healthy = runtime && runtime["usable"] && runtime["last_poll_result"] == "success"
      {"source_name" => name, "execution_id" => identity, "registry_id" => registry_id, "policy" => source.fetch("policy"),
       "mode" => request ? "managed" : "legacy/unmanaged", "offer_eligible" => request ? eligibility : nil,
       "scheduling_eligible" => eligibility, "offer_expires_at" => request && request["expires_at"],
       "requested" => request, "effective" => effective, "authoritative_boundary" => effective,
       "active_attempt_count" => active, "inference_running" => live && !uncertain && active.positive? ? true : nil,
       "runtime_health" => runtime, "manager_live" => !!live,
       "drain_result" => effective && !eligibility && healthy && registry_id && !uncertain ? (active.zero? ? "fully_drained" : "draining") : "uncertain"}
    end

    # Observe the existing executor lock without creating it or trusting a PID.
    def executor_active?
      %w[.execution.lock .source-offers-manager.lock].all? do |name|
        path = File.join(@output, name)
        next false unless File.exist?(path)

        regular!(path)
        File.open(path, File::RDONLY) { |file| !file.flock(File::LOCK_EX | File::LOCK_NB) }
      end
    end

    # Read the control owner's lock incarnation. Missing/empty lock content
    # is unknown; it cannot make a retained source acknowledgment current.
    def executor_token
      path = File.join(@output, ".source-offers-manager.lock")
      return nil unless File.exist?(path)

      document = read_json(path)
      token = document["token"]
      return token if document.keys == ["token"] && token.is_a?(String) && PauseRequests::TOKEN.match?(token)

      nil
    rescue JSON::ParserError
      nil
    end

    # Accept only canonical parseable UTC timestamps, returning false on errors.
    def timestamp?(value)
      value.is_a?(String) && Time.iso8601(value).utc.iso8601(6) == value
    rescue ArgumentError
      false
    end

    # Validate regular paths beneath the execution before reads/writes; links,
    # directories and hard-linked records are not control authority.
    def regular!(path, missing: false, retry_replacement: false)
      parent = File.dirname(path)
      raise Error, "unsafe source control directory" unless File.realpath(parent) == parent
      return if missing && !File.exist?(path) && !File.symlink?(path)

      stat = File.lstat(path)
      return false if retry_replacement && stat.file? && stat.nlink.zero?
      raise Error, "unsafe source control path #{path}" unless stat.file? && stat.nlink == 1

      true
    end

    # Parse only validated regular JSON files. A concurrent atomic execution
    # update can unlink the inode during lstat; retry only that transient case,
    # at most three times. Hard links and unsafe paths still raise immediately.
    def read_json(path)
      3.times do
        next unless regular!(path, retry_replacement: true)

        document = JSON.parse(File.binread(path))
        raise Error, "source evidence must be an object" unless document.is_a?(Hash)

        return document
      end
      raise Error, "source evidence repeatedly replaced during inspection"
    end

    # Keep a replaced/deleted synchronization file from creating a second lock
    # namespace for retained authority. A fresh preparation must repair damage.
    def lock_identity
      stat = File.stat(File.join(@output, ".source-offers.lock"))
      [stat.dev, stat.ino]
    end

    # Reuse the canonical checkpoint validator without invoking a publisher.
    # Failed, stale or missing health never establishes a fully drained result.
    def validate_health!(health, sources)
      registry_ids = []
      health.each do |row|
        name = row.fetch("source_name")
        raise Error, "health has foreign source authority" unless sources[name] && sources[name]["policy"] == row["policy"]

        directory = File.join(@output, "dynamic-workers", "sources", name)
        directory = File.join(@output, "dynamic-workers") unless File.directory?(directory)
        regular!(File.join(directory, "health.json"))
        path = File.join(directory, "checkpoint.json")
        regular!(path, missing: true)
        poller = WorkerRegistryPoller.new(source: StaticWorkerSource.new("{}"), checkpoint_path: path)
        checkpoint = poller.accepted_checkpoint
        raise Error, "health/checkpoint identity changed" unless checkpoint&.fetch("registry_id") == row["registry_id"]
        registry_ids << row["registry_id"] if row["registry_id"]
      end
      raise Error, "duplicate source registry identity" unless registry_ids.uniq == registry_ids
    end

    # Change execution metadata under the same state lock as ExecutionStore.
    # This never rewrites the frozen plan or live source configuration.
    def update_state
      path = File.join(@output, ".state.lock")
      regular!(path, missing: true)
      File.open(path, File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)
        state = read_json(File.join(@output, "execution.json"))
        yield state
        atomic_write(File.join(@output, "execution.json"), state)
      end
    end

    # Publish complete control evidence with durable file and directory sync.
    def write_record(record)
      atomic_write(record_path, record)
    end

    # Return the single execution-local control record path.
    def record_path
      File.join(@output, "source-offers.json")
    end

    # Atomically replace a regular record, rejecting pre-existing unsafe paths.
    def atomic_write(path, document)
      regular!(path, missing: true)
      temporary = "#{path}.tmp.#{SecureRandom.hex(12)}"
      File.open(temporary, File::WRONLY | File::CREAT | File::EXCL, 0o600) do |file|
        file.write(JSON.generate(document) + "\n")
        file.flush
        file.fsync
      end
      File.rename(temporary, path)
      File.open(File.dirname(path), File::RDONLY, &:fsync)
    ensure
      File.delete(temporary) if temporary && File.exist?(temporary)
    end
  end
end
