# frozen_string_literal: true

require "fileutils"
require "thread"
require "tempfile"
require_relative "paid_budget"

module WorkloadOrchestrator
  # A library seam for future fulfillment/dispatch. It never provisions resources.
  # RPOF remains the durable accounting and independent enforcement authority.
  class PaidBudgetLifecycle
    RECORD_VERSION = "wlo-paid-budget-binding/v0.1"

    def initialize(budget:, client:, binding_path:, clock: -> { Time.now.utc })
      @budget = budget
      @client = client
      @path = File.expand_path(binding_path)
      @clock = clock
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @operations = Mutex.new
      @running = false
      @started = false
      @suspended = false
      @failure = nil
    end

    def start!
      raise Error, "budget lifecycle already started" if @started
      acquire_binding!
      @started = true
      begin
        bind_declaration!
        # Persist attempted arm before contacting RPOF. On restart, never re-arm
        # or refresh a stale heartbeat: evaluate the same ledger and deadline.
        @provider_touched = true
        snapshot = @new_binding ? @client.arm_budget(budget: @budget) : @client.budget_status(budget: @budget)
        verify!(snapshot)
        verify!(@client.heartbeat_budget(budget: @budget))
        @mutex.synchronize { @running = true }
        @thread = Thread.new { heartbeat_loop }
        @budget.bounds.merge("deadline_at_utc" => @deadline)
      rescue StandardError => e
        fail_closed!(e)
        release_binding!
        raise Error, "paid budget startup refused: #{e.message}"
      end
    end

    # Refresh readiness before future capacity operations. This is NOT a resource
    # reservation; RPOF must also reserve liability atomically before any mutation.
    def check!
      @operations.synchronize do
        raise Error, "budget lifecycle is not active" unless @mutex.synchronize { @running }
        raise Error, "budget lifecycle failed: #{@failure.message}" if @failure
        verify!(@client.budget_status(budget: @budget))
      rescue StandardError => e
        fail_closed!(e) if @lock
        raise Error, e.message
      end
    end

    # Capacity mutations must use the same provider client and frozen budget
    # whose guardian/heartbeat lifecycle is active. A check is not a reservation.
    def check_for!(budget:, client:)
      unless @client.equal?(client) && @budget.document == budget.document &&
             @budget.execution_profile_sha256 == budget.execution_profile_sha256
        raise Error, "fulfillment client/budget differs from active lifecycle"
      end
      check!
    end

    def finish!(reason:)
      stop_heartbeat
      return false if @suspended
      return false unless @started && @lock

      @operations.synchronize do
        @teardown_snapshot = @client.begin_budget_teardown(budget: @budget, reason: reason)
      end
      true
    rescue StandardError => e
      @failure ||= e
      false
    ensure
      release_binding!
    end

    # A graceful WLO pause leaves the immutable provider budget and capacity in
    # place only until the original heartbeat timeout/deadline. A later WLO may
    # reconnect to that same ledger; it cannot re-arm or extend it. If no owner
    # returns, the independent guardian remains the cleanup authority.
    def suspend!
      stop_heartbeat
      @suspended = true
      true
    ensure
      release_binding!
    end

    def suspended?
      @suspended
    end

    def last_error
      @failure
    end

    attr_reader :teardown_snapshot

    def teardown_status(timeout_seconds:)
      @budget.validate_snapshot!(@client.budget_status(budget: @budget, timeout_seconds: timeout_seconds))
    end

    private

    def acquire_binding!
      FileUtils.mkdir_p(File.dirname(@path))
      lock = File.open("#{@path}.lock", File::RDWR | File::CREAT, 0o600)
      unless lock.flock(File::LOCK_EX | File::LOCK_NB)
        lock.close
        raise Error, "budget binding is already owned by another WLO lifecycle"
      end
      @lock = lock
    end

    def bind_declaration!
      @new_binding = !File.exist?(@path)
      if @new_binding
        @record = { "contract_version" => RECORD_VERSION, "budget" => @budget.document, "deadline_at_utc" => nil,
                    "execution_profile_sha256" => @budget.execution_profile_sha256 }
        persist_binding!
      else
        @record = JSON.parse(File.read(@path))
        unless @record.is_a?(Hash) && @record["contract_version"] == RECORD_VERSION &&
               @record["budget"] == @budget.document &&
               @record["execution_profile_sha256"] == @budget.execution_profile_sha256
          raise Error, "persisted budget declaration differs; cannot reset limits or identity on resume"
        end
        @deadline = @record.fetch("deadline_at_utc")
      end
    rescue JSON::ParserError, KeyError, SystemCallError => e
      raise Error, "unreadable budget binding: #{e.message}"
    end

    def persist_binding!
      Tempfile.create(["wlo-budget-binding-", ".json"], File.dirname(@path)) do |file|
        file.write(JSON.pretty_generate(@record) + "\n")
        file.flush
        file.fsync
        File.rename(file.path, @path)
      end
    end

    def verify!(snapshot)
      @budget.validate_snapshot!(snapshot, ready: true, now: @clock.call)
      deadline = snapshot.fetch("deadline_at_utc")
      raise Error, "provider changed frozen budget deadline" if @deadline && @deadline != deadline

      @budget.validate_guardian!(@client.guardian_status(budget: @budget), now: @clock.call)
      unless @deadline
        @deadline = deadline.dup.freeze
        @record["deadline_at_utc"] = deadline
        persist_binding!
      end
      snapshot
    end

    def heartbeat_loop
      heartbeat_once! while wait_interval
    end

    def heartbeat_once!
      @operations.synchronize do
        return unless @mutex.synchronize { @running }
        # Evaluate BEFORE heartbeat: stale state must never be revived.
        verify!(@client.budget_status(budget: @budget))
        verify!(@client.heartbeat_budget(budget: @budget))
      rescue StandardError => e
        fail_closed!(e)
      end
    end

    def wait_interval
      @mutex.synchronize do
        return false unless @running
        @condition.wait(@mutex, @budget.document.fetch("guardian_poll_seconds"))
        @running
      end
    end

    def fail_closed!(error)
      @failure ||= error
      @mutex.synchronize do
        @running = false
        @condition.broadcast
      end
      # Even an arm timeout may have armed the remote ledger. Request teardown
      # without ever disabling/closing the independent guardian. If unreachable,
      # heartbeat cessation and the original deadline remain the fallback.
      @client.begin_budget_teardown(budget: @budget, reason: "wlo_budget_failure") if @provider_touched
    rescue StandardError
      nil
    end

    def stop_heartbeat
      @mutex.synchronize do
        @running = false
        @condition.broadcast
      end
      @thread&.join
    end

    def release_binding!
      @lock&.close
      @lock = nil
    end
  end
end
