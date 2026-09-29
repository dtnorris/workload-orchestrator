# frozen_string_literal: true

require_relative "rpof_client"
require_relative "paid_budget"

module WorkloadOrchestrator
  # Only budget control. No create/scale/fulfill/close or guardian-disable surface.
  class RpofBudgetClient < RpofClient
    def initialize(executable:, timeout_seconds: 30)
      super(executable: executable)
      unless timeout_seconds.is_a?(Numeric) && timeout_seconds.finite? && timeout_seconds.positive?
        raise Error, "budget command timeout must be positive and finite"
      end
      @timeout_seconds = timeout_seconds
    end

    def arm_budget(budget:)
      Tempfile.create(["wlo-budget-", ".json"]) do |file|
        file.write(JSON.generate(budget.provider_request))
        file.flush
        snapshot = budget_command("arm", "--request", file.path)
        budget.validate_snapshot!(snapshot)
      end
    end

    def heartbeat_budget(budget:)
      snapshot = budget_command("heartbeat", *identity_arguments(budget), "--source", "orchestrator")
      budget.validate_snapshot!(snapshot)
    end

    def budget_status(budget:)
      # Evaluate persists expired/failed state; a status-only read does not.
      snapshot = budget_command("evaluate", *identity_arguments(budget))
      budget.validate_snapshot!(snapshot)
    end

    def guardian_status(budget:)
      budget_command("guardian-status", *identity_arguments(budget))
    end

    def begin_budget_teardown(budget:, reason:)
      unless reason.is_a?(String) && !reason.strip.empty? && !reason.include?("\0")
        raise Error, "teardown reason must be nonempty text"
      end
      snapshot = budget_command("begin-teardown", *identity_arguments(budget), "--reason", reason)
      budget.validate_snapshot!(snapshot)
      unless %w[TEARDOWN_REQUIRED CLOSED].include?(snapshot["state"])
        raise Error, "provider did not begin budget teardown"
      end
      snapshot
    end

    private

    def identity_arguments(budget)
      ["--budget-id", budget.identity.fetch("budget_id"), "--plan-sha256", budget.identity.fetch("plan_sha256")]
    end

    def budget_command(*arguments)
      stdout, stderr, status = capture_budget(arguments)
      raise Error, "RPOF budget command failed: #{stderr.strip}" unless status.success?

      document = JSON.parse(stdout)
      raise Error, "RPOF budget result must be an object" unless document.is_a?(Hash)

      document
    rescue JSON::ParserError, SystemCallError => e
      raise Error, "RPOF budget command failed: #{e.message}"
    end

    def capture_budget(arguments)
      Open3.popen3([@executable, @executable], "budget", *arguments, pgroup: true) do |input, output, error, waiter|
        input.close
        readers = [output, error].map { |io| Thread.new { io.read } }
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout_seconds
        begin
          unless waiter.join(@timeout_seconds)
            raise Error, "RPOF budget command timed out; outcome unknown, independent guardian remains enabled"
          end
          readers.each do |reader|
            remaining = [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
            raise Error, "RPOF budget output timed out" unless reader.join(remaining)
          end
          [readers[0].value, readers[1].value, waiter.value]
        ensure
          # Kill only this command's process group, never the calling terminal or guardian.
          if !waiter.join(0) || readers.any?(&:alive?)
            signal_group("TERM", waiter.pid)
            waiter.join(1)
            signal_group("KILL", waiter.pid)
          end
          readers.each { |reader| reader.kill if reader.alive? }
          readers.each(&:join)
        end
      end
    end

    def signal_group(signal, pid)
      Process.kill(signal, -pid)
    rescue Errno::ESRCH
      nil
    end
  end
end
