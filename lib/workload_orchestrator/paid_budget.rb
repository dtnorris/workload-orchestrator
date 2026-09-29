# frozen_string_literal: true

require "digest"
require "json"
require "time"
require_relative "error"

module WorkloadOrchestrator
  # Provider-neutral declaration. No defaults: every financial limit is explicit.
  class PaidBudget
    VERSION = "wlo-paid-budget/v0.1"
    WIRE_VERSION = "afio-production-burst-budget/v0.1"
    STATE_VERSION = "rpof-production-burst-budget-state/v0.1"
    LIMITS = %w[max_cumulative_compute_usd max_runtime_seconds guardian_poll_seconds
                orchestrator_heartbeat_timeout_seconds teardown_reserve_seconds].freeze
    COSTS = %w[expected_compute_usd max_hourly_rate_usd].freeze
    KEYS = (["contract_version", "budget_id", "plan_sha256"] + LIMITS + COSTS).freeze

    attr_reader :document, :execution_profile_sha256

    def initialize(document, plan_bytes:, execution_profile: nil)
      unless document.is_a?(Hash) && document.keys.all? { |key| key.is_a?(String) } && document.keys.sort == KEYS.sort
        raise Error, "paid budget must contain exactly: #{KEYS.join(', ')}"
      end
      @document = JSON.parse(JSON.generate(document))
      raise Error, "unsupported paid budget version" unless @document["contract_version"] == VERSION
      text!(@document["budget_id"], "budget_id")
      unless @document["plan_sha256"] == Digest::SHA256.hexdigest(plan_bytes)
        raise Error, "paid budget plan_sha256 must match exact plan bytes"
      end
      (LIMITS + COSTS).each { |key| @document[key] = number!(@document[key], key) }
      validate_limits!
      validate_profile!(execution_profile) if execution_profile
      @document.each_value { |value| value.freeze }
      @document.freeze
      freeze
    rescue JSON::GeneratorError, JSON::ParserError, TypeError => e
      raise Error, "invalid paid budget: #{e.message}"
    end

    def identity
      @document.slice("budget_id", "plan_sha256")
    end

    # Compatibility is isolated here; RPOF's existing guardian contract is unchanged.
    def provider_request
      @document.slice("budget_id", "plan_sha256", *LIMITS).merge("contract_version" => WIRE_VERSION)
    end

    def crash_horizon_seconds
      @document.fetch("guardian_poll_seconds") +
        @document.fetch("orchestrator_heartbeat_timeout_seconds") + @document.fetch("teardown_reserve_seconds")
    end

    def bounds
      rate = @document.fetch("max_hourly_rate_usd")
      {
        "expected_compute_usd" => @document.fetch("expected_compute_usd"),
        "max_hourly_rate_usd" => rate,
        "max_cumulative_compute_usd" => @document.fetch("max_cumulative_compute_usd"),
        "max_runtime_seconds" => @document.fetch("max_runtime_seconds"),
        "max_time_to_absence_seconds" => @document.fetch("max_runtime_seconds") +
          @document.fetch("guardian_poll_seconds") + @document.fetch("teardown_reserve_seconds"),
        "crash_additional_compute_usd" => rate * crash_horizon_seconds / 3600.0
      }
    end

    def validate_snapshot!(value, ready: false, now: Time.now.utc)
      unless value.is_a?(Hash) && value["contract_version"] == STATE_VERSION &&
             value.slice("budget_id", "plan_sha256") == identity && value["limits"] == provider_request
        raise Error, "provider budget version, identity, or immutable limits mismatch"
      end
      unless %w[ARMED TEARDOWN_REQUIRED CLOSED].include?(value["state"])
        raise Error, "invalid provider budget state"
      end
      armed = timestamp!(value["armed_at_utc"])
      deadline = timestamp!(value["deadline_at_utc"])
      unless deadline > armed && deadline - armed <= @document.fetch("max_runtime_seconds") + 0.001
        raise Error, "provider budget extended the runtime lease"
      end
      %w[accrued_compute_usd committed_rate_usd_per_hour committed_maximum_liability_usd
         remaining_uncommitted_budget_usd].each { |key| number!(value[key], key, zero: true) }
      validate_ready!(value, now, armed, deadline) if ready
      value
    end

    def validate_guardian!(value, now: Time.now.utc)
      unless value.is_a?(Hash) && value["enabled"] == true && value["launchd_loaded"] == true &&
             value["ready"] == true && value["state"] == "ARMED" && value["last_error"].nil? &&
             value["pid"].is_a?(Integer) && value["pid"].positive? && value["pid"] != Process.pid
        raise Error, "independent guardian is not ready"
      end
      fresh!(value["ledger_heartbeat_at_utc"], 2 * @document.fetch("guardian_poll_seconds"), now)
      timestamp!(value["provider_probe_at_utc"])
      value
    end

    private

    def validate_profile!(profile)
      expected = {
        "max_hourly_rate_usd" => @document.fetch("max_hourly_rate_usd"),
        "max_total_cost_usd" => @document.fetch("max_cumulative_compute_usd"),
        "max_runtime_seconds" => @document.fetch("max_runtime_seconds")
      }
      unless profile.rpof? && profile.document["budget"] == expected
        raise Error, "execution profile and paid budget ceilings must agree exactly"
      end
      @execution_profile_sha256 = profile.sha256
    end

    def validate_limits!
      if @document.fetch("orchestrator_heartbeat_timeout_seconds") < 2 * @document.fetch("guardian_poll_seconds")
        raise Error, "heartbeat timeout must be at least twice guardian polling interval"
      end
      cap = @document.fetch("max_cumulative_compute_usd")
      unless bounds.values.all?(&:finite?) && crash_horizon_seconds.finite?
        raise Error, "derived paid budget bounds must be finite"
      end
      if @document.fetch("expected_compute_usd") + bounds.fetch("crash_additional_compute_usd") > cap
        raise Error, "expected compute plus crash/teardown reserve exceeds cumulative cap"
      end
      if @document.fetch("max_runtime_seconds") <= crash_horizon_seconds
        raise Error, "runtime lease must exceed the crash/teardown horizon"
      end
    end

    def validate_ready!(value, now, armed, deadline)
      unless value["state"] == "ARMED" && value["mutation_allowed"] == true && armed <= now && now < deadline
        raise Error, "provider budget is not mutation-ready"
      end
      fresh!(value["last_guardian_heartbeat_at_utc"], 2 * @document.fetch("guardian_poll_seconds"), now)
      fresh!(value["last_orchestrator_heartbeat_at_utc"],
             @document.fetch("orchestrator_heartbeat_timeout_seconds"), now)
      if value.fetch("committed_rate_usd_per_hour") > @document.fetch("max_hourly_rate_usd") ||
         value.fetch("committed_maximum_liability_usd") >= @document.fetch("max_cumulative_compute_usd")
        raise Error, "provider budget rate or liability exceeds WLO allowance"
      end
    end

    def fresh!(value, limit, now)
      age = now - timestamp!(value)
      raise Error, "budget/guardian heartbeat is stale or in the future" unless age >= 0 && age <= limit
    end

    def timestamp!(value)
      Time.iso8601(value)
    rescue ArgumentError, TypeError
      raise Error, "invalid provider budget timestamp"
    end

    def number!(value, label, zero: false)
      unless value.is_a?(Numeric) && value.finite? && (zero ? value >= 0 : value > 0)
        raise Error, "#{label} must be a #{zero ? 'nonnegative' : 'positive'} finite number"
      end
      value.to_f
    end

    def text!(value, label)
      unless value.is_a?(String) && !value.strip.empty? && !value.include?("\0")
        raise Error, "#{label} must be nonempty text"
      end
    end
  end
end
