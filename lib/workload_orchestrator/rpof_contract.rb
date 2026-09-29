# frozen_string_literal: true

require_relative "error"
require_relative "rpof_contract_values"

module WorkloadOrchestrator
  # WLO's public boundary. The provider's older wire names belong only in RpofClient.
  module RpofContract
    CAPABILITY_REQUEST = "wlo-rpof-capability-check-request/v0.1"
    CAPABILITY_RESULT = "wlo-rpof-capability-check-result/v0.1"
    DISPATCH_REQUEST = "wlo-rpof-dispatch-request/v0.1"
    DISPATCH_SUMMARY = "wlo-rpof-dispatch-summary/v0.1"
    ID = /\A[A-Za-z0-9][A-Za-z0-9._-]*\z/
    ENV_KEY = /\A[A-Za-z_][A-Za-z0-9_]*\z/
    DIGEST = /\A[0-9a-f]{64}\z/i

    extend RpofContractValues

    module_function

    def capability_request!(request)
      object!(request, %w[contract_version fleet_key worker_selector requirements])
      version!(request, CAPABILITY_REQUEST)
      text!(request["fleet_key"], "fleet_key", max: 64, pattern: ID)
      selector = request["worker_selector"]
      raise Error, "worker_selector must be an object" unless selector.is_a?(Hash)

      case selector["mode"]
      when "all"
        object!(selector, %w[mode])
      when "indices"
        object!(selector, %w[mode indices])
        indices!(selector["indices"])
      else
        raise Error, "worker_selector.mode must be all or indices"
      end
      requirements!(request["requirements"])
      request
    end

    def requirements!(requirements)
      object!(requirements, %w[models required_context_length require_fully_gpu_resident], %w[required_gpu_id])
      array!(requirements["models"], "models").each do |model|
        object!(model, %w[name expected_digest])
        text!(model["name"], "model name", max: 256)
        text!(model["expected_digest"], "expected_digest", max: 64, pattern: DIGEST)
      end
      value = requirements["required_context_length"]
      raise Error, "required_context_length must be a positive integer" unless value.is_a?(Integer) && value.positive?
      raise Error, "require_fully_gpu_resident must be true" unless requirements["require_fully_gpu_resident"] == true

      text!(requirements["required_gpu_id"], "required_gpu_id", max: 256) if requirements.key?("required_gpu_id")
    end

    # Explicit historical dispatch calls load only their compatibility validator.
    def dispatch_request!(request)
      legacy_dispatch_contract.dispatch_request!(request)
    end

    def job!(job)
      legacy_dispatch_contract.job!(job)
    end

    def dispatch_summary!(result, request, exit_status)
      legacy_dispatch_contract.dispatch_summary!(result, request, exit_status)
    end

    def legacy_dispatch_contract
      require_relative "legacy/rpof_dispatch_contract"
      Legacy::RpofDispatchContract
    end
    private_class_method :legacy_dispatch_contract

    def capability_result!(result, request, exit_status)
      boolean!(result["ready"], "ready")
      raise Error, "capability fleet_key mismatch" unless result["fleet_key"] == request["fleet_key"]
      raise Error, "capability diagnostics must be an array" unless result["diagnostics"].is_a?(Array)
      raise Error, "capability readiness disagrees with exit status" unless result["ready"] == exit_status.zero?

      return result unless result["ready"]

      text!(result["fleet_id"], "fleet_id", max: 256)
      indices!(result["selected_worker_indices"])
      selector = request["worker_selector"]
      if selector["mode"] == "indices" && result["selected_worker_indices"].sort != selector["indices"].sort
        raise Error, "capability worker selection mismatch"
      end
      raise Error, "ready capability result must include capabilities" unless result["capabilities"].is_a?(Hash)

      result
    end
  end
end
