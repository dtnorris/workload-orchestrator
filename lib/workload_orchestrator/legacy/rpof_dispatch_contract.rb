# frozen_string_literal: true

require_relative "../rpof_contract"
require_relative "../rpof_dispatch_validation"

module WorkloadOrchestrator
  module Legacy
    # Frozen historical request/result validation; not part of dynamic execution.
    module RpofDispatchContract
      DISPATCH_REQUEST = RpofContract::DISPATCH_REQUEST
      ID = RpofContract::ID
      ENV_KEY = RpofContract::ENV_KEY

      extend RpofContractValues
      extend RpofDispatchValidation
      module_function

      def dispatch_request!(request)
        object!(request, %w[contract_version target group_by_affinity jobs])
        version!(request, DISPATCH_REQUEST)
        target = request["target"]
        object!(target, %w[fleet_key expected_fleet_id worker_indices])
        text!(target["fleet_key"], "fleet_key", max: 64, pattern: ID)
        text!(target["expected_fleet_id"], "expected_fleet_id", max: 256)
        indices!(target["worker_indices"])
        boolean!(request["group_by_affinity"], "group_by_affinity")
        jobs = array!(request["jobs"], "jobs")
        jobs.each { |job| job!(job) }
        ids = jobs.map { |job| job["job_id"] }
        raise Error, "duplicate job_id" unless ids.uniq.length == ids.length

        request
      end

      def job!(job)
        object!(job, %w[job_id argv], %w[env affinity])
        text!(job["job_id"], "job_id", max: 128, pattern: ID)
        array!(job["argv"], "argv").each do |value|
          raise Error, "argv must contain strings without NUL bytes" unless value.is_a?(String) && !value.include?("\0")
        end
        text!(job["argv"].first, "argv executable")
        if job.key?("env")
          env = job["env"]
          valid = env.is_a?(Hash) && env.all? do |key, value|
            key.is_a?(String) && key.match?(ENV_KEY) && value.is_a?(String) && !value.include?("\0")
          end
          raise Error, "env must map names to NUL-free strings; RPOF does not support null/unset" unless valid
        end
        text!(job["affinity"], "affinity", max: 256) if job.key?("affinity")
      end

    end
  end
end
