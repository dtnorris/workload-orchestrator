# frozen_string_literal: true

require_relative "error"

module WorkloadOrchestrator
  module RpofDispatchValidation
    def dispatch_summary!(result, request, exit_status)
      target = request["target"]
      unless result["fleet_key"] == target["fleet_key"] && result["fleet_id"] == target["expected_fleet_id"]
        raise Error, "dispatch fleet identity mismatch"
      end

      dispatch_workers!(result, target)
      dispatch_status!(result, exit_status)
      dispatch_counts!(result, request)
      validate_job_evidence!(result, request)
      if result["status"] == "completed" && result["completed_count"] != result["job_count"]
        raise Error, "completed dispatch has unfinished jobs"
      end

      result
    end

    def dispatch_status!(result, exit_status)
      states = %w[completed workload_failed infrastructure_failed integrity_failed drained interrupted]
      raise Error, "unsupported dispatch status" unless states.include?(result["status"])
      return if (result["status"] == "completed") == exit_status.zero?

      raise Error, "dispatch status disagrees with exit status"
    end

    def dispatch_counts!(result, request)
      counts = %w[job_count completed_count failed_count not_started_count].map do |key|
        value = result[key]
        raise Error, "#{key} must be a nonnegative integer" unless value.is_a?(Integer) && value >= 0

        value
      end
      return if counts.first == request["jobs"].length && counts.first == counts.drop(1).sum

      raise Error, "dispatch job counts mismatch"
    end

    def dispatch_workers!(result, target)
      # RPOF's early infrastructure-failure summary may omit worker_indices.
      if result.key?("worker_indices")
        indices!(result["worker_indices"])
        unless result["worker_indices"].sort == target["worker_indices"].sort
          raise Error, "dispatch worker selection mismatch"
        end
      elsif result["status"] != "infrastructure_failed"
        raise Error, "dispatch summary is missing worker_indices"
      end
    end

    def validate_job_evidence!(result, request)
      jobs = result["jobs"]
      pending = result["not_started_job_ids"]
      unless jobs.is_a?(Array) && jobs.all?(Hash) && pending.is_a?(Array)
        raise Error, "dispatch job evidence must be arrays"
      end

      job_identities!(jobs, pending, request)
      job_outcomes!(jobs, pending, result)
    end

    def job_identities!(jobs, pending, request)
      ids = jobs.map { |job| job["job_id"] } + pending
      expected = request["jobs"].map { |job| job["job_id"] }
      return if ids.all?(String) && ids.uniq.length == ids.length && ids.sort == expected.sort

      raise Error, "dispatch job identities mismatch"
    end

    def job_outcomes!(jobs, pending, result)
      unless pending.length == result["not_started_count"] &&
             jobs.count { |job| job["status"] == "completed" } == result["completed_count"] &&
             jobs.count { |job| job["status"] == "failed" } == result["failed_count"]
        raise Error, "dispatch job outcomes disagree with counts"
      end
    end
  end
end
