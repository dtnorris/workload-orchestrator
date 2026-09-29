# frozen_string_literal: true

module WorkloadOrchestrator
  # A conservative, provider-neutral estimate made only after a completed job.
  # The provider remains responsible for live price checks and reservations.
  class WorkerAdmissionPolicy
    def evaluate(unclaimed:, workers:, ceiling:, job_seconds:, bootstrap_seconds:, deadline_seconds:)
      inputs = { "unclaimed" => unclaimed, "workers" => workers, "ceiling" => ceiling,
                 "job_seconds" => job_seconds, "bootstrap_seconds" => bootstrap_seconds,
                 "deadline_seconds" => deadline_seconds }
      reason = if unclaimed.zero?
                 "no_unclaimed_work"
               elsif workers >= ceiling
                 "worker_ceiling_reached"
               elsif !job_seconds || !job_seconds.positive? || !bootstrap_seconds || !bootstrap_seconds.positive?
                 "insufficient_measurements"
               elsif deadline_seconds <= bootstrap_seconds
                 "deadline_before_worker_ready"
               end
      return { "expand" => false, "reason" => reason, "inputs" => inputs } if reason

      without = (unclaimed.to_f / workers).ceil * job_seconds
      # Only the unclaimed queue can move to the new worker. Existing workers
      # may already have jobs, so this is an optimistic upper bound on benefit.
      with = [bootstrap_seconds + job_seconds, (unclaimed.to_f / (workers + 1)).ceil * job_seconds].max
      saved = [without - with, 0.0].max
      threshold = [5.0, without * 0.05].max
      useful = [saved, deadline_seconds].min >= threshold
      { "expand" => useful, "reason" => useful ? "useful_capacity" : "negligible_deadline_benefit",
        "inputs" => inputs, "estimated_seconds_saved" => saved, "minimum_seconds_saved" => threshold }
    end
  end
end
