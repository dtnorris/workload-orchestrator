# frozen_string_literal: true

require_relative "test_helper"

class WorkerAdmissionPolicyTest < Minitest::Test
  def test_expands_only_when_measured_queue_saves_meaningful_time_before_deadline
    policy = WorkloadOrchestrator::WorkerAdmissionPolicy.new
    inputs = { unclaimed: 10, workers: 1, ceiling: 3, job_seconds: 20.0,
               bootstrap_seconds: 12.0, deadline_seconds: 300.0 }
    assert_equal "useful_capacity", policy.evaluate(**inputs).fetch("reason")
    assert_equal "no_unclaimed_work", policy.evaluate(**inputs.merge(unclaimed: 0)).fetch("reason")
    assert_equal "worker_ceiling_reached", policy.evaluate(**inputs.merge(workers: 3)).fetch("reason")
    assert_equal "insufficient_measurements", policy.evaluate(**inputs.merge(job_seconds: nil)).fetch("reason")
    assert_equal "deadline_before_worker_ready", policy.evaluate(**inputs.merge(deadline_seconds: 5)).fetch("reason")
    assert_equal "negligible_deadline_benefit", policy.evaluate(**inputs.merge(unclaimed: 1)).fetch("reason")
  end
end
