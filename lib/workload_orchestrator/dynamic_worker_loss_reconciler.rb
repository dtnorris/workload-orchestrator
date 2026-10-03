# frozen_string_literal: true

module WorkloadOrchestrator
  # Compares durable running-attempt bindings with DW-10's last accepted
  # registry checkpoint. It persists in-doubt state before returning a
  # transition that the dynamic scheduler uses to release occupancy.
  class DynamicWorkerLossReconciler
    attr_reader :store

    def initialize(store:)
      @store = store
    end

    def reconcile!(poller)
      checkpoints = accepted_checkpoints(poller)
      return [].freeze if checkpoints.empty?

      store.dynamic_running_attempts.filter_map do |attempt|
        registry_id = attempt.worker_binding.execution_identity.fetch("registry_id")
        checkpoint = checkpoints[registry_id]
        unless checkpoint
          raise Error, "no accepted registry checkpoint for running attempt namespace #{registry_id.inspect}"
        end

        workers = checkpoint.fetch("workers")
        workers_by_id = workers.to_h { |worker| [worker.fetch("worker_id"), worker] }
        reconcile_attempt(attempt, checkpoint, workers, workers_by_id)
      end.freeze
    end

    private

    def accepted_checkpoints(poller)
      return poller.accepted_checkpoints if poller.respond_to?(:accepted_checkpoints)

      unless poller.is_a?(WorkerRegistryPoller)
        raise Error, "worker-loss reconciliation requires accepted registry pollers"
      end

      checkpoint = poller.accepted_checkpoint
      checkpoint ? { checkpoint.fetch("registry_id") => checkpoint }.freeze : {}.freeze
    end

    def reconcile_attempt(attempt, checkpoint, workers, workers_by_id)
      binding = attempt.worker_binding
      identity = binding.execution_identity
      current = workers_by_id[identity.fetch("worker_id")]
      event = historical_loss_event(binding, checkpoint)
      return if !event && current && current.fetch("execution_identity") == binding.tuple

      reason = event ? event_reason(event) : loss_reason(identity, current)
      replacement = replacement_identity(binding, checkpoint, event, current, workers)
      evidence = loss_evidence(attempt, checkpoint, reason, replacement, event)
      result = store.record_dynamic_worker_loss!(attempt: attempt, evidence: evidence)
      {
        "job_id" => attempt.job.id,
        "attempt_id" => attempt.attempt_id,
        "reason" => reason,
        "result" => result.to_s
      }.freeze
    end

    def loss_reason(identity, current)
      return "worker_disappeared" unless current
      return "worker_generation_replaced" unless current.fetch("generation_id") == identity.fetch("generation_id")
      return "worker_endpoint_changed" unless current.fetch("endpoint") == identity.fetch("endpoint")
      return "worker_capability_changed" unless
        current.fetch("capability_fingerprint") == identity.fetch("capability_fingerprint")

      "worker_disappeared"
    end

    def endpoint_replacement(identity, workers)
      workers.select { |worker| worker.fetch("endpoint") == identity.fetch("endpoint") }
             .min_by { |worker| worker.fetch("execution_identity") }
    end

    def replacement_identity(binding, checkpoint, event, current, workers)
      if event&.fetch("event", nil) == "changed"
        return identity_from_row(checkpoint, event.fetch("details").fetch("current"))
      end

      unless current&.fetch("execution_identity") == binding.tuple
        row = current || endpoint_replacement(binding.execution_identity, workers)
        return identity_from_row(checkpoint, row) if row
      end
      nil
    end

    def loss_evidence(attempt, checkpoint, reason, replacement, event)
      evidence = {
        "kind" => "dynamic_worker_loss_in_doubt",
        "job_id" => attempt.job.id,
        "attempt_id" => attempt.attempt_id,
        "reason" => reason,
        "outcome_known" => false,
        "worker_execution_identity" => attempt.worker_binding.execution_identity,
        "worker_registry_binding" => attempt.worker_binding.registry_binding,
        "last_accepted_registry" => registry_evidence(checkpoint),
        "reconciliation_event" => event || checkpoint_absence_event(checkpoint),
        "observed_at" => checkpoint.fetch("accepted_at")
      }
      evidence["observed_replacement_identity"] = replacement if replacement
      evidence.freeze
    end

    def registry_evidence(checkpoint)
      checkpoint.slice("registry_id", "revision", "snapshot_sha256").freeze
    end

    def identity_from_row(checkpoint, row)
      values = row.fetch("execution_identity")
      DynamicWorkerBinding::IDENTITY_KEYS.zip(values).to_h.freeze.tap do |identity|
        unless identity.fetch("registry_id") == checkpoint.fetch("registry_id")
          raise Error, "worker registry checkpoint contains a foreign execution identity"
        end
      end
    end

    def historical_loss_event(binding, checkpoint)
      checkpoint.fetch("reconciliation_history").reverse_each do |entry|
        next unless entry.fetch("revision") > binding.registry_binding.fetch("registry_revision")

        event = event_from_changes(binding, entry.fetch("changes"))
        next unless event

        return {
          "source" => "reconciliation_history",
          "event" => event.fetch("event"),
          "revision" => entry.fetch("revision"),
          "snapshot_sha256" => entry.fetch("snapshot_sha256"),
          "observed_at" => entry.fetch("observed_at"),
          "details" => event.fetch("details")
        }.freeze
      end
      nil
    end

    def checkpoint_absence_event(checkpoint)
      {
        "source" => "accepted_checkpoint",
        "event" => "bound_identity_absent",
        "revision" => checkpoint.fetch("revision"),
        "snapshot_sha256" => checkpoint.fetch("snapshot_sha256"),
        "observed_at" => checkpoint.fetch("accepted_at")
      }.freeze
    end

    def event_reason(event)
      return "worker_disappeared" if event.fetch("event") == "disappeared"

      kinds = event.fetch("details").fetch("kinds")
      return "worker_generation_replaced" if kinds.include?("generation")
      return "worker_endpoint_changed" if kinds.include?("endpoint")

      "worker_capability_changed"
    end

    def event_from_changes(binding, changes)
      disappeared = changes.fetch("disappeared").find do |row|
        row.fetch("execution_identity") == binding.tuple
      end
      return { "event" => "disappeared", "details" => disappeared } if disappeared

      changed = changes.fetch("changed").find do |row|
        identity_changed = row.fetch("kinds").intersect?(%w[generation endpoint capability])
        identity_changed && row.fetch("previous").fetch("execution_identity") == binding.tuple
      end
      return { "event" => "changed", "details" => changed } if changed

      nil
    end
  end
end
