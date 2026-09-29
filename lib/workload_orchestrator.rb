# frozen_string_literal: true

require_relative "workload_orchestrator/version"
require_relative "workload_orchestrator/error"
require_relative "workload_orchestrator/config"
require_relative "workload_orchestrator/plan"
require_relative "workload_orchestrator/execution_profile"
require_relative "workload_orchestrator/worker"
require_relative "workload_orchestrator/worker_set"
require_relative "workload_orchestrator/worker_source"
require_relative "workload_orchestrator/registry_worker"
require_relative "workload_orchestrator/dynamic_worker_binding"
require_relative "workload_orchestrator/capability_matcher"
require_relative "workload_orchestrator/dynamic_scheduler"
require_relative "workload_orchestrator/dynamic_worker_registry"
require_relative "workload_orchestrator/worker_registry_poller"
require_relative "workload_orchestrator/dynamic_worker_loss_reconciler"
require_relative "workload_orchestrator/worker_check"
require_relative "workload_orchestrator/job_claim"
require_relative "workload_orchestrator/execution_store"
require_relative "workload_orchestrator/execution_report"
require_relative "workload_orchestrator/live_execution_report"
require_relative "workload_orchestrator/live_execution_display"
require_relative "workload_orchestrator/runner"
require_relative "workload_orchestrator/cli"

module WorkloadOrchestrator
  LEGACY_RPOF_COMPONENTS = {
    RpofContract: "rpof_contract",
    RpofReadiness: "rpof_readiness",
    RpofClient: "rpof_client",
    RpofBudgetClient: "rpof_budget_client",
    RpofCapacityClient: "rpof_capacity_client",
    PaidBudget: "paid_budget",
    PaidBudgetLifecycle: "paid_budget_lifecycle",
    ExecutionPoolPlan: "execution_pool_plan",
    PoolFulfillment: "pool_fulfillment",
    WorkerAdmissionPolicy: "worker_admission_policy"
  }.freeze

  LEGACY_RPOF_COMPONENTS.each do |constant, file|
    autoload constant, File.expand_path("workload_orchestrator/#{file}", __dir__)
  end
end
require_relative "workload_orchestrator/detached_manager"
require_relative "workload_orchestrator/execution_report"
require_relative "workload_orchestrator/execution_watch"
