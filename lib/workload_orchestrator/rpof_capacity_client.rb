# frozen_string_literal: true

require_relative "rpof_budget_client"
require_relative "execution_pool_plan"

module WorkloadOrchestrator
  class RpofCapacityClient < RpofBudgetClient
    WIRE_REQUEST = "afio-rpof-execution-pool-fulfill-request/v0.2"
    WIRE_RESULT = "afio-rpof-execution-pool-fulfill-result/v0.1"
    RESULT_VERSION = "wlo-rpof-execution-pool-fulfill-result/v0.1"

    def plan_pool(pool_plan:, pool_id:, output_dir:)
      invoke_pool(pool_plan, pool_id, output_dir, dry_run: true, timeout: @timeout_seconds)
    end

    def fulfill_pool(pool_plan:, pool_id:, output_dir:, lifecycle:, authorize_paid: false)
      raise Error, "paid fulfillment requires explicit authorize_paid: true" unless authorize_paid == true
      snapshot = lifecycle.check_for!(budget: pool_plan.budget, client: self)
      remaining = Time.iso8601(snapshot.fetch("deadline_at_utc")) - Time.now.utc
      result = invoke_pool(pool_plan, pool_id, output_dir, dry_run: false, timeout: remaining)
      lifecycle.check_for!(budget: pool_plan.budget, client: self)
      result
    end

    private

    def invoke_pool(pool_plan, pool_id, output_dir, dry_run:, timeout:)
      request = pool_plan.request(pool_id)
      output = File.expand_path(output_dir)
      FileUtils.mkdir_p(File.dirname(output))
      Dir.mkdir(output)
      wire = request.merge("contract_version" => WIRE_REQUEST, "budget" => pool_plan.budget.provider_request)
      File.write(File.join(output, "request.json"), JSON.pretty_generate(request) + "\n")
      wire_path = File.join(output, "wire-request.json")
      File.write(wire_path, JSON.pretty_generate(wire) + "\n")
      result_path = File.join(output, "result.json")
      stdout, stderr, status = capture_process(
        ["execution-pool-fulfill", "--request", wire_path, "--output", result_path, dry_run ? "--dry-run" : "--yes"],
        timeout_seconds: timeout
      )
      File.write(File.join(output, "stdout.log"), stdout)
      File.write(File.join(output, "stderr.log"), stderr)
      unless status.exited? && [0, 1].include?(status.exitstatus)
        raise Error, "RPOF fulfillment ended unexpectedly; retained evidence: #{output}"
      end
      document = read_result(result_path, WIRE_RESULT)
      validate_pool_result!(document, request, pool_plan.execution_handle(request), dry_run, status.exitstatus)
      Result.new(document: document.merge("contract_version" => RESULT_VERSION),
                 exit_status: status.exitstatus, stdout: stdout, stderr: stderr).freeze
    rescue Errno::EEXIST
      raise Error, "fulfillment output already exists; prior evidence cannot be reused or overwritten"
    rescue SystemCallError => e
      raise Error, "RPOF fulfillment failed: #{e.message}"
    end

    def validate_pool_result!(result, request, handle, dry_run, exit_status)
      %w[plan_sha256 pool_id requirements].each do |key|
        raise Error, "RPOF fulfillment #{key} mismatch" unless result[key] == request[key]
      end
      raise Error, "RPOF execution handle mismatch" unless result["execution_handle"] == handle
      RpofContract.boolean!(result["ready"], "ready")
      expected_statuses = dry_run ? %w[planned unavailable] : %w[ready partial_ready unfulfilled failed]
      unless expected_statuses.include?(result["status"])
        raise Error, "unexpected RPOF fulfillment status #{result['status'].inspect}"
      end
      ready = %w[ready partial_ready].include?(result["status"])
      unless result["ready"] == ready && exit_status == (ready || result["status"] == "planned" ? 0 : 1)
        raise Error, "fulfillment status, readiness, and exit disagree"
      end
      capacity = result["capacity"]
      unless capacity.is_a?(Hash) && request.fetch("capacity").all? { |key, value| capacity[key] == value }
        raise Error, "RPOF changed requested capacity or cost ceilings"
      end
      %w[initial_workers final_workers].each do |key|
        value = capacity[key]
        raise Error, "invalid fulfillment worker count" unless value.is_a?(Integer) && value >= 0
      end
      final = capacity.fetch("final_workers")
      minimum = capacity.fetch("minimum_workers")
      desired = capacity.fetch("desired_workers")
      raise Error, "provider exceeded desired capacity" if final > desired
      if ready && (final < minimum || (result["status"] == "ready") != (final == desired))
        raise Error, "ready fulfillment does not meet requested minimum/desired capacity"
      end
      indices = ready ? (1..final).to_a : []
      raise Error, "RPOF fulfillment worker selection mismatch" unless result["worker_indices"] == indices
      if ready && !result["capabilities"].is_a?(Hash)
        raise Error, "ready fulfillment lacks capability evidence"
      end
      if dry_run && final != capacity.fetch("initial_workers")
        raise Error, "dry-run fulfillment unexpectedly changed capacity"
      end
    end

    # Capability calls in this client also have a finite deadline. Existing
    # RpofClient transport semantics and normalized capability results are reused.
    def invoke(request, wire_version, operation, arguments)
      Tempfile.create(["wlo-capacity-check-", ".json"]) do |file|
        file.write(JSON.generate(request.merge("contract_version" => wire_version)))
        file.flush
        stdout, stderr, status = capture_process(
          [operation, "--request", file.path, *arguments], timeout_seconds: @timeout_seconds
        )
        unless status.exited? && [0, 1].include?(status.exitstatus)
          raise Error, "RPOF #{operation} ended unexpectedly"
        end
        Result.new(exit_status: status.exitstatus, stdout: stdout, stderr: stderr)
      end
    end
  end
end
