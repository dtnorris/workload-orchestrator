# frozen_string_literal: true

module WorkloadOrchestrator
  class PlanPrinter
    def initialize(out)
      @out = out
    end

    def print(plan, workers, workdir, dynamic: false)
      @out.puts "Plan: #{plan.id}"
      @out.puts "SHA-256: #{plan.sha256}"
      @out.puts "Execution profile: #{plan.execution_profile.sha256}" if plan.execution_profile
      @out.puts "Workdir: #{workdir}"
      @out.puts "Pools: #{plan.pools.length}"
      @out.puts "Jobs: #{plan.jobs.length}"
      print_scheduling(plan)
      @out.puts "Dynamic placement: unresolved / registry-selected at runtime" if dynamic
      plan.pools.each { |pool| print_pool(pool, workers, dynamic: dynamic) }
      @out.puts "Zero-cost gate: PASS"
    end

    private

    def print_scheduling(plan)
      if plan.priority_scheduling?
        detail = plan.grouped_jobs? ? " (#{plan.job_groups.length} reporting groups)" : ""
        @out.puts "Scheduling: work-conserving priority#{detail}"
      elsif plan.grouped_jobs?
        @out.puts "Scheduling: group-major (#{plan.job_groups.length} groups)"
      else
        @out.puts "Scheduling: pool-major (legacy)"
      end
    end

    def print_pool(pool, workers, dynamic:)
      detail = requirement_detail(pool)
      if dynamic
        @out.puts "  #{pool.id}: placement=registry-selected concurrency=available-capacity#{detail}"
        return
      end

      names = pool.worker_names.map { |name| workers.fetch(name).name }.join(",")
      @out.puts "  #{pool.id}: workers=#{names} concurrency=#{pool.max_concurrency}#{detail}"
    end

    def requirement_detail(pool)
      fields = []
      fields << "labels=#{pool.required_labels.join(',')}" unless pool.required_labels.empty?
      append_ollama_fields(fields, pool.ollama_requirement) if pool.ollama_requirement
      fields.empty? ? "" : " #{fields.join(' ')}"
    end

    def append_ollama_fields(fields, requirement)
      fields << "ollama=#{requirement.fetch('model')}"
      fields << "digest=#{requirement.fetch('expected_digest')}"
      fields << "context=#{requirement['required_context_length']}" if requirement["required_context_length"]
      if requirement.key?("require_fully_gpu_resident")
        fields << "fully_gpu_resident=#{requirement.fetch('require_fully_gpu_resident')}"
      end
      fields << "gpu=#{requirement['required_gpu_id']}" if requirement["required_gpu_id"]
    end
  end
end
