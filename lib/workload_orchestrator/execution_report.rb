# frozen_string_literal: true

module WorkloadOrchestrator
  class ExecutionReport
    def initialize(plan:, output:, clock: -> { Time.now.utc })
      @plan = plan
      @root = File.expand_path(output)
      @clock = clock
    end

    def document
      state = read_json("execution.json")
      unless state["plan_id"] == @plan.id && state["plan_sha256"] == @plan.sha256
        raise Error, "output execution identity does not match plan"
      end

      jobs = read_json("jobs.json").fetch("jobs")
      counts = job_counts(jobs)
      terminal = counts.fetch("complete") + counts.fetch("failed") + counts.fetch("interrupted", 0)
      report = report_document(state, jobs, counts, terminal)
      apply_owner_crash!(report)
      report["pool_status"] = ExecutionPoolStatus.new(
        plan: @plan, output: @root, clock: @clock
      ).document(report)
      report
    rescue SystemCallError, JSON::ParserError, KeyError => e
      raise Error, "cannot read execution status: #{e.message}"
    end

    def print(out, verbose: false, width: ExecutionDashboard::DEFAULT_WIDTH)
      state = document
      return print_dashboard(out, state, width) unless verbose || state.fetch("pool_status").empty?

      print_detailed(out, state)
    end

    private

    def print_detailed(out, state)
      out.puts "Plan: #{state.fetch('plan_id')}"
      out.puts "Execution: #{state.fetch('status')} | executor #{state['executor_active'] ? 'active' : 'inactive'}"
      out.puts "Progress: [#{state['terminal']}/#{state['total']}] terminal (#{state['progress_percent']}%)"
      out.puts "Jobs: #{state.fetch('counts').map { |key, value| "#{key}=#{value}" }.join(' ')}"
      out.puts "Resources: #{state.dig('resource_disposition', 'phase')}" if state["resource_disposition"]
      print_controls(out, state)
      print_pools(out, state.fetch("pool_status"))
      print_timing(out, state)
      print_jobs(out, state.fetch("jobs"), state["executor_active"])
      print_manager(out, state["manager"])
      out.puts "Evidence: #{@root}"
    end

    def print_dashboard(out, state, width)
      out.write(ExecutionDashboard.new(width:).render(state))
    end

    def report_document(state, jobs, counts, terminal)
      state.merge(
        "paused" => File.file?(File.join(@root, "control", "pause")), "jobs" => jobs,
        "counts" => counts, "total" => jobs.length, "terminal" => terminal,
        "progress_percent" => jobs.empty? ? 100.0 : (100.0 * terminal / jobs.length).round(1),
        "executor_active" => executor_active?, "manager" => manager_record
      )
    end

    def apply_owner_crash!(report)
      active_without_owner = %w[running cleanup_pending].include?(report["status"]) && !report["executor_active"]
      return unless active_without_owner && rpof_execution?

      report["status"] = "owner_crashed"
      report["resource_disposition"] ||= {
        "phase" => "guardian_pending",
        "detail" => "owner vanished; inspect the original RPOF budget"
      }
    end

    def rpof_execution?
      path = File.join(@root, "execution-profile.json")
      return false unless File.file?(path)

      JSON.parse(File.read(path)).fetch("pools").any? { |pool| pool["backend"] == "rpof" }
    end

    def job_counts(jobs)
      observed = jobs.map { |job| job.fetch("status") }.tally
      statuses = %w[complete failed running pending]
      statuses << "interrupted" if observed.fetch("interrupted", 0).positive?
      statuses.to_h { |status| [status, observed.fetch(status, 0)] }
    end

    def print_controls(out, state)
      out.puts "Pause: requested (draining active jobs)" if state["paused"] && state["executor_active"]
      out.puts "Pause: requested" if state["paused"] && !state["executor_active"]
      out.puts "Breaker: #{state.dig('circuit_breaker', 'reason')}" if state.dig("circuit_breaker", "tripped")
    end

    def print_pools(out, pools)
      return if pools.empty?

      out.puts "Pools:"
      out.puts "  Pool             Jobs C/R/F/I/P  Ready  Busy  Idle  State"
      pools.each do |pool|
        jobs = pool.fetch("jobs")
        workers = pool.fetch("workers")
        counts = %w[complete running failed interrupted pending].map { |key| jobs.fetch(key) }.join("/")
        out.puts format(
          "  %<pool>-16s %<counts>-13s %<ready>5d %<busy>5d %<idle>5d  %<state>s",
          pool: pool.fetch("pool_id"), counts:, ready: workers.fetch("compatible_ready"),
          busy: workers.fetch("busy"), idle: workers.fetch("idle"), state: human_pool_state(pool)
        )
      end
    end

    def human_pool_state(pool)
      reason = pool.fetch("reason")
      return reason if %w[RUNNING READY_TO_DISPATCH COMPLETE FAILED INTERRUPTED].include?(reason)

      "#{pool.fetch('state')}: #{reason.downcase.tr('_', ' ')}"
    end

    def read_json(name)
      JSON.parse(File.read(File.join(@root, name)))
    end

    def executor_active?
      path = File.join(@root, ".execution.lock")
      return false unless File.file?(path)

      File.open(path, File::RDONLY) { |file| !file.flock(File::LOCK_EX | File::LOCK_NB) }
    end

    def manager_record
      return nil unless File.file?(File.join(@root, "manager.json"))

      pointer = read_json("manager.json")
      # Resolve only our own launch record, never an arbitrary path from metadata.
      id = pointer.fetch("launch_id")
      raise Error, "invalid manager launch id" unless id.match?(/\A[0-9a-f-]{36}\z/)

      read_json(File.join("manager", "#{id}.json"))
    end

    def print_timing(out, state)
      start = state["last_run_started_at"]
      return unless start

      ending = state["last_run_finished_at"]
      out.puts "Last run started: #{start}"
      out.puts "Last run finished: #{ending}" if ending
      return unless ending || state["executor_active"]

      seconds = (ending ? Time.iso8601(ending) : Time.now) - Time.iso8601(start)
      out.puts "Last run elapsed: #{[seconds.round, 0].max}s"
    end

    def print_jobs(out, jobs, active)
      %w[running failed interrupted].each do |status|
        selected = jobs.select { |row| row["status"] == status }
        selected.first(10).each do |row|
          reason = row["failure_reason"] ? " reason=#{row['failure_reason']}" : ""
          out.puts "#{status.capitalize}: #{row['job_id']} worker=#{row['worker']} attempt=#{row['attempt']} " \
                   "exit=#{row['exit_status'].inspect}#{reason}"
        end
        out.puts "#{status.capitalize}: #{selected.length - 10} more; use status --json" if selected.length > 10
      end
      return unless jobs.any? { |row| row["status"] == "running" } && !active

      out.puts "No executor holds the lock; running records may be stale. Check job processes before resuming."
    end

    def print_manager(out, manager)
      return unless manager

      out.puts "Last manager: PID #{manager['pid']} | recorded #{manager['status']} " \
               "| exit=#{manager['exit_status'].inspect}"
      out.puts "Manager log: #{manager['log_path']}"
      out.puts "Manager record: #{manager['record_path']}"
    end
  end
end
