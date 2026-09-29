# frozen_string_literal: true

module WorkloadOrchestrator
  class ExecutionReport
    def initialize(plan:, output:)
      @plan = plan
      @root = File.expand_path(output)
    end

    def document
      state = read_json("execution.json")
      unless state["plan_id"] == @plan.id && state["plan_sha256"] == @plan.sha256
        raise Error, "output execution identity does not match plan"
      end

      jobs = read_json("jobs.json").fetch("jobs")
      counts = job_counts(jobs)
      terminal = counts.fetch("complete") + counts.fetch("failed")
      state.merge(
        "paused" => File.file?(File.join(@root, "control", "pause")), "jobs" => jobs,
        "counts" => counts, "total" => jobs.length, "terminal" => terminal,
        "progress_percent" => jobs.empty? ? 100.0 : (100.0 * terminal / jobs.length).round(1),
        "executor_active" => executor_active?, "manager" => manager_record
      )
    rescue SystemCallError, JSON::ParserError, KeyError => e
      raise Error, "cannot read execution status: #{e.message}"
    end

    def print(out)
      state = document
      out.puts "Plan: #{state.fetch('plan_id')}"
      out.puts "Execution: #{state.fetch('status')} | executor #{state['executor_active'] ? 'active' : 'inactive'}"
      out.puts "Progress: [#{state['terminal']}/#{state['total']}] terminal (#{state['progress_percent']}%)"
      out.puts "Jobs: #{state.fetch('counts').map { |key, value| "#{key}=#{value}" }.join(' ')}"
      print_controls(out, state)
      print_timing(out, state)
      print_jobs(out, state.fetch("jobs"), state["executor_active"])
      print_manager(out, state["manager"])
      out.puts "Evidence: #{@root}"
    end

    private

    def job_counts(jobs)
      observed = jobs.map { |job| job.fetch("status") }.tally
      %w[complete failed running pending].to_h { |status| [status, observed.fetch(status, 0)] }
    end

    def print_controls(out, state)
      out.puts "Pause: requested (draining active jobs)" if state["paused"] && state["executor_active"]
      out.puts "Pause: requested" if state["paused"] && !state["executor_active"]
      out.puts "Breaker: #{state.dig('circuit_breaker', 'reason')}" if state.dig("circuit_breaker", "tripped")
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
      %w[running failed].each do |status|
        selected = jobs.select { |row| row["status"] == status }
        selected.first(10).each do |row|
          out.puts "#{status.capitalize}: #{row['job_id']} worker=#{row['worker']} attempt=#{row['attempt']} " \
                   "exit=#{row['exit_status'].inspect}"
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
