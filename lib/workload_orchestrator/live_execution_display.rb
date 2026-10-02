# frozen_string_literal: true

module WorkloadOrchestrator
  # Plain scrollback on every stream. Previous values only suppress repeated
  # rendering; all counts, conditions and lifecycle events come from durable state.
  class LiveExecutionDisplay
    def initialize(plan:, output:, out:)
      @report = LiveExecutionReport.new(plan: plan, output: output)
      @out = out
      @workers = nil
      @previous = nil
      @mutex = Mutex.new
    end

    def accept_workers(workers)
      @mutex.synchronize { @workers = workers }
    end

    def refresh(resume: false)
      @mutex.synchronize do
        state = @report.document(workers: @workers)
        @out.puts "#{resume ? 'RESUME' : 'EXECUTION'} #{state.fetch('plan_id')}" unless @previous
        print_events(state)
        print_progress(state)
        print_workers(state)
        condition = state["condition"]
        if condition != @previous&.fetch("condition")
          @out.puts(condition || "ACTIVE compatible capacity available") if condition || @previous&.dig("condition")&.start_with?("WAIT")
        end
        @previous = state
        @out.flush if @out.respond_to?(:flush)
      end
    end

    private

    def print_events(state)
      previous = Array(@previous && @previous["jobs"]).to_h { |row| [row.fetch("job_id"), row] }
      if !@previous
        running = state.fetch("jobs").select { |row| row["status"] == "running" }
        running.first(10).each { |row| @out.puts "RUNNING #{@report.job_label(row)}" }
        @out.puts "RUNNING #{running.length - 10} more; use status --json" if running.length > 10
        return
      end
      state.fetch("jobs").each do |row|
        prior = previous[row.fetch("job_id")]
        next if prior && prior.values_at("attempt", "status") == row.values_at("attempt", "status")

        event = case row["status"]
                when "running" then "START"
                when "complete" then "DONE"
                when "failed" then row["in_doubt"] ? "IN_DOUBT" : "FAIL"
                when "interrupted" then "INTERRUPTED"
                end
        next unless event

        reason = row["failure_reason"] ? " reason=#{row['failure_reason']}" : ""
        @out.puts "#{event} #{@report.job_label(row)}#{reason}"
      end
    end

    def print_progress(state)
      return if @previous && state["counts"] == @previous["counts"]

      counts = state.fetch("counts")
      interrupted = counts.fetch("interrupted", 0)
      interrupted_text = interrupted.positive? ? " / #{interrupted} interrupted" : ""
      @out.puts "Progress: #{counts['complete']} complete / #{counts['running']} running / " \
                "#{counts['pending']} pending / #{counts['failed']} failed#{interrupted_text} " \
                "(#{state['terminal']}/#{state['total']} terminal, #{state['progress_percent']}%)"
    end

    def print_workers(state)
      keys = %w[registry_known ready_workers busy_workers idle_workers workers]
      return if @previous && state.values_at(*keys) == @previous.values_at(*keys)
      unless state["registry_known"]
        @out.puts "Workers: awaiting accepted registry snapshot"
        return
      end

      @out.puts "Workers: #{state['ready_workers']} READY / #{state['busy_workers']} busy / #{state['idle_workers']} idle"
      previous_workers = Array(@previous && @previous["workers"])
      changed = state.fetch("workers").reject { |worker| previous_workers.include?(worker) }
      changed.first(10).each do |worker|
        @out.puts "  #{worker['worker_id']} #{worker['busy'] ? 'busy' : 'idle'} " \
                  "models=#{worker['models'].join(',')} pools=#{worker['pools'].join(',')} " \
                  "generation=#{worker['generation_id']}"
      end
      remaining = changed.length - 10
      @out.puts "  #{remaining} more READY workers" if remaining.positive?
    end
  end
end
