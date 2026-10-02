# frozen_string_literal: true

module WorkloadOrchestrator
  # Compact, width-bounded rendering of the truthful FO-04 pool status model.
  class ExecutionDashboard
    DEFAULT_WIDTH = 72
    MINIMUM_WIDTH = 48
    LABEL_WIDTH = 8

    STATE_LABELS = {
      "RUNNING" => "RUN",
      "READY_TO_DISPATCH" => "READY",
      "NO_COMPATIBLE_READY_WORKERS" => "WAIT no-ready",
      "ALL_COMPATIBLE_WORKERS_BUSY" => "WAIT busy",
      "WORKERS_NOT_READY" => "WAIT not-ready",
      "READY_WORKERS_INCOMPATIBLE" => "WAIT incompatible",
      "PAUSED" => "PAUSE",
      "CIRCUIT_BREAKER" => "BLOCK breaker",
      "DISPATCH_HALTED" => "BLOCK dispatch",
      "NO_ACCEPTED_REGISTRY_SNAPSHOT" => "BLOCK no-registry",
      "REGISTRY_INVALID_OR_STALE" => "BLOCK registry",
      "COMPLETE" => "DONE",
      "FAILED" => "FAIL",
      "INTERRUPTED" => "FAIL interrupted",
      "NO_PENDING_WORK" => "DONE",
      "NO_RUNNABLE_WORK" => "WAIT dependencies"
    }.freeze

    def initialize(width: DEFAULT_WIDTH)
      @width = [Integer(width), MINIMUM_WIDTH].max
    rescue ArgumentError, TypeError
      raise Error, "dashboard width must be an integer"
    end

    def render(document)
      specs = document.fetch("pool_status").map { |pool| pool_spec(pool) }
      specs << overall_spec(document)
      bar_width = specs.map { |spec| available_bar_width(spec) }.min
      lines = specs.map { |spec| progress_line(*spec, bar_width) }
      "#{lines.join("\n")}\n"
    end

    private

    def pool_spec(pool)
      jobs = pool.fetch("jobs")
      workers = pool.fetch("workers")
      complete = jobs.fetch("complete")
      total = jobs.values.sum
      state = STATE_LABELS.fetch(pool.fetch("reason"))
      suffix = "#{state} r#{workers.fetch('compatible_ready')} b#{workers.fetch('busy')} " \
               "i#{workers.fetch('idle')} f#{jobs.fetch('failed')}"
      [pool.fetch("pool_id"), complete, total, suffix]
    end

    def overall_spec(document)
      counts = document.fetch("counts")
      complete = counts.fetch("complete")
      total = document.fetch("total")
      terminal = complete + counts.fetch("failed") + counts.fetch("interrupted", 0)
      suffix = "run:#{counts.fetch('running')} fail:#{counts.fetch('failed')} term:#{terminal}"
      ["ALL", complete, total, suffix]
    end

    def available_bar_width(spec)
      _label, current, total, suffix = spec
      count = "#{current}/#{total}"
      fixed = LABEL_WIDTH + count.length + percentage(current, total).to_s.length + suffix.length + 14
      [@width - fixed, 4].max
    end

    def progress_line(label, current, total, suffix, bar_width)
      count = "#{current}/#{total}"
      percent = percentage(current, total)
      line = format(
        "%-#{LABEL_WIDTH}s [%s] %s %d%% %s",
        truncate(label, LABEL_WIDTH), bar(current, total, bar_width), count, percent, suffix
      )
      line[0, @width].rstrip
    end

    def percentage(current, total)
      total.zero? ? 100 : (100.0 * current / total).floor
    end

    def bar(current, total, width)
      ratio = total.zero? ? 1.0 : current.fdiv(total)
      filled = (ratio * width).round.clamp(0, width)
      "#{'=' * filled}#{'-' * (width - filled)}"
    end

    def truncate(value, width)
      text = value.to_s
      return text if text.length <= width

      "#{text[0, width - 1]}~"
    end
  end
end
