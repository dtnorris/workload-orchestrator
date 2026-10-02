# frozen_string_literal: true

require_relative "test_helper"

class ExecutionDashboardTest < Minitest::Test
  def test_four_pool_lines_plus_overall_line_preserve_order
    document = dashboard_document(
      pool("qwen", complete: 94, running: 3, failed: 1, pending: 46, reason: "ALL_COMPATIBLE_WORKERS_BUSY"),
      pool("qwen27", complete: 6, failed: 1, pending: 9, reason: "NO_COMPATIBLE_READY_WORKERS"),
      pool("gemma", complete: 10, running: 1, pending: 5, reason: "RUNNING"),
      pool("gptoss", complete: 3, pending: 13, reason: "READY_TO_DISPATCH", idle: 1)
    )

    lines = render(document).lines(chomp: true)

    assert_equal 5, lines.length
    ids = lines.map { |line| line.split.first }
    assert_equal %w[qwen qwen27 gemma gptoss ALL], ids
    assert_includes lines[0], "WAIT busy"
    assert_includes lines[1], "WAIT no-ready"
    assert_includes lines[2], "RUN"
    assert_includes lines[3], "READY"
    bar_widths = lines.map { |line| line.split("[", 2).last.split("]", 2).first.length }
    assert_equal 1, bar_widths.uniq.length
  end

  def test_success_and_terminal_progress_are_not_conflated
    document = dashboard_document(
      pool("qwen", complete: 5, failed: 3, pending: 2, reason: "NO_COMPATIBLE_READY_WORKERS")
    )

    lines = render(document).lines(chomp: true)

    assert_match(%r{5/10 50%}, lines.first)
    assert_includes lines.first, "f3"
    assert_match(%r{5/10 50%}, lines.last)
    assert_includes lines.last, "fail:3 term:8"
  end

  def test_interrupted_work_is_terminal_but_does_not_fill_success_bar
    document = dashboard_document(
      pool("qwen", complete: 2, interrupted: 1, pending: 1, reason: "INTERRUPTED")
    )

    lines = render(document).lines(chomp: true)

    assert_match(%r{2/4 50%}, lines.first)
    assert_includes lines.first, "FAIL interrupted"
    assert_includes lines.last, "fail:0 term:3"
  end

  def test_state_labels_are_direct_reason_mappings
    expected = {
      "READY_TO_DISPATCH" => "READY",
      "ALL_COMPATIBLE_WORKERS_BUSY" => "WAIT busy",
      "NO_COMPATIBLE_READY_WORKERS" => "WAIT no-ready",
      "PAUSED" => "PAUSE",
      "CIRCUIT_BREAKER" => "BLOCK breaker"
    }

    expected.each do |reason, label|
      line = render(dashboard_document(pool("pool", pending: 1, reason:))).lines.first
      assert_includes line, label
    end
  end

  def test_widths_are_bounded_and_long_pool_names_truncate_visibly
    document = dashboard_document(
      pool(
        "an-extremely-long-model-pool",
        reason: "READY_WORKERS_INCOMPATIBLE", complete: 3, failed: 1, pending: 8
      )
    )

    [72, 80, 100].each do |width|
      lines = render(document, width:).lines(chomp: true)
      assert_operator lines.map(&:length).max, :<=, width
      assert_includes lines.first, "an-extr~"
    end
  end

  private

  def render(document, width: 72)
    WorkloadOrchestrator::ExecutionDashboard.new(width:).render(document)
  end

  def dashboard_document(*pools)
    counts = %w[complete running failed interrupted pending].to_h do |key|
      [key, pools.sum { |pool| pool.dig("jobs", key) }]
    end
    {
      "pool_status" => pools,
      "counts" => counts,
      "total" => counts.values.sum
    }
  end

  def pool(id, reason:, **values)
    {
      "pool_id" => id,
      "jobs" => {
        "complete" => values.fetch(:complete, 0), "running" => values.fetch(:running, 0),
        "failed" => values.fetch(:failed, 0), "interrupted" => values.fetch(:interrupted, 0),
        "pending" => values.fetch(:pending, 0)
      },
      "workers" => {
        "compatible_ready" => values.fetch(:ready, 0),
        "busy" => values.fetch(:busy, 0), "idle" => values.fetch(:idle, 0)
      },
      "reason" => reason
    }
  end
end
