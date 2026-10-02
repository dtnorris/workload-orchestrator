# frozen_string_literal: true

require "time"

module WorkloadOrchestrator
  class CommandCancelled < StandardError
    attr_reader :stdout, :stderr, :status, :signal, :requested_at, :termination_mode,
                :pid, :process_group_id

    def initialize(stdout:, stderr:, status:, evidence:)
      super("command cancelled by #{evidence.fetch(:signal)}")
      @stdout = stdout
      @stderr = stderr
      @status = status
      @signal = evidence.fetch(:signal)
      @requested_at = evidence.fetch(:requested_at)
      @termination_mode = evidence.fetch(:termination_mode)
      @pid = evidence.fetch(:pid)
      @process_group_id = evidence.fetch(:process_group_id)
    end
  end

  # Runs each job in an execution-owned process group. The group boundary makes
  # descendants cancellable without matching process names or touching the
  # invoking shell's process group.
  class OwnedCommandRunner
    DEFAULT_TERM_GRACE_SECONDS = 1.0
    FORCE_SETTLE_SECONDS = 1.0
    POLL_SECONDS = 0.01

    Handle = Struct.new(
      :pid, :pgid, :cancelled, :signal, :requested_at, :termination_mode,
      keyword_init: true
    )

    def initialize(term_grace_seconds: DEFAULT_TERM_GRACE_SECONDS, clock: -> { Time.now.utc })
      @term_grace_seconds = Float(term_grace_seconds)
      raise Error, "cancellation grace interval must be nonnegative" if @term_grace_seconds.negative?

      @clock = clock
      @mutex = Mutex.new
      @active = {}
      @cancellation_handles = {}
      @cancelled = false
      @cancel_signal = nil
      @force_requested = false
      @cleanup_threads = []
    rescue ArgumentError, TypeError
      raise Error, "cancellation grace interval must be a nonnegative number"
    end

    def call(environment, *argv, chdir:)
      stdout_reader, stdout_writer, stderr_reader, stderr_writer = command_streams
      handle = spawn_handle(environment, argv, chdir, stdout_writer, stderr_writer)
      cancellation = register(handle)
      stdout_writer.close
      stderr_writer.close
      stdout_thread = Thread.new { stdout_reader.read }
      stderr_thread = Thread.new { stderr_reader.read }
      signal_group(handle, "TERM") if cancellation

      _, status = Process.wait2(handle.pid)
      stdout = stdout_thread.value
      stderr = stderr_thread.value
      wait_for_cancellation if handle.cancelled
      cancellation = cancellation_for(handle)
      if cancellation
        raise CommandCancelled.new(
          stdout: stdout, stderr: stderr, status: status,
          evidence: cancellation.merge(pid: handle.pid, process_group_id: handle.pgid)
        )
      end

      [stdout, stderr, status]
    ensure
      [stdout_writer, stderr_writer, stdout_reader, stderr_reader].compact.each do |stream|
        stream.close unless stream.closed?
      rescue IOError
        nil
      end
      unregister(handle) if handle
    end

    def cancel(signal:, force: false)
      handles = []
      first = false
      @mutex.synchronize do
        first = !@cancelled
        @cancelled = true
        @cancel_signal ||= signal.to_s
        @force_requested ||= force || !first
        handles = @active.values
        handles.each { |handle| mark_cancelled(handle, signal) }
        @cleanup_threads << Thread.new { finish_cancellation } if first
      end
      handles.each { |handle| signal_group(handle, @force_requested ? "KILL" : "TERM") }
      handles.length
    end

    def wait_for_cancellation
      loop do
        threads = @mutex.synchronize { @cleanup_threads.dup }
        threads.each(&:value)
        break if @mutex.synchronize { @cleanup_threads == threads }
      end
    end

    private

    def command_streams
      stdout_reader, stdout_writer = IO.pipe
      stderr_reader, stderr_writer = IO.pipe
      [stdout_reader, stdout_writer, stderr_reader, stderr_writer]
    end

    def spawn_handle(environment, argv, chdir, stdout_writer, stderr_writer)
      pid = Process.spawn(
        environment, *argv, chdir: chdir, pgroup: true, in: File::NULL,
        out: stdout_writer, err: stderr_writer
      )
      Handle.new(pid: pid, pgid: pid, cancelled: false)
    end

    def register(handle)
      cancellation = nil
      @mutex.synchronize do
        @active[handle.pid] = handle
        if @cancelled
          mark_cancelled(handle, @cancel_signal || "INT")
          @cleanup_threads << Thread.new { finish_cancellation }
          cancellation = true
        end
      end
      cancellation
    end

    def unregister(handle)
      @mutex.synchronize { @active.delete(handle.pid) }
    end

    def mark_cancelled(handle, signal)
      return if handle.cancelled

      handle.cancelled = true
      handle.signal = signal.to_s
      handle.requested_at = @clock.call.iso8601
      handle.termination_mode = "term"
      @cancellation_handles[handle.pid] = handle
    end

    def cancellation_for(handle)
      @mutex.synchronize do
        next unless handle.cancelled

        {
          signal: handle.signal,
          requested_at: handle.requested_at,
          termination_mode: handle.termination_mode
        }
      end
    end

    def finish_cancellation
      deadline = monotonic_now + @term_grace_seconds
      loop do
        break if cancellation_groups.none? { |handle| group_alive?(handle.pgid) }
        break if force_requested? || monotonic_now >= deadline

        sleep POLL_SECONDS
      end

      force_alive_groups
      settle_deadline = monotonic_now + FORCE_SETTLE_SECONDS
      while cancellation_groups.any? { |handle| group_alive?(handle.pgid) }
        break if monotonic_now >= settle_deadline

        sleep POLL_SECONDS
      end
    end

    def force_alive_groups
      cancellation_groups.each do |handle|
        next unless group_alive?(handle.pgid)

        @mutex.synchronize { handle.termination_mode = "kill" }
        signal_group(handle, "KILL")
      end
    end

    def cancellation_groups
      @mutex.synchronize { @cancellation_handles.values.dup }
    end

    def force_requested?
      @mutex.synchronize { @force_requested }
    end

    def signal_group(handle, signal)
      @mutex.synchronize { handle.termination_mode = "kill" } if signal == "KILL"
      Process.kill(signal, -handle.pgid)
    rescue Errno::ESRCH
      nil
    end

    def group_alive?(pgid)
      Process.kill(0, -pgid)
      true
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true
    end

    def monotonic_now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
