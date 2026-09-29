# frozen_string_literal: true

require "securerandom"

module WorkloadOrchestrator
  # Unix session detachment. The existing Runner owns the execution lock.
  class DetachedManager
    def initialize(runner)
      @runner = runner
      @root = runner.store.output_dir
    end

    def start(resume: false, acknowledge_circuit_breaker: false)
      raise Error, "detached start requires fork support" unless Process.respond_to?(:fork)

      reader, writer = IO.pipe
      pid = Process.fork do
        reader.close
        child(writer, resume, acknowledge_circuit_breaker)
      end
      writer.close
      Process.detach(pid)
      response = reader.gets
      raise Error, "manager #{pid} exited before acknowledging startup" unless response

      document = JSON.parse(response)
      raise Error, document.fetch("error") if document.key?("error")

      document
    rescue SystemCallError => e
      raise Error, "cannot start detached manager: #{e.message}"
    ensure
      reader&.close unless reader&.closed?
      writer&.close unless writer&.closed?
    end

    private

    def child(writer, resume, acknowledge)
      Process.setsid
      $stdin.reopen(File::NULL, "r")
      $stdout.reopen(File::NULL, "w")
      $stderr.reopen(File::NULL, "w")
      status = @runner.run(resume: resume, acknowledge_circuit_breaker: acknowledge) do
        publish_start(writer)
      end
      code = %w[completed paused].include?(status) ? 0 : 2
      finish(status, code)
    rescue StandardError => e
      warn "#{e.class}: #{e.message}"
      writer.puts(JSON.generate("error" => e.message)) unless writer.closed?
      finish("error", 1, e.message)
      code = 1
    ensure
      writer.close unless writer.closed?
      exit!(code || 1)
    end

    def publish_start(writer)
      id = SecureRandom.uuid
      directory = File.join(@root, "manager")
      FileUtils.mkdir_p(directory)
      @record_path = File.join(directory, "#{id}.json")
      @record = {
        "launch_id" => id, "pid" => Process.pid, "started_at" => Time.now.iso8601,
        "status" => "running", "log_path" => File.join(directory, "#{id}.log"),
        "record_path" => @record_path
      }
      $stdout.reopen(@record.fetch("log_path"), "a")
      $stderr.reopen($stdout)
      $stdout.sync = $stderr.sync = true
      write_record(@record_path, @record)
      # This pointer is only changed by a new launch while holding the execution lock.
      write_record(File.join(@root, "manager.json"), @record)
      puts "Manager PID #{Process.pid}; started #{@record.fetch('started_at')}"
      writer.puts(JSON.generate(@record))
      writer.close
    end

    def finish(status, code, error = nil)
      return unless @record

      @record.merge!("status" => status, "exit_status" => code, "finished_at" => Time.now.iso8601)
      @record["error"] = error if error
      write_record(@record_path, @record)
    end

    def write_record(path, record)
      temporary = "#{path}.tmp.#{Process.pid}"
      File.write(temporary, "#{JSON.pretty_generate(record)}\n")
      File.rename(temporary, path)
    ensure
      File.delete(temporary) if temporary && File.exist?(temporary)
    end
  end
end
