# frozen_string_literal: true

require "socket"
require "net/http"
require "open3"
require "rbconfig"

# Test-only, fail-closed guard. Explicit argv allowlisting applies to every
# subprocess; children inherit this guard. No shell or provider client is allowed.
module Fo24aOfflineGuard
  class Violation < Exception; end # Do not let production StandardError rescue hide an escape.
  COMPONENTS = %w[workload_orchestrator local_ollama_workers runpod_ollama_fleet
                  local_model_evaluation adventure_finder af_workloads].freeze
  @allowed = []
  @component = nil
  @violations = []

  class << self
    attr_reader :violations

    def deny!(operation)
      @violations << operation
      raise Violation, "FO-24A blocked #{operation}"
    end

    def component!(name)
      @component = name
    end

    def import!(path)
      return unless @component

      normalized = path.to_s.tr("-", "_")
      own = @component == "runpod_ollama_fleet" ? %w[runpod_ollama_fleet local_model_evaluation] : [@component]
      forbidden = (COMPONENTS - own).find { |name| normalized.match?(%r{(?:\A|/)#{name}(?:/|\.rb|\z)}) }
      deny!("sibling import #{path}") if forbidden
    end

    def permit(argv)
      @allowed << argv
      yield
    ensure
      @allowed.delete(argv)
    end

    def spawn!(arguments)
      values = arguments.dup
      values.shift if values.first.is_a?(Hash)
      values.pop if values.last.is_a?(Hash)
      deny!("subprocess #{values.inspect}") unless @allowed.include?(values)
    end

    def assert_clean!
      raise Violation, "guard violations: #{@violations.inspect}" unless @violations.empty?
    end

    def self_check!
      checks = [
        -> { Socket.new(:INET, :STREAM) },
        -> { TCPSocket.new("127.0.0.1", 1) },
        -> { Net::HTTP.get(URI("http://127.0.0.1:1")) },
        -> { Process.spawn("ssh", "forbidden") },
        -> { IO.popen("ollama run forbidden") },
        -> { system("curl", "http://127.0.0.1:1") },
        -> { ENV["RUNPOD_API_KEY"] }
      ]
      checks.each do |check|
        begin
          check.call
        rescue Violation
          next
        end
        raise Violation, "offline guard self-check escaped"
      end
      @violations.clear # Only deliberate self-checks are cleared.
    end
  end

  module ImportsAndProcesses
    def require(path)
      Fo24aOfflineGuard.import!(path)
      super
    end

    def require_relative(path)
      location = caller_locations(1, 1).first
      require File.expand_path(path, File.dirname(location.absolute_path || location.path))
    end

    def load(path, *args)
      Fo24aOfflineGuard.import!(path)
      super
    end

    def system(*) = Fo24aOfflineGuard.deny!("Kernel.system")
    def exec(*) = Fo24aOfflineGuard.deny!("Kernel.exec")
    def spawn(*args, **options)
      Fo24aOfflineGuard.spawn!(args)
      super
    end
    def fork(*) = Fo24aOfflineGuard.deny!("Kernel.fork")
    def `(*) = Fo24aOfflineGuard.deny!("shell command")
  end

  module Processes
    def spawn(*args, **options)
      Fo24aOfflineGuard.spawn!(args)
      super
    end
    def exec(*) = Fo24aOfflineGuard.deny!("Process.exec")
    def fork(*) = Fo24aOfflineGuard.deny!("Process.fork")
  end

  module Credentials
    def [](key)
      Fo24aOfflineGuard.deny!("credential read") if key.to_s.match?(/RUNPOD.*(?:KEY|TOKEN)|API_TOKEN/i)
      super
    end
    def fetch(key, *)
      Fo24aOfflineGuard.deny!("credential read") if key.to_s.match?(/RUNPOD.*(?:KEY|TOKEN)|API_TOKEN/i)
      super
    end
  end
end

Kernel.prepend(Fo24aOfflineGuard::ImportsAndProcesses)
Process.singleton_class.prepend(Fo24aOfflineGuard::Processes)
ENV.singleton_class.prepend(Fo24aOfflineGuard::Credentials)
[Socket, TCPSocket, TCPServer, UDPSocket, UNIXSocket, UNIXServer].each do |klass|
  %i[new open tcp udp unix getaddrinfo gethostbyname].each do |method|
    klass.define_singleton_method(method) { |*| Fo24aOfflineGuard.deny!("socket #{method}") }
  end
end
%i[connect connect_nonblock send sendmsg sendmsg_nonblock].each do |method|
  BasicSocket.define_method(method) { |*| Fo24aOfflineGuard.deny!("socket #{method}") }
end
Net::HTTP.prepend(Module.new do
  def request(*) = Fo24aOfflineGuard.deny!("HTTP request")
  def connect(*) = Fo24aOfflineGuard.deny!("HTTP connect")
end)
IO.define_singleton_method(:popen) { |*| Fo24aOfflineGuard.deny!("IO.popen") }
