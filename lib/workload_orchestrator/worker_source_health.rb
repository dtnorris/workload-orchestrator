# frozen_string_literal: true

require "fileutils"
require "json"
require "time"

module WorkloadOrchestrator
  # Durable poll-attempt evidence kept separate from accepted registry
  # checkpoints. Failed polls may update health, but never checkpoint authority.
  class WorkerSourceHealth
    CONTRACT_VERSION = "wlo-worker-source-health/v0.1"
    POLICIES = %w[required optional].freeze
    RESULTS = %w[success failure].freeze
    RECORD_KEYS = %w[
      contract_version source_name policy last_attempted_at last_poll_result failure_reason
    ].freeze

    class << self
      def write(path:, source_name:, policy:, attempted_at:, result:, failure_reason: nil)
        validate_policy!(policy)
        raise Error, "worker source poll result is invalid" unless RESULTS.include?(result)

        document = {
          "contract_version" => CONTRACT_VERSION,
          "source_name" => source_name,
          "policy" => policy,
          "last_attempted_at" => attempted_at.iso8601,
          "last_poll_result" => result,
          "failure_reason" => failure_reason
        }
        atomic_write(path, document)
        deep_freeze(document)
      end

      def load_root(root, clock: -> { Time.now.utc })
        root = File.expand_path(root)
        named = Dir.glob(File.join(root, "sources", "*", "health.json"))
        legacy = File.join(root, "health.json")
        paths = named.empty? && File.file?(legacy) ? [legacy] : named
        now = current_time(clock)
        paths.sort.map do |path|
          record = load_record(path)
          checkpoint_path = File.join(File.dirname(path), "checkpoint.json")
          checkpoint = load_checkpoint(checkpoint_path)
          build_view(record:, checkpoint:, now:)
        end.freeze
      end

      def load_record(path)
        document = JSON.parse(File.read(path))
        validate_record!(document)
        deep_freeze(document)
      rescue Errno::ENOENT, JSON::ParserError, KeyError, ArgumentError, TypeError => e
        raise Error, "invalid worker source health #{path}: #{e.message}"
      end

      def view(source_name:, policy:, checkpoint:, poll:, now:)
        record = {
          "contract_version" => CONTRACT_VERSION,
          "source_name" => source_name,
          "policy" => policy,
          "last_attempted_at" => poll.fetch(:last_attempted_at),
          "last_poll_result" => poll.fetch(:last_poll_result),
          "failure_reason" => poll.fetch(:failure_reason)
        }
        build_view(record:, checkpoint:, now:)
      end

      private

      def build_view(record:, checkpoint:, now:)
        validate_record!(record)
        now = now.getutc
        expires_at = checkpoint && Time.iso8601(checkpoint.fetch("expires_at"))
        state = if checkpoint.nil?
                  "unavailable"
                elsif expires_at > now
                  "fresh"
                else
                  "stale"
                end
        usable = state == "fresh"
        reason = record.fetch("failure_reason")
        if !usable && reason.nil?
          reason = if checkpoint
                     "last accepted snapshot expired at #{checkpoint.fetch('expires_at')}"
                   else
                     "no accepted registry snapshot"
                   end
        end
        deep_freeze(
          "source_name" => record.fetch("source_name"),
          "policy" => record.fetch("policy"),
          "registry_id" => checkpoint&.fetch("registry_id"),
          "last_accepted_revision" => checkpoint&.fetch("revision"),
          "last_accepted_published_at" => checkpoint&.fetch("published_at"),
          "last_accepted_expires_at" => checkpoint&.fetch("expires_at"),
          "state" => state,
          "usable" => usable,
          "last_poll_result" => record.fetch("last_poll_result"),
          "last_attempted_at" => record.fetch("last_attempted_at"),
          "blocking" => record.fetch("policy") == "required" && !usable,
          "failure_reason" => reason
        )
      end

      def load_checkpoint(path)
        return unless File.file?(path)

        JSON.parse(File.read(path))
      rescue Errno::ENOENT, JSON::ParserError => e
        raise Error, "invalid worker source checkpoint #{path}: #{e.message}"
      end

      def validate_record!(document)
        unless document.is_a?(Hash) && document.keys.sort == RECORD_KEYS.sort
          raise Error, "worker source health fields are invalid"
        end
        raise Error, "worker source health contract is invalid" unless
          document.fetch("contract_version") == CONTRACT_VERSION

        name = document.fetch("source_name")
        raise Error, "worker source health name is invalid" unless
          name.is_a?(String) && name.match?(WorkerSourceSet::NAME)

        validate_policy!(document.fetch("policy"))
        result = document.fetch("last_poll_result")
        raise Error, "worker source health poll result is invalid" unless
          RESULTS.include?(result) || result == "never"

        attempted_at = document.fetch("last_attempted_at")
        Time.iso8601(attempted_at) if attempted_at
        reason = document.fetch("failure_reason")
        return if reason.nil? || (reason.is_a?(String) && !reason.empty?)

        raise Error, "worker source health failure reason is invalid"
      end

      def validate_policy!(policy)
        return if POLICIES.include?(policy)

        raise Error, "worker source policy must be required or optional"
      end

      def current_time(clock)
        value = clock.call
        raise Error, "worker source health clock must return a Time" unless value.is_a?(Time)

        value.getutc
      end

      def atomic_write(path, document)
        path = File.expand_path(path)
        FileUtils.mkdir_p(File.dirname(path))
        temporary = "#{path}.tmp.#{Process.pid}.#{Thread.current.object_id}"
        File.write(temporary, "#{JSON.pretty_generate(document)}\n")
        File.rename(temporary, path)
      ensure
        File.delete(temporary) if defined?(temporary) && temporary && File.exist?(temporary)
      end

      def deep_freeze(value)
        case value
        when Hash
          value.each do |key, item|
            deep_freeze(key)
            deep_freeze(item)
          end
        when Array then value.each { |item| deep_freeze(item) }
        end
        value.freeze
      end
    end
  end
end
