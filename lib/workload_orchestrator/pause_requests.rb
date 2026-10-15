# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"

module WorkloadOrchestrator
  # Independent durable pause holds for one execution. Manual control remains
  # control/pause; automatic release can unlink only its token-bound request.
  class PauseRequests
    CONTRACT = "wlo-pause-control/v0.1"
    TOKEN = /\A[0-9a-f]{64}\z/

    # Bind controls to an existing output directory; no files are created.
    def initialize(output)
      @output = File.expand_path(output)
      @directory = File.join(@output, "control", "pause-requests")
    end

    # True for any outstanding hold, including malformed evidence (fail closed).
    def paused?
      File.exist?(manual_path) || File.symlink?(manual_path) || !request_paths.empty?
    end

    # Publish a token's immutable hold, returning its public fingerprint.
    # Repeating identical authority is safe; changed/corrupt evidence raises Error.
    def request!(token)
      identifier = request_id(token)
      document = {"contract_version" => CONTRACT, "consumer_id" => consumer_id, "request_id" => identifier}
      path = File.join(@directory, "#{identifier}.json")
      raise Error, "invalid pause request directory" if File.symlink?(@directory) ||
        (File.exist?(@directory) && !File.directory?(@directory))
      FileUtils.mkdir_p(@directory)
      begin
        File.open(path, File::WRONLY | File::CREAT | File::EXCL, 0o600) do |file|
          file.write(JSON.generate(document) + "\n")
          file.flush
          file.fsync
        end
      rescue Errno::EEXIST
        raise Error, "pause request authority changed" unless read_request(path) == document
      end
      identifier
    end

    # Release exactly one retained, execution-bound token. Other owned and manual
    # holds are never touched; missing, forged or corrupt authority raises Error.
    def release!(token)
      identifier = request_id(token)
      path = File.join(@directory, "#{identifier}.json")
      document = read_request(path)
      raise Error, "pause request does not belong to this execution" unless
        document["consumer_id"] == consumer_id && document["request_id"] == identifier
      File.delete(path)
    rescue Errno::ENOENT
      raise Error, "pause request authority is missing or already released"
    end

    # Explicit manual resume clears all currently observed controls, preserving
    # the existing operator recovery experience. New later requests remain holds.
    def clear_all!
      ([manual_path] + request_paths).each do |path|
        File.delete(path) if File.exist?(path) || File.symlink?(path)
      rescue Errno::ENOENT
        # A concurrent scoped release already removed this same hold.
      end
    end

    # Return public fingerprints and execution identity, never bearer tokens.
    # Corrupt, copied, or unknown request files raise Error without mutation.
    def snapshot
      identity = consumer_id
      identifiers = request_paths.filter_map do |path|
        begin
          document = read_request(path)
        rescue Errno::ENOENT
          next # An independently released hold is no longer outstanding.
        end
        raise Error, "pause request does not belong to this execution" unless document["consumer_id"] == identity
        document.fetch("request_id")
      end
      {"contract_version" => CONTRACT, "consumer_id" => identity,
       "operator_pause" => File.exist?(manual_path) || File.symlink?(manual_path),
       "request_ids" => identifiers.sort}
    end

    private

    # Validate a private bearer token and return its public SHA-256 identifier.
    def request_id(token)
      raise Error, "pause owner token must be 64 lowercase hexadecimal characters" unless token.is_a?(String) && TOKEN.match?(token)
      Digest::SHA256.hexdigest(token)
    end

    # Derive the same execution identity as public consumer demand. A frozen
    # plan mismatch, missing state, or malformed binding raises Error.
    def consumer_id
      state = JSON.parse(File.binread(File.join(@output, "execution.json")))
      plan = Plan.load(File.join(@output, "plan.json"))
      valid = state.is_a?(Hash) && state["contract_version"] == ExecutionStore::CONTRACT_VERSION &&
        state["plan_id"] == plan.id &&
        state["plan_sha256"].is_a?(String) && TOKEN.match?(state["plan_sha256"]) &&
        state["workdir"].is_a?(String) && state["workdir"].start_with?("/") &&
        plan.sha256 == state["plan_sha256"]
      raise Error, "invalid pause execution identity" unless valid
      Digest::SHA256.hexdigest(JSON.generate([state.fetch("plan_sha256"), state.fetch("workdir"), @output]))
    rescue JSON::ParserError, Errno::ENOENT, TypeError => error
      raise Error, "invalid pause execution identity: #{error.message}"
    end

    # Read one immutable regular-file authority; reject unknown names and shapes.
    def read_request(path)
      identifier = File.basename(path, ".json")
      stat = File.lstat(path)
      raise Error, "invalid pause request path" unless TOKEN.match?(identifier) &&
        File.basename(path) == "#{identifier}.json" && stat.file?
      document = JSON.parse(File.binread(path))
      raise Error, "invalid pause request evidence" unless document.is_a?(Hash) &&
        document.keys.sort == %w[consumer_id contract_version request_id] &&
        document["contract_version"] == CONTRACT && document["request_id"] == identifier &&
        document["consumer_id"].is_a?(String) && TOKEN.match?(document["consumer_id"])
      document
    rescue JSON::ParserError => error
      raise Error, "invalid pause request evidence: #{error.message}"
    end

    # Enumerate every hold so unknown entries cannot silently permit execution.
    def request_paths
      raise Error, "invalid pause request directory" if File.symlink?(@directory)
      return [] unless File.exist?(@directory)
      raise Error, "invalid pause request directory" unless File.directory?(@directory)
      Dir.children(@directory).sort.map { |name| File.join(@directory, name) }
    end

    # Return the unchanged legacy/operator marker location.
    def manual_path
      File.join(@output, "control", "pause")
    end
  end
end
