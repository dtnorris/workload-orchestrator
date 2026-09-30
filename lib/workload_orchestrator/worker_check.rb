# frozen_string_literal: true

require "json"
require "net/http"
require "uri"

module WorkloadOrchestrator
  class WorkerCheck
    Result = Struct.new(:pool_id, :worker_name, :ok, :detail, :version, :model_digest,
                        keyword_init: true)

    def initialize(fetch_json: nil, open_timeout: 3, read_timeout: 10)
      @fetch_json = fetch_json || method(:http_json)
      @open_timeout = open_timeout
      @read_timeout = read_timeout
    end

    def check_plan(plan, workers = nil)
      plan.execution_profile&.ensure_runnable!

      raise Error, "worker configuration is required" unless workers

      workers.validate_plan!(plan)
      plan.pools.flat_map do |pool|
        pool.worker_names.map { |name| check_worker(pool, workers.fetch(name)) }
      end
    end

    def check_plan!(plan, workers = nil)
      results = check_plan(plan, workers)
      failures = results.reject(&:ok)
      return results if failures.empty?

      detail = failures.map { |row| "#{row.pool_id}/#{row.worker_name}: #{row.detail}" }.join("; ")
      raise Error, "worker readiness failed: #{detail}"
    end

    private

    def check_worker(pool, worker)
      requirement = pool.ollama_requirement
      return Result.new(pool_id: pool.id, worker_name: worker.name, ok: true, detail: "ready") unless requirement

      check_ollama(pool, worker, requirement)
    rescue StandardError => e
      Result.new(pool_id: pool.id, worker_name: worker.name, ok: false, detail: "#{e.class}: #{e.message}")
    end

    def check_ollama(pool, worker, requirement)
      version = @fetch_json.call(worker.base_url, "/api/version").fetch("version", "?").to_s
      tags = @fetch_json.call(worker.base_url, "/api/tags")
      digest = digest_for(tags, requirement.fetch("model"))
      expected = requirement.fetch("expected_digest")
      if digest != expected
        actual = digest || "missing"
        return Result.new(
          pool_id: pool.id,
          worker_name: worker.name,
          ok: false,
          detail: "model digest mismatch: expected #{expected}, got #{actual}",
          version: version,
          model_digest: digest
        )
      end

      Result.new(
        pool_id: pool.id,
        worker_name: worker.name,
        ok: true,
        detail: "ready",
        version: version,
        model_digest: digest
      )
    end

    def digest_for(tags, model)
      rows = Array(tags["models"])
      normalized = model.sub(/:latest\z/, "")
      match = rows.find do |row|
        names = [row["name"], row["model"]].compact.map { |value| value.to_s.sub(/:latest\z/, "") }
        names.include?(normalized)
      end
      digest = match && match["digest"].to_s.strip.downcase
      digest unless digest.nil? || digest.empty?
    end

    def http_json(base_url, path)
      uri = URI.join("#{base_url}/", path.sub(%r{\A/}, ""))
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == "https"
      http.open_timeout = @open_timeout
      http.read_timeout = @read_timeout
      response = http.get(uri.request_uri)
      raise Error, "HTTP #{response.code} from #{uri}" unless response.is_a?(Net::HTTPSuccess)

      JSON.parse(response.body)
    end
  end
end
