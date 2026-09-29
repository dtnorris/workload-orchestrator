# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require_relative "rpof_readiness"

module WorkloadOrchestrator
  class WorkerCheck
    Result = Struct.new(:pool_id, :worker_name, :ok, :detail, :version, :model_digest,
                        :provider_result, keyword_init: true)

    def initialize(fetch_json: nil, open_timeout: 3, read_timeout: 10, rpof_client: nil)
      @rpof_client = rpof_client
      @fetch_json = fetch_json || method(:http_json)
      @open_timeout = open_timeout
      @read_timeout = read_timeout
    end

    def check_plan(plan, workers = nil)
      return check_remote_plan(plan, workers) if plan.execution_profile&.rpof?

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

    def check_remote_plan(plan, workers)
      checks = {}
      fixed = plan.pools.reject do |pool|
        binding = plan.execution_profile.binding_for(pool.id)
        next false unless binding.fetch("backend") == "rpof"

        checks[pool.id] = RpofReadiness.new(pool, binding)
      end
      # Validate all input before the first provider process or HTTP request.
      unless fixed.empty?
        raise Error, "worker configuration is required for fixed pools" unless workers

        workers.validate_pools!(fixed)
      end
      raise Error, "RPOF readiness requires --rpof-executable FILE" unless @rpof_client

      plan.pools.flat_map do |pool|
        if checks.key?(pool.id)
          [check_rpof(checks.fetch(pool.id))]
        else
          pool.worker_names.map { |name| check_worker(pool, workers.fetch(name)) }
        end
      end
    end

    def check_rpof(check)
      document = check.check(@rpof_client)
      Result.new(pool_id: check.pool.id, worker_name: "rpof:#{check.request.fetch('fleet_key')}",
                 ok: document.fetch("ready"), detail: check.detail(document), provider_result: document)
    rescue Error => e
      Result.new(pool_id: check.pool.id, worker_name: "rpof:#{check.request.fetch('fleet_key')}",
                 ok: false, detail: e.message)
    end

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
