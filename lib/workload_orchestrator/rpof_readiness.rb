# frozen_string_literal: true

require_relative "rpof_contract"

module WorkloadOrchestrator
  # Translate immutable workload requirements into a read-only provider check.
  # Fleet selection belongs to the profile; requirements belong to the plan.
  class RpofReadiness
    attr_reader :pool, :binding, :request

    def initialize(pool, binding)
      @pool = pool
      @binding = binding
      unless pool.required_labels.empty?
        raise Error, "pool #{pool.id}: RPOF cannot verify required_labels"
      end
      target = binding["target"]
      raise Error, "pool #{pool.id}: RPOF readiness requires a profile target" unless target

      requirement = pool.ollama_requirement
      unless requirement && requirement.key?("required_context_length") &&
             requirement["require_fully_gpu_resident"] == true
        raise Error, "pool #{pool.id}: RPOF readiness requires exact model/digest, " \
                     "required_context_length and require_fully_gpu_resident=true"
      end
      requirements = {
        "models" => [{
          "name" => requirement.fetch("model"), "expected_digest" => requirement.fetch("expected_digest")
        }],
        "required_context_length" => requirement.fetch("required_context_length"),
        "require_fully_gpu_resident" => true
      }
      requirements["required_gpu_id"] = requirement["required_gpu_id"] if requirement.key?("required_gpu_id")
      @request = {
        "contract_version" => RpofContract::CAPABILITY_REQUEST,
        "fleet_key" => target.fetch("fleet_key"),
        "worker_selector" => target.fetch("worker_selector"),
        "requirements" => requirements
      }
      RpofContract.capability_request!(request)
      selector = request.fetch("worker_selector")
      validate_capacity!(selector.fetch("indices").length) if selector.fetch("mode") == "indices"
    end

    def check(client)
      result = client.capability_check(request)
      document = result.document
      RpofContract.version!(document, RpofContract::CAPABILITY_RESULT)
      RpofContract.capability_result!(document, request, result.exit_status)
      validate_diagnostics!(document)
      return document unless document.fetch("ready")

      validate_capacity!(document.fetch("selected_worker_indices").length)
      validate_capabilities!(document.fetch("capabilities"))
      document
    end

    def detail(document)
      diagnostics = document.fetch("diagnostics").map do |row|
        "#{row.fetch('code')}=#{row.fetch('status')}: #{row.fetch('detail')}"
      end
      identity = "fleet=#{document['fleet_id']} workers=#{Array(document['selected_worker_indices']).join(',')}"
      ([identity] + diagnostics).join("; ")
    end

    private

    def validate_capacity!(count)
      minimum = [binding.fetch("min_workers"), binding.fetch("max_concurrency")].max
      maximum = binding.fetch("desired_workers")
      return if count.between?(minimum, maximum)

      raise Error, "pool #{pool.id}: selected worker count #{count} must be within #{minimum}..#{maximum}"
    end

    def validate_diagnostics!(document)
      valid = document.fetch("diagnostics").all? do |row|
        row.is_a?(Hash) && row["code"].is_a?(String) && !row["code"].empty? &&
          %w[PASS FAIL SKIP].include?(row["status"]) && row["detail"].is_a?(String)
      end
      raise Error, "malformed RPOF readiness diagnostics" unless valid
      if document.fetch("ready") && document.fetch("diagnostics").any? { |row| row["status"] == "FAIL" }
        raise Error, "RPOF ready result contains a failed diagnostic"
      end
    end

    def validate_capabilities!(capabilities)
      required = request.fetch("requirements")
      gpu = capabilities["gpu_id"]
      unless gpu.is_a?(String) && !gpu.strip.empty? &&
             (!required.key?("required_gpu_id") || gpu == required["required_gpu_id"])
        raise Error, "RPOF GPU capability is missing or mismatched"
      end
      expected = required.fetch("models").first
      models = capabilities["models"]
      unless models.is_a?(Array) && models.all? { |row| row.is_a?(Hash) }
        raise Error, "RPOF model capabilities are missing or malformed"
      end
      matches = models.select { |row| row["name"] == expected.fetch("name") }
      raise Error, "RPOF must report exactly one capability for #{expected.fetch('name')}" unless matches.length == 1

      observed = matches.first
      unless observed["digest"].is_a?(String) && observed["digest"].downcase == expected.fetch("expected_digest")
        raise Error, "RPOF model digest capability mismatch"
      end
      context = observed["context_length"]
      unless context.is_a?(Integer) && context == required.fetch("required_context_length")
        raise Error, "RPOF context capability mismatch"
      end
      raise Error, "RPOF full GPU residency is not proven" unless observed["fully_gpu_resident"] == true
    end
  end
end
