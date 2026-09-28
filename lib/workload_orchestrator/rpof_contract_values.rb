# frozen_string_literal: true

require_relative "error"

module WorkloadOrchestrator
  module RpofContractValues
    def object!(value, required, optional = [])
      raise Error, "expected a JSON object" unless value.is_a?(Hash) && value.keys.all?(String)
      raise Error, "missing required fields" unless (required - value.keys).empty?
      raise Error, "unknown fields" unless (value.keys - required - optional).empty?
    end

    def version!(document, expected)
      raise Error, "unsupported contract_version; expected #{expected}" unless document["contract_version"] == expected
    end

    def text!(value, label, max: nil, pattern: nil)
      unless value.is_a?(String) && !value.empty? && !value.include?("\0") &&
             (!max || value.length <= max) && (!pattern || value.match?(pattern))
        raise Error, "invalid #{label}"
      end

      value
    end

    def array!(value, label)
      raise Error, "#{label} must be a non-empty array" unless value.is_a?(Array) && !value.empty?

      value
    end

    def indices!(value)
      array!(value, "worker indices")
      return if value.all? { |index| index.is_a?(Integer) && index.positive? } && value.uniq.length == value.length

      raise Error, "worker indices must be unique positive integers"
    end

    def boolean!(value, label)
      raise Error, "#{label} must be boolean" unless [true, false].include?(value)
    end
  end
end
