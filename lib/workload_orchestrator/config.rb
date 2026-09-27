# frozen_string_literal: true

require "yaml"

module WorkloadOrchestrator
  class Config
    ENV_PATTERN = /\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}/

    def self.load_yaml(path)
      raw = File.read(File.expand_path(path))
      YAML.safe_load(expand_environment(raw), aliases: false) || {}
    rescue Errno::ENOENT => e
      raise Error, e.message
    rescue Psych::Exception => e
      raise Error, "invalid YAML in #{path}: #{e.message}"
    end

    def self.expand_environment(raw)
      raw.gsub(ENV_PATTERN) do
        name = Regexp.last_match(1)
        default = Regexp.last_match(2)
        value = ENV.fetch(name, nil)
        next value unless value.nil? || value.empty?
        next default unless default.nil?

        raise Error, "environment variable #{name} is not set"
      end
    end
    private_class_method :expand_environment
  end
end
