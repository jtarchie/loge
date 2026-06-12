# frozen_string_literal: true

require_relative "lib/loge/version"

Gem::Specification.new do |spec|
  spec.name = "loge"
  spec.version = Loge::VERSION
  spec.authors = ["JT Archie"]
  spec.email = ["jtarchie@gmail.com"]

  spec.summary = "Ship Ruby and Rails logs to a loge server"
  spec.description = "An asynchronous, batching logger that pushes log lines to a loge " \
                     "server's /api/v1/push endpoint. Includes a Railtie that broadcasts " \
                     "Rails.logger to loge when an endpoint is configured."
  spec.homepage = "https://github.com/jtarchie/loge"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}/tree/main/ruby"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["lib/**/*.rb"] + %w[LICENSE README.md]
  spec.require_paths = ["lib"]

  # logger is stdlib, but it is a bundled gem since Ruby 4.0 and so must be
  # declared for apps running under bundler.
  spec.add_dependency "logger", "~> 1.6"
end
