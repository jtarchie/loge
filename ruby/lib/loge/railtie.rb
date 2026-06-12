# frozen_string_literal: true

require "rails/railtie"

module Loge
  # Railtie broadcasts Rails.logger to a Loge::Logger when an endpoint is
  # configured, leaving the app's existing logging untouched. It is a no-op
  # unless config.loge.endpoint or the LOGE_ENDPOINT environment variable is
  # set, and can be disabled outright with config.loge.enabled = false.
  #
  #   # config/environments/production.rb
  #   config.loge.endpoint = "http://loge.internal:3000"
  #   config.loge.labels = { "app" => "checkout" }   # defaults: app + env
  #   config.loge.level = :info                       # defaults to Rails.logger.level
  class Railtie < ::Rails::Railtie
    CLIENT_OPTIONS = %i[
      path batch_size flush_interval queue_capacity bearer_token max_retries
      open_timeout read_timeout block_on_full on_error
    ].freeze

    config.loge = ActiveSupport::OrderedOptions.new

    initializer "loge.logger", after: :initialize_logger do |app|
      Loge::Railtie.attach(app)
    end

    def self.attach(app)
      options = app.config.loge
      endpoint = options.endpoint || ENV.fetch("LOGE_ENDPOINT", nil)
      return if options.enabled == false || endpoint.to_s.empty?

      logger = Loge.logger(
        endpoint: endpoint,
        labels: default_labels(app).merge((options.labels || {}).transform_keys(&:to_s)),
        level: options.level || ::Rails.logger&.level || ::Logger::INFO,
        **options.slice(*CLIENT_OPTIONS).compact,
      )

      broadcast(::Rails.logger, logger)
      at_exit { logger.close }
      logger
    end

    def self.default_labels(app)
      labels = {"env" => ::Rails.env.to_s}
      name = app.class.name&.deconstantize
      labels["app"] = name.underscore unless name.nil? || name.empty?
      labels
    end

    def self.broadcast(current, loge_logger)
      if current.respond_to?(:broadcast_to)
        current.broadcast_to(loge_logger) # Rails >= 7.1 BroadcastLogger
      elsif current && ActiveSupport::Logger.respond_to?(:broadcast)
        current.extend(ActiveSupport::Logger.broadcast(loge_logger)) # Rails < 7.1
      else
        ::Rails.logger = loge_logger
      end
    end
  end
end
