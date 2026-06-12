# frozen_string_literal: true

require "rails"
require "active_support/logger"
require "active_support/broadcast_logger"
require "loge/railtie"

RSpec.describe Loge::Railtie do
  it "broadcasts Rails.logger to a loge logger when an endpoint is configured" do
    requests = []
    stub_request(:post, "http://loge.test:3000/api/v1/push")
      .with { |request| requests << JSON.parse(request.body) }
      .to_return(status: 200)

    app_class = Class.new(Rails::Application) do
      def self.name = "RailtieSpec::Application"
    end
    app = app_class.instance
    app.config.eager_load = false
    app.config.logger = ActiveSupport::BroadcastLogger.new(ActiveSupport::Logger.new(IO::NULL))
    app.config.loge.endpoint = "http://loge.test:3000"
    app.config.loge.labels = {"team" => "core"}
    app.config.loge.flush_interval = 60
    app.initialize!

    Rails.logger.info("hello from rails")

    loge_logger = Rails.logger.broadcasts.find { |logger| logger.is_a?(Loge::Logger) }
    expect(loge_logger).not_to be_nil
    loge_logger.flush

    stream = requests.fetch(0).fetch("streams").fetch(0)
    expect(stream["stream"]).to eq(
      "app" => "railtie_spec",
      "env" => "test",
      "team" => "core",
      "level" => "INFO",
    )
    expect(JSON.parse(stream["values"].fetch(0).fetch(1))).to eq("msg" => "hello from rails")
  end

  it "stays out of the way when no endpoint is configured" do
    config = ActiveSupport::OrderedOptions.new
    app = Struct.new(:config).new(Struct.new(:loge).new(config))

    expect(described_class.attach(app)).to be_nil
  end
end
