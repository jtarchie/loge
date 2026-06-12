# frozen_string_literal: true

RSpec.describe Loge::Logger do
  # FakeClient records pushes so logger behavior is tested without threads or
  # HTTP.
  class FakeClient
    Push = Struct.new(:line, :labels, :timestamp_ns, keyword_init: true)

    attr_reader :pushes

    def initialize
      @pushes = []
    end

    def push(line, labels: {}, timestamp_ns: nil)
      @pushes << Push.new(line: line, labels: labels, timestamp_ns: timestamp_ns)
      nil
    end

    def flush(timeout: nil) = self

    def close(timeout: nil) = nil
  end

  let(:client) { FakeClient.new }

  subject(:logger) { described_class.new(client) }

  it "labels each record with its severity and timestamps it in nanoseconds" do
    logger.info("hello")

    push = client.pushes.fetch(0)
    expect(push.labels).to eq("level" => "INFO")
    expect(JSON.parse(push.line)).to eq("msg" => "hello")
    expect(push.timestamp_ns).to be_within(60 * 1_000_000_000).of(Time.now.to_i * 1_000_000_000)
  end

  it "drops records below the configured level" do
    logger.level = ::Logger::WARN

    logger.info("quiet")
    logger.warn("loud")

    expect(client.pushes.map(&:labels)).to eq([{"level" => "WARN"}])
  end

  it "evaluates message blocks lazily and keeps their progname" do
    logger.level = ::Logger::WARN
    evaluated = false

    logger.info("job") { evaluated = true }
    expect(evaluated).to be(false)

    logger.error("job") { "boom" }
    expect(JSON.parse(client.pushes.fetch(0).line)).to eq("msg" => "boom", "progname" => "job")
  end

  it "uses the default progname when logging plain messages" do
    logger = described_class.new(client, progname: "worker")

    logger.info("hi")

    expect(JSON.parse(client.pushes.fetch(0).line)).to eq("msg" => "hi", "progname" => "worker")
  end

  it "merges hash messages into the line" do
    logger.info(msg: "login", user_id: "abc123")

    expect(JSON.parse(client.pushes.fetch(0).line)).to eq("msg" => "login", "user_id" => "abc123")
  end

  it "pushes << messages raw, without labels or filtering" do
    logger.level = ::Logger::FATAL

    logger << "raw line"

    push = client.pushes.fetch(0)
    expect(push.line).to eq("raw line")
    expect(push.labels).to eq({})
  end

  it "omits the level label when disabled" do
    logger = described_class.new(client, level_label: nil)

    logger.info("hello")

    expect(client.pushes.fetch(0).labels).to eq({})
  end

  it "renders through a custom formatter" do
    logger = described_class.new(client, formatter: ->(severity, _time, _progname, msg) { "#{severity}: #{msg}" })

    logger.warn("watch out")

    expect(client.pushes.fetch(0).line).to eq("WARN: watch out")
  end

  it "delegates flush and close to the client" do
    expect(client).to receive(:flush).with(timeout: nil)
    expect(client).to receive(:close)

    logger.flush
    logger.close
  end
end
