# frozen_string_literal: true

RSpec.describe Loge::Client do
  let(:endpoint) { "http://loge.test:3000" }
  let(:push_url) { "#{endpoint}/api/v1/push" }
  let(:requests) { [] }

  def stub_push
    stub_request(:post, push_url)
      .with { |request| requests << JSON.parse(request.body) }
      .to_return(status: 200)
  end

  # flush_interval defaults high so only the behavior under test triggers
  # delivery.
  def build_client(**options)
    described_class.new(endpoint: endpoint, labels: {app: "web"}, flush_interval: 60, **options)
  end

  it "groups records sharing a label set into one stream" do
    stub_push
    client = build_client

    client.push("one", labels: {level: "INFO"}, timestamp_ns: 1)
    client.push("two", labels: {level: "INFO"}, timestamp_ns: 2)
    client.push("three", labels: {level: "ERROR"}, timestamp_ns: 3)
    client.flush
    client.close

    expect(requests.size).to eq(1)
    expect(requests.first.fetch("streams")).to contain_exactly(
      {"stream" => {"app" => "web", "level" => "INFO"}, "values" => [["1", "one"], ["2", "two"]]},
      {"stream" => {"app" => "web", "level" => "ERROR"}, "values" => [["3", "three"]]},
    )
  end

  it "sends the bearer token and content type" do
    stub_push
    client = build_client(bearer_token: "secret")

    client.push("hello")
    client.flush
    client.close

    expect(WebMock).to have_requested(:post, push_url)
      .with(headers: {"Authorization" => "Bearer secret", "Content-Type" => "application/json"})
  end

  it "delivers once the batch reaches batch_size without an explicit flush" do
    stub_push
    client = build_client(batch_size: 2)

    client.push("one", timestamp_ns: 1)
    client.push("two", timestamp_ns: 2)

    eventually { requests.size == 1 }
    expect(requests.first["streams"].first["values"]).to eq([["1", "one"], ["2", "two"]])
    client.close
  end

  it "delivers on the flush interval" do
    stub_push
    client = described_class.new(endpoint: endpoint, labels: {app: "web"}, flush_interval: 0.05)

    client.push("tick")

    eventually { requests.size == 1 }
    client.close
  end

  it "retries failed deliveries until one succeeds" do
    stub_request(:post, push_url).to_return({status: 500}, {status: 500}, {status: 200})
    errors = []
    client = build_client(max_retries: 2, on_error: ->(error) { errors << error })

    client.push("x")
    client.flush
    client.close

    expect(errors).to be_empty
    expect(WebMock).to have_requested(:post, push_url).times(3)
  end

  it "routes the failure to on_error once retries are exhausted" do
    stub_request(:post, push_url).to_return(status: 500)
    errors = []
    client = build_client(max_retries: 1, on_error: ->(error) { errors << error })

    client.push("x")
    client.flush
    client.close

    expect(errors.first.message).to include("500")
    expect(WebMock).to have_requested(:post, push_url).times(2)
  end

  it "drops records instead of blocking when the queue is full" do
    entered = Queue.new
    release = Queue.new
    stub_request(:post, push_url).to_return do |_request|
      entered.push(true)
      release.pop
      {status: 200}
    end

    errors = []
    client = build_client(batch_size: 1, queue_capacity: 1, on_error: ->(error) { errors << error })

    client.push("first") # picked up by the worker, which then blocks mid-request
    entered.pop
    client.push("second") # fills the queue
    client.push("third")  # queue full -> dropped

    expect(client.dropped).to eq(1)

    2.times { release.push(true) }
    client.close

    expect(errors.map(&:message)).to include(a_string_matching(/dropped 1 record/))
  end

  it "close is idempotent and flushes what was accepted" do
    stub_push
    client = build_client

    client.push("last words")
    client.close
    client.close

    expect(requests.size).to eq(1)
    expect(client.push("after close")).to be_nil
    expect(client.dropped).to eq(1)
  end

  it "rejects non-http endpoints" do
    expect { described_class.new(endpoint: "localhost:3000") }.to raise_error(ArgumentError, /http/)
  end
end
