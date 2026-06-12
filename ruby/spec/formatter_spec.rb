# frozen_string_literal: true

RSpec.describe Loge::Formatter do
  subject(:formatter) { described_class.new }

  it "wraps plain messages in a msg field, leaving severity and time out-of-band" do
    line = formatter.call("INFO", Time.now, nil, "user logged in")

    expect(JSON.parse(line)).to eq("msg" => "user logged in")
  end

  it "merges hash messages into the JSON root" do
    line = formatter.call("INFO", Time.now, nil, {msg: "login", user_id: "abc123"})

    expect(JSON.parse(line)).to eq("msg" => "login", "user_id" => "abc123")
  end

  it "serializes exceptions with their class and backtrace" do
    error = begin
      raise ArgumentError, "boom"
    rescue ArgumentError => raised
      raised
    end

    record = JSON.parse(formatter.call("ERROR", Time.now, nil, error))

    expect(record["msg"]).to eq("boom")
    expect(record["error"]).to eq("ArgumentError")
    expect(record["backtrace"].first).to include("formatter_spec")
  end

  it "includes the progname without clobbering an explicit one" do
    expect(JSON.parse(formatter.call("INFO", Time.now, "worker", "hi")))
      .to eq("msg" => "hi", "progname" => "worker")
    expect(JSON.parse(formatter.call("INFO", Time.now, "worker", {progname: "custom"})))
      .to eq("progname" => "custom")
  end
end
