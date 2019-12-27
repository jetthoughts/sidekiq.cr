require "./spec_helper"
require "../src/sidekiq/server"
require "../src/sidekiq/server/fetch"
require "../src/sidekiq/server/consumer"
require "../src/sidekiq/job"

describe "Sidekiq::Consumer" do
  it "maintains the processor list" do
    s = Sidekiq::Server.new

    job = Sidekiq::Job.new
    3.times do
      Sidekiq::BasicFetch::UnitOfWork.new("default", job.to_json, s).requeue
    end

    consumer = Sidekiq::Consumer.new(s.fetcher)

    consumer.start(s)

    puts "Expecting ..."

    work = consumer.retrieve_work(s)
    work.job.to_json.should contain "jid"
    work.requeue

    3.times { consumer.retrieve_work(s) }
  end
end
