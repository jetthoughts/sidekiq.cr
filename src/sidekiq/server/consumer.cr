require "random/secure"
require "./util"
require "./fetch"

module Sidekiq
  class Consumer
    include Util

    class UnitOfWork < ::Sidekiq::UnitOfWork
      def initialize(@queue_name : String, @job : String)
      end

      def job
        @job
      end

      def acknowledge
        # nothing to do
      end

      def queue_name
        @queue_name
      end

      def requeue
      end
    end

    # class UnitOfWork < ::Sidekiq::UnitOfWork
    #   def initialize(@consumer : Sidekiq::Consumer, @work : ::Sidekiq::UnitOfWork)
    #   end

    #   def job
    #     @work.job
    #   end

    #   def acknowledge
    #     # nothing to do
    #   end

    #   def queue_name
    #     @work.queue_name
    #   end

    #   def requeue
    #     @consumer.requeue(self)
    #   end
    # end

    def initialize(@fetcher : ::Sidekiq::Fetch)
      @works_channel = Channel(Tuple(String, String)).new
      # @returned_works_channel = Channel(Sidekiq::UnitOfWork).new
    end

    def start(mgr)
      20.times do
        safe_routine(mgr, "consumer_reader") do
          while true
            work = @fetcher.retrieve_work(mgr)
            @works_channel.send({work.queue_name, work.job}) if work
          end
        end
      end

      # safe_routine(mgr, "consumer_writer") do
      #   while true
      #     work = @returned_works_channel.receive
      #     mgr.pool.redis do |conn|
      #       conn.rpush("queue:#{work.queue_name}", work.job)
      #     end
      #   end
      # end
    end

    def retrieve_work(ctx) : ::Sidekiq::UnitOfWork
      work = @works_channel.receive
      UnitOfWork.new(work[0], work[1])
    end

    def requeue(work)
      @returned_works_channel.send(work)
    end

    def bulk_requeue(ctx, inprogress : Array(Sidekiq::UnitOfWork))
      @fetcher.bulk_requeue(ctx, inprogress)
    end
  end
end
