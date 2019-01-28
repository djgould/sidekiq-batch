require_relative 'extension/worker'

module Sidekiq
  class Batcher
    module Middleware
      class ClientMiddleware
        def call(_worker, msg, _queue, _redis_pool = nil)
          if (batch = Thread.current[:batch])
            batch.increment_job_queue(msg['jid']) if (msg[:bid] = batch.bid)
          end
          yield
        end
      end

      class ServerMiddleware
        def call(_worker, msg, _queue)
          if (bid = msg['bid'])
            begin
              Thread.current[:batch] = Sidekiq::Batcher.new(bid)
              yield
              Thread.current[:batch] = nil
              Batcher.process_successful_job(bid, msg['jid'])
            rescue
              Batcher.process_failed_job(bid, msg['jid'])
              raise
            ensure
              Thread.current[:batch] = nil
            end
          else
            yield
          end
        end
      end

      def self.configure
        Sidekiq.configure_client do |config|
          config.client_middleware do |chain|
            chain.add Sidekiq::Batcher::Middleware::ClientMiddleware
          end
        end
        Sidekiq.configure_server do |config|
          config.client_middleware do |chain|
            chain.add Sidekiq::Batcher::Middleware::ClientMiddleware
          end
          config.server_middleware do |chain|
            chain.add Sidekiq::Batcher::Middleware::ServerMiddleware
          end
        end
        Sidekiq::Worker.send(:include, Sidekiq::Batcher::Extension::Worker)
      end
    end
  end
end

Sidekiq::Batcher::Middleware.configure
