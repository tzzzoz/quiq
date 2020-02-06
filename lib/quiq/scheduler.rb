# frozen_string_literal: true

require_relative 'processor'

module Quiq
  class Scheduler
    SET_NAME = 'scheduled'

    def start
      Async do
        loop do
          result = Quiq.redis.zrange(SET_NAME, 0, 0, withscores: true)
          payload, scheduled_at = result

          if payload && scheduled_at.to_f < Time.now.to_f
            job = JSON.parse(payload)

            #TODO: Imolement fine-grained locking

            pushed = Quiq.redis.lpush("queue:#{job['queue_name']}", payload)

            #TODO: Use proper logging when implemented
            next lpush_error_message(job['queue_name']) if pushed <= 0

            Quiq.redis.zrem(SET_NAME, payload)
          end
        end
      ensure
        Quiq.redis.close
      end
    end

    private

    def lpush_error_message(queue_name)
      puts "Could not lpush to the queue:#{queue_name}"
    end
  end
end
