# frozen_string_literal: true

require 'json'
require 'uri'

module Quiq
  class Client
    def push(job, scheduled_at)
      serialized = JSON.dump(job.serialize)

      Async do
        if scheduled_at
          Quiq.redis.zadd(Quiq::Scheduler::SET_NAME, scheduled_at, serialized) if scheduled_at
        else
          Quiq.redis.lpush("queue:#{job.queue_name}", serialized)
        end
      end
    end

    def self.push(job, scheduled_at: nil)
      new.push(job, scheduled_at)
    end
  end
end
