# frozen_string_literal: true

require 'singleton'
require 'async/redis'
require_relative 'job_wrapper'

module Quiq
  class Server
    include Singleton

    def run
      Async do
        loop do
          data = Quiq.redis.brpop(Quiq.configuration.queue)
          JobWrapper.new(data.last).run
        end
      ensure
        Quiq.redis.close
      end
    end
  end
end
