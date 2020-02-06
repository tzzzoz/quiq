# frozen_string_literal: true

require 'singleton'
require 'async/redis'
require_relative 'worker'
require_relative 'scheduler'

module Quiq
  class Server
    include Singleton

    def run
      # Launch one worker per queue
      Quiq.queues.each do |q|
        fork { Worker.new("queue:#{q}").start }
      end

      # Launch scheduler for jobs to be performed at certain time
      fork { Scheduler.new.start }

      # TODO: handle graceful shutdowns
      Process.waitall
    end
  end
end
