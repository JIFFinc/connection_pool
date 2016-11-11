require_relative 'connection_pool/version'
require_relative 'connection_pool/timed_stack'


# Generic connection pool class for e.g. sharing a limited number of network connections
# among many threads.  Note: Connections are lazily created.
#
# Example usage with block (faster):
#
#    @pool = ConnectionPool.new { Redis.new }
#
#    @pool.with do |redis|
#      redis.lpop('my-list') if redis.llen('my-list') > 0
#    end
#
# Using optional timeout override (for that single invocation)
#
#    @pool.with(timeout: 2.0) do |redis|
#      redis.lpop('my-list') if redis.llen('my-list') > 0
#    end
#
# Example usage replacing an existing connection (slower):
#
#    $redis = ConnectionPool.wrap { Redis.new }
#
#    def do_work
#      $redis.lpop('my-list') if $redis.llen('my-list') > 0
#    end
#
# Accepts the following options:
# - :min - min number of connections to pool, defaults to 0
# - :max - max of connections to pool, defaults to 5
# - :size - overrides min/max and sets the min to 0 and max to this size
# - :timeout - amount of time to wait for a connection if none currently available, defaults to 5 seconds
# - :max_age - how long a connection is allow to remain active. Oldest connections are culled. If the connection pool
# drops below the minimum then a new connection is created.
#

class ConnectionPool
  DEFAULTS = {min: 0, max: 5, size: nil, timeout: 5, max_age: 3600}

  class Error < RuntimeError
  end

  def self.wrap(options, &block)
    Wrapper.new(options, &block)
  end

  def initialize(options = {}, &block)
    raise ArgumentError, 'Connection pool requires a block' unless block

    options = DEFAULTS.merge(options)

    @min = options.fetch(:min)
    @max = options.fetch(:max)
    if options.fetch(:size)
      @min = 0
      @max = options.fetch(:size)
    end
    @timeout = options.fetch(:timeout)
    @max_age = options.fetch(:max_age)

    @available = TimedStack.new(min: @min, max: @max, max_age: @max_age, shutdown: options.fetch(:shutdown, nil), &block)
    @key = :"current-#{@available.object_id}"
  end

if Thread.respond_to?(:handle_interrupt)

  # MRI
  def with(options = {})
    Thread.handle_interrupt(Exception => :never) do
      conn = checkout(options)
      begin
        Thread.handle_interrupt(Exception => :immediate) do
          yield conn
        end
      ensure
        checkin
      end
    end
  end

else

  # jruby 1.7.x
  def with(options = {})
    conn = checkout(options)
    begin
      yield conn
    ensure
      checkin
    end
  end

end

  def checkout(options = {})
    conn = if stack.empty?
      timeout = options[:timeout] || @timeout
      @available.pop(timeout: timeout)
    else
      stack.last
    end

    stack.push conn
    conn.connection
  end

  def checkin
    conn = pop_connection # mutates stack, must be on its own line
    @available.push(conn) if stack.empty?

    nil
  end

  def shutdown(&block)
    @available.shutdown(&block)
  end

  private

  def pop_connection
    if stack.empty?
      raise ConnectionPool::Error, 'no connections are checked out'
    else
      stack.pop
    end
  end

  def stack
    ::Thread.current[@key] ||= []
  end

  class Wrapper < ::BasicObject
    METHODS = [:with, :pool_shutdown]

    def initialize(options = {}, &block)
      @pool = ::ConnectionPool.new(options, &block)
    end

    def with(&block)
      @pool.with(&block)
    end

    def pool_shutdown(&block)
      @pool.shutdown(&block)
    end

    def respond_to?(id, *args)
      METHODS.include?(id) || with { |c| c.respond_to?(id, *args) }
    end

    def method_missing(name, *args, &block)
      with do |connection|
        connection.send(name, *args, &block)
      end
    end
  end
end
