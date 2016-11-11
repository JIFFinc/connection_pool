require 'thread'
require 'timeout'
require_relative 'monotonic_time'

##
# Raised when you attempt to retrieve a connection from a pool that has been
# shut down.

class ConnectionPool::PoolShuttingDownError < RuntimeError; end

##
# The TimedStack manages a pool of homogeneous connections (or any resource
# you wish to manage).  Connections are created lazily up to a given maximum
# number. Expired connections are shutdown automatically every 5 seconds.

# Examples:
#
#    ts = TimedStack.new(min: 1, max: 1, max_age: 3600) { MyConnection.new }
#
#    # fetch a connection
#    conn = ts.pop
#
#    # return a connection
#    ts.push conn
#
#    conn = ts.pop
#    ts.pop timeout: 5
#    #=> raises Timeout::Error after 5 seconds

class ConnectionPool::TimedStack

  class Connection
    attr_accessor :created_at, :connection
    def initialize(connection, created_at: nil)
      @created_at = created_at || Time.now
      @connection = connection
    end

    def shutdown(&block)
      block.call(connection) if block_given?
    rescue
      # the shutdown block should handle its own exceptions so it should be safe to ignore any uncaught exceptions
    end
  end

  ##
  # Creates a new pool with +min+ connections that are created from the given
  # +block+.

  def initialize(min: 0, max: 0, max_age: 0, shutdown: nil, cleanup_frequency: 5, &block)
    raise ArgumentError, 'min is greater than max' if min > max
    @create_block = block
    @created = 0
    @que = []
    @min = min
    @max = max
    @max_age = max_age
    @mutex = Mutex.new
    @resource = ConditionVariable.new
    @shutdown_block = shutdown
    @shutting_down = false
    @cleanup_frequency = cleanup_frequency

    @thread = nil
    if @max_age > 0
      @thread = Thread.new do
        cleanup_loop
      end
      @thread.run
    end

    # setup min connections
    @min.times do
      conn = try_create()
      store_connection(conn)
    end
  end

  ##
  # Returns +obj+ to the stack.  +options+ is ignored in TimedStack but may be
  # used by subclasses that extend TimedStack.

  def push(obj, options = {})
    @mutex.synchronize do
      if @shutting_down
        obj.shutdown(&@shutdown_block)
      else
        store_connection obj, options
      end

      @resource.broadcast
    end
  end
  alias_method :<<, :push

  ##
  # Retrieves a connection from the stack.  If a connection is available it is
  # immediately returned.  If no connection is available within the given
  # timeout a Timeout::Error is raised.
  #
  # +:timeout+ is the only checked entry in +options+ and is preferred over
  # the +timeout+ argument (which will be removed in a future release).  Other
  # options may be used by subclasses that extend TimedStack.

  def pop(timeout = 0.5, options = {})
    options, timeout = timeout, 0.5 if Hash === timeout
    timeout = options.fetch :timeout, timeout

    deadline = ConnectionPool.monotonic_time + timeout
    @mutex.synchronize do
      loop do
        raise ConnectionPool::PoolShuttingDownError if @shutting_down
        # Connections may have expired so this may return nil
        connection = fetch_connection(options) if connection_stored?(options)

        connection ||= try_create(options)
        return connection if connection

        to_wait = deadline - ConnectionPool.monotonic_time
        raise Timeout::Error, "Waited #{timeout} sec" if to_wait <= 0
        @resource.wait(@mutex, to_wait)
      end
    end
  end

  ##
  # Shuts down the TimedStack which prevents connections from being checked
  # out.

  def shutdown(options={},&block)
    @shutdown_block = block if block_given?

    # prevent push, pop and stop the cleanup_loop
    @mutex.synchronize do
      return if @shutting_down
      @shutting_down = true
      @resource.broadcast
    end
    @thread.join if @thread

    # finish shutting down connections
    @mutex.synchronize do
      while connection_stored?(options)
        conn = fetch_connection(options)
        if conn
          conn.shutdown(&@shutdown_block)
        end
      end
    end
  end

  ##
  # Returns +true+ if there are no available connections.

  def empty?
    (@created - @que.length) >= @max
  end

  ##
  # The number of connections available on the stack.

  def length
    @max - @created + @que.length
  end

  private

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must returns true if a connection is available on the stack.

  def connection_stored?(options = nil)
    !@que.empty?
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method reaps expired connections and returns a valid connection from the stack.
  # Returns nil when there are no more valid connections

  def fetch_connection(options = nil)
    while conn = @que.pop do
      # check for expired connections
      if @max_age > 0 && conn.created_at < Time.now - @max_age
        @created -= 1
        conn.shutdown(&@shutdown_block)
      else
        return conn
      end
    end
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must return +obj+ to the stack.

  def store_connection(obj, options = nil)
    @que.push obj
  end

  ##
  # This is an extension point for TimedStack and is called with a mutex.
  #
  # This method must create a connection if and only if the total number of
  # connections allowed has not been met.

  def try_create(options = nil)
    unless @created == @max
      object = Connection.new(@create_block.call)
      @created += 1
      object
    end
  end

  ##
  # Cleans up expired connections periodically

  def cleanup_loop
    # Connections expire on their own when fetched. Periodically fetch connections to ensure they don't get stale.
    # Over time this will clear out expired connections.
    next_cleanup = Time.now + @cleanup_frequency
    @mutex.synchronize do
      while !@shutting_down do
        time = Time.now
        if time >= next_cleanup
          conn = fetch_connection
          if conn
            store_connection(conn)
          end

          # ensure we have not dropped below the minimum
          if @created < @min
            store_connection(try_create)
          end

          next_cleanup = time + @cleanup_frequency
        end

        # This wait will return faster than we want, but that is desired so the thread can exit immediately during shutdown
        @resource.wait(@mutex, next_cleanup - time)
      end
    end

  end
end
