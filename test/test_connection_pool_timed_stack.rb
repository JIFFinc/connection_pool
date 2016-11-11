require_relative 'helper'

class TestConnectionPoolTimedStack < Minitest::Test

  def setup
    @stack = ConnectionPool::TimedStack.new { Object.new }
  end

  def test_empty_eh
    stack = ConnectionPool::TimedStack.new(min:0 ,max:1) { Object.new }

    refute_empty stack

    popped = stack.pop

    assert_empty stack

    stack.push popped

    refute_empty stack
  end

  def test_length
    stack = ConnectionPool::TimedStack.new(min:1 ,max:1) { Object.new }

    assert_equal 1, stack.length

    popped = stack.pop

    assert_equal 0, stack.length

    stack.push popped

    assert_equal 1, stack.length
  end

  def test_object_creation_fails
    stack = ConnectionPool::TimedStack.new(min:0, max:2) { raise 'failure' }

    begin
      stack.pop
    rescue => error
      assert_equal 'failure', error.message
    end

    begin
      stack.pop
    rescue => error
      assert_equal 'failure', error.message
    end

    refute_empty stack
    assert_equal 2, stack.length
  end

  def test_pop
    object = Object.new
    @stack.push object

    popped = @stack.pop

    assert_same object, popped
  end

  def test_pop_empty
    e = assert_raises Timeout::Error do
      @stack.pop timeout: 0
    end

    assert_equal 'Waited 0 sec', e.message
  end

  def test_pop_empty_2_0_compatibility
    e = assert_raises Timeout::Error do
      @stack.pop 0
    end

    assert_equal 'Waited 0 sec', e.message
  end

  def test_pop_full
    stack = ConnectionPool::TimedStack.new(min:1, max:1) { Object.new }

    popped = stack.pop

    refute_nil popped
    assert_empty stack
  end

  def test_pop_wait
    thread = Thread.start do
      @stack.pop
    end

    Thread.pass while thread.status == 'run'

    object = Object.new

    @stack.push object

    assert_same object, thread.value
  end

  def test_pop_shutdown
    @stack.shutdown { }

    assert_raises ConnectionPool::PoolShuttingDownError do
      @stack.pop
    end
  end

  def test_push
    stack = ConnectionPool::TimedStack.new(min:1, max:1) { Object.new }

    conn = stack.pop

    stack.push conn

    refute_empty stack
  end

  def test_push_shutdown
    called = []

    @stack.shutdown do |object|
      called << object
    end

    @stack.push ConnectionPool::TimedStack::Connection.new(Object.new)

    refute_empty called
    assert_empty @stack
  end

  def test_shutdown
    @stack.push ConnectionPool::TimedStack::Connection.new(Object.new)

    called = []

    @stack.shutdown do |object|
      called << object
    end

    refute_empty called
    assert_empty @stack
  end

  def test_initialized_shutdown
    called = []
    shutdown_block = lambda do |object|
      called << object
    end

    stack = ConnectionPool::TimedStack.new(shutdown: shutdown_block) { Object.new }

    stack.push ConnectionPool::TimedStack::Connection.new(Object.new)

    stack.shutdown

    refute_empty called
    assert_empty stack
  end

  def test_reap_expired_connection
    stack = ConnectionPool::TimedStack.new(min: 1, max: 1, max_age: 1) { 'valid' }

    stack.push ConnectionPool::TimedStack::Connection.new('expired', created_at: Time.now - 2)

    conn = stack.pop

    assert_equal conn.connection, 'valid'

    stack.shutdown
  end

  def test_not_expired_connection
    stack = ConnectionPool::TimedStack.new(min: 1, max: 1, max_age: 60) { 'invalid' }

    stack.push ConnectionPool::TimedStack::Connection.new('valid')

    conn = stack.pop

    assert_equal conn.connection, 'valid'

    stack.shutdown
  end

  def test_cleanup_loop
    called = []

    t = 0.0001
    stack = ConnectionPool::TimedStack.new(cleanup_frequency: t, max_age: t, max: 3, shutdown: lambda{|c| called << c }) { Object.new }

    conns = []

    # coerce connections in the queue
    3.times do
      conns << stack.pop
    end
    conns.each do |conn|
      stack.push(conn)
    end

    # sleep just long enough for cleanup_loop to reap the connection
    sleep(t*3)

    assert_equal 3, called.size

    stack.shutdown
  end

  def test_shutdown_expired_on_fetch
    called = []

    t = 0.0001
    stack = ConnectionPool::TimedStack.new(cleanup_frequency: t, max_age: t, max: 1, shutdown: lambda{|c| called << c }) { Object.new }

    # coerce 1 item in the queue
    conn = stack.pop
    stack.push(conn)

    sleep(t)
    # one more pop will trigger a shutdown
    stack.pop

    assert_equal 1, called.size

    stack.shutdown
  end

end

