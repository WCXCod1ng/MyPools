package redis_pool

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ===== Mock Setup =====

// openConnections 用于追踪当前“打开”的连接总数
var openConnections int32

// mockConn 模拟一个 redis.Conn
type mockConn struct {
	err     error
	isClose bool
}

func (m *mockConn) Close() error {
	if m.isClose {
		return errors.New("already closed")
	}
	m.isClose = true
	return nil
}

func (m *mockConn) Err() error {
	return m.err
}

func (m *mockConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	if m.isClose {
		return nil, errors.New("connection is closed")
	}
	return "OK", nil
}

func (m *mockConn) Send(commandName string, args ...interface{}) error {
	if m.isClose {
		return errors.New("connection is closed")
	}
	return nil
}

func (m *mockConn) Flush() error {
	if m.isClose {
		return errors.New("connection is closed")
	}
	return nil
}

func (m *mockConn) Receive() (reply interface{}, err error) {
	if m.isClose {
		return nil, errors.New("connection is closed")
	}
	return "OK", nil
}

// factory 创建一个新的 mockConn
var factory = func() (redis.Conn, error) {
	time.Sleep(time.Duration(rand.IntN(100)+50) * time.Millisecond)
	return &mockConn{}, nil
}

// errorFactory 创建一个失败的 mockConn
var errorFactory = func() (redis.Conn, error) {
	time.Sleep(time.Duration(rand.IntN(150)+60) * time.Millisecond)
	return nil, errors.New("factory failed to create connection")
}

// ===== Functionality Tests =====

func TestNewRedisPool(t *testing.T) {
	// 测试有效参数
	pool, err := NewRedisPool(10, 5, factory)
	if err != nil {
		t.Fatalf("Failed to create pool with valid args: %v", err)
	}
	if pool.maxActive != 10 {
		t.Errorf("Expected maxActive 10, got %d", pool.maxActive)
	}
	if cap(pool.idleQueue) != 5 {
		t.Errorf("Expected idleQueue capacity 5, got %d", cap(pool.idleQueue))
	}
	if len(pool.idleQueue) != 5 {
		t.Errorf("Expected 5 initial connections, got %d", len(pool.idleQueue))
	}

	// 测试无效参数
	invalidArgs := [][]int{
		{0, 5},  // maxActive <= 0
		{10, 0}, // maxIdle <= 0
		{5, 10}, // maxActive < maxIdle
	}
	for _, args := range invalidArgs {
		_, err := NewRedisPool(args[0], args[1], factory)
		if err == nil {
			t.Errorf("Expected error for args maxActive=%d, maxIdle=%d, but got nil", args[0], args[1])
		}
	}
	_, err = NewRedisPool(10, 5, nil)
	if err == nil {
		t.Errorf("Expected error for nil factory, but got nil")
	}
}

func TestPool_GetAndRelease(t *testing.T) {
	pool, _ := NewRedisPool(10, 5, factory)
	defer pool.Close()

	// 1. 从池中获取一个连接
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get a connection: %v", err)
	}
	if len(pool.idleQueue) != 4 {
		t.Errorf("Expected idle queue size 4 after Get, got %d", len(pool.idleQueue))
	}

	// 2. 释放连接
	err = pool.Release(conn)
	if err != nil {
		t.Fatalf("Failed to release a connection: %v", err)
	}
	if len(pool.idleQueue) != 5 {
		t.Errorf("Expected idle queue size 5 after Release, got %d", len(pool.idleQueue))
	}
}

func TestPool_ExceedIdleCreatesNew(t *testing.T) {
	pool, _ := NewRedisPool(10, 2, factory)
	defer pool.Close()

	conns := make([]redis.Conn, 3)
	// 获取3个连接，超过了 idle (2)
	for i := 0; i < 3; i++ {
		c, err := pool.Get()
		if err != nil {
			t.Fatalf("Failed to get connection %d: %v", i+1, err)
		}
		conns[i] = c
	}

	if pool.currentSize != 3 {
		t.Errorf("Expected currentSize to be 3, got %d", pool.currentSize)
	}
}

func TestPool_ReleaseExceedsIdleClosesConn(t *testing.T) {
	pool, _ := NewRedisPool(5, 2, factory)
	defer pool.Close()

	conns := make([]redis.Conn, 3)
	for i := 0; i < 3; i++ {
		conns[i], _ = pool.Get()
	}

	// 释放所有连接
	for _, c := range conns {
		pool.Release(c)
	}

	if len(pool.idleQueue) != 2 {
		t.Errorf("Expected idle queue size to be capped at 2, got %d", len(pool.idleQueue))
	}
	if pool.currentSize != 2 {
		t.Errorf("Expected currentSize to shrink to 2, got %d", pool.currentSize)
	}
}

func TestPool_ReleaseBrokenConn(t *testing.T) {
	pool, _ := NewRedisPool(5, 5, factory)
	defer pool.Close()

	// 获取一个连接并模拟它已损坏
	conn, _ := pool.Get()
	conn.(*mockConn).err = errors.New("connection broken")

	pool.Release(conn)

	if pool.currentSize != 4 {
		t.Errorf("Expected currentSize to be 4 after releasing broken conn, got %d", pool.currentSize)
	}
	if len(pool.idleQueue) != 4 {
		t.Errorf("Expected idle queue size to be 4 after releasing broken conn, got %d", len(pool.idleQueue))
	}
}

func TestPool_GetBlocksWhenFull(t *testing.T) {
	// 这个测试会验证原始代码的死锁问题
	// 在修正后的代码上，它应该能正确地阻塞并最终成功
	pool, _ := NewRedisPool(2, 1, factory)
	defer pool.Close()

	// 占满所有连接
	c1, _ := pool.Get()
	c2, _ := pool.Get()

	done := make(chan bool)
	go func() {
		// 这个Get应该会阻塞
		c3, err := pool.Get()
		if err != nil {
			t.Errorf("Blocked Get failed: %v", err)
		}
		pool.Release(c3)
		done <- true
	}()

	// 等待一小段时间确保goroutine已阻塞
	time.Sleep(100 * time.Millisecond)

	// 释放一个连接，让阻塞的goroutine可以继续
	pool.Release(c1)

	select {
	case <-done:
		// 测试成功
	case <-time.After(1 * time.Second):
		t.Fatal("Get call did not unblock after a connection was released. Possible deadlock!")
	}
	pool.Release(c2)
}

func TestPool_Close(t *testing.T) {
	pool, _ := NewRedisPool(5, 2, factory)
	conn, _ := pool.Get()

	pool.Close()

	if !pool.closed {
		t.Error("Expected pool to be marked as closed")
	}

	_, err := pool.Get()
	if err != RedisPoolCloseErr {
		t.Errorf("Expected RedisPoolCloseErr on Get from closed pool, got %v", err)
	}

	err = pool.Release(conn)
	if err != RedisPoolCloseErr {
		t.Errorf("Expected RedisPoolCloseErr on Release to closed pool, got %v", err)
	}
}

// ===== Concurrency Test =====

func TestPool_Concurrency(t *testing.T) {
	pool, _ := NewRedisPool(20, 10, factory)
	defer pool.Close()

	var wg sync.WaitGroup
	numGoroutines := 100
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			conn, err := pool.Get()
			if err != nil {
				// 在并发测试中，我们不希望 t.Fatal，而是记录错误
				fmt.Printf("Error getting connection: %v\n", err)
				return
			}
			// 模拟工作
			time.Sleep(time.Duration(10+i%10) * time.Millisecond)
			pool.Release(conn)
		}()
	}

	wg.Wait()

	if pool.currentSize > pool.maxActive {
		t.Errorf("currentSize %d exceeded maxActive %d", pool.currentSize, pool.maxActive)
	}
	if len(pool.idleQueue) > cap(pool.idleQueue) {
		t.Errorf("idleQueue size %d exceeded capacity %d", len(pool.idleQueue), cap(pool.idleQueue))
	}
}

// ===== Performance Benchmarks =====

func BenchmarkPool_GetRelease(b *testing.B) {
	pool, _ := NewRedisPool(50, 50, factory)
	defer pool.Close()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		conn, _ := pool.Get()
		pool.Release(conn)
	}
}

func BenchmarkPool_ConcurrentGetRelease(b *testing.B) {
	pool, _ := NewRedisPool(50, 50, factory)
	defer pool.Close()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get()
			if err != nil {
				b.Fatal(err)
			}
			if err := pool.Release(conn); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestNewRedisPool_FactoryError 验证初始化时如果factory失败，不会泄露连接
func TestNewRedisPool_FactoryError(t *testing.T) {
	// 重置计数器
	atomic.StoreInt32(&openConnections, 0)

	// 这个factory会在第3次调用时失败
	var createCount int
	failingFactory := func() (redis.Conn, error) {
		createCount++
		if createCount > 2 {
			return nil, errors.New("failed on purpose")
		}
		return factory() // 调用我们正常的factory
	}

	// 尝试创建一个有5个空闲连接的池，但它应该会在创建第3个时失败
	_, err := NewRedisPool(10, 5, failingFactory)

	if err == nil {
		t.Fatal("Expected an error during pool creation, but got nil")
	}

	// 最关键的断言：检查是否所有的连接都被清理了
	// 等待一小段时间确保Close()有时间执行
	time.Sleep(50 * time.Millisecond)
	if open := atomic.LoadInt32(&openConnections); open != 0 {
		t.Errorf("Expected 0 open connections after creation failure, but found %d. Resources were leaked!", open)
	}
}

// TestPool_GetWhileClosing 验证当Get阻塞时，如果池被关闭，Get应该返回错误而不是nil连接
func TestPool_GetWhileClosing(t *testing.T) {
	pool, err := NewRedisPool(1, 1, factory)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// 1. 先取走唯一的一个连接，让池子变空
	conn1, err := pool.Get()
	if err != nil {
		t.Fatalf("Failed to get the first connection: %v", err)
	}

	resultChan := make(chan error)

	// 2. 启动一个goroutine，它会因为池子满了而阻塞在Get()
	go func() {
		// 这个Get()应该会阻塞，直到Close()被调用
		_, err := pool.Get()
		resultChan <- err
	}()

	// 3. 等待一小会，确保goroutine已经开始并阻塞了
	time.Sleep(100 * time.Millisecond)

	// 4. 关闭连接池，这应该会唤醒阻塞的Get()
	pool.Close()

	// 5. 检查阻塞的Get()返回的错误
	select {
	case errFromGet := <-resultChan:
		if errFromGet != RedisPoolCloseErr {
			t.Errorf("Expected RedisPoolCloseErr when getting from a closing pool, but got: %v", errFromGet)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("The blocked Get call did not return after the pool was closed.")
	}

	// 清理工作
	pool.Release(conn1)
}

// TestPool_GetWithFailingFactory 验证当需要创建新连接但factory失败时，池的状态是正确的
func TestPool_GetWithFailingFactory(t *testing.T) {
	var createCount int
	// 这个factory只允许成功创建一次
	factoryOnce := func() (redis.Conn, error) {
		if createCount > 0 {
			return nil, errors.New("factory can only be called once")
		}
		createCount++
		return factory()
	}

	// maxIdle=0, 这样每次Get都需要创建新连接
	pool, err := NewRedisPool(5, 1, factoryOnce)
	if err != nil {
		t.Fatal(err)
	}

	// 第一次Get，使用预创建的连接
	c, err := pool.Get()
	if err != nil {
		t.Fatalf("First Get failed: %v", err)
	}
	pool.Release(c)

	// currentSize 应该为1
	if pool.currentSize != 1 {
		t.Fatalf("Expected currentSize to be 1, but got %d", pool.currentSize)
	}

	// 现在池是满的，我们占满它
	conns := []redis.Conn{}
	c, _ = pool.Get()
	conns = append(conns, c)

	// 第二次Get，会尝试创建新连接，但factory会失败
	_, err = pool.Get()
	if err == nil {
		t.Fatal("Expected error from Get when factory fails, but got nil")
	}

	// 关键断言：即使创建失败，currentSize也不应该被错误地增加
	if pool.currentSize != 1 {
		t.Errorf("Expected currentSize to remain 1 after a failed factory call, but got %d", pool.currentSize)
	}
}
