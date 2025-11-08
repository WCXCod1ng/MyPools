package redis_pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ===== Mock Setup (与之前类似，需要全局计数器) =====
var openConnections int32

type mockConn struct {
	id      int
	isClose bool
	err     error
}

func (m *mockConn) Close() error {
	if m.isClose {
		return errors.New("already closed")
	}
	m.isClose = true
	atomic.AddInt32(&openConnections, -1)
	return nil
}
func (m *mockConn) Err() error { return m.err }
func (m *mockConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return "OK", nil
}
func (m *mockConn) Send(commandName string, args ...interface{}) error { return nil }
func (m *mockConn) Flush() error                                       { return nil }
func (m *mockConn) Receive() (reply interface{}, err error)            { return "OK", nil }

var connCounter int32

var factory = func() (redis.Conn, error) {
	atomic.AddInt32(&openConnections, 1)
	return &mockConn{id: int(atomic.AddInt32(&connCounter, 1))}, nil
}

var testOnBorrow = func(conn redis.Conn, ctx context.Context, lastUsed time.Time) error {
	return nil
}

// ===== Bug & Functionality Tests =====

// TestGet_MutexUnlock a simple test to check for the most obvious deadlock.
// Run this with `go test -race`
func TestGet_MutexUnlock(t *testing.T) {
	pool, err := NewRedisPool(2, 2, 0, factory, testOnBorrow, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Get and release, this should work
	c1, _ := pool.Get()
	pool.Release(c1)

	// Get again. If Bug #1 exists, this second Get will hang.
	done := make(chan struct{})
	go func() {
		c2, err := pool.Get()
		if err == nil {
			pool.Release(c2)
		}
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("Get() call deadlocked, mutex was likely not unlocked.")
	}
}

// TestIdleTimeout_RemovesOldestConn directly targets Bug #3
func TestIdleTimeout_RemovesOldestConn(t *testing.T) {
	atomic.StoreInt32(&openConnections, 0)
	idleTimeout := 100 * time.Millisecond
	pool, err := NewRedisPool(5, 5, idleTimeout, factory, testOnBorrow, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Initially, pool should have 5 connections
	if atomic.LoadInt32(&openConnections) != 5 {
		t.Fatalf("Expected 5 open connections, got %d", atomic.LoadInt32(&openConnections))
	}

	// Wait for longer than the timeout
	time.Sleep(idleTimeout + 50*time.Millisecond)

	// Trigger the lazy check by getting a connection
	// If the fix is correct, this should clean up all 5 old connections and create a new one.
	conn, err := pool.Get()
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer pool.Release(conn)

	// After cleanup and getting one, the total active connections should be 1.
	// In your original code, it would be 5 because nothing was cleaned.
	// With a correct implementation: 5 old are closed, 1 new is created.
	if pool.active != 1 {
		t.Errorf("Expected active connections to be 1 after idle cleanup, got %d", pool.active)
	}
	if atomic.LoadInt32(&openConnections) != 1 {
		t.Errorf("Expected total open connections to be 1, got %d", atomic.LoadInt32(&openConnections))
	}
}

// TestSemaphore_BlocksAndUnblocks tests if the semaphore mechanism (Bug #2) works.
// On the broken code, this will hang forever.
func TestSemaphore_BlocksAndUnblocks(t *testing.T) {
	// Use maxActive=1 to easily test the blocking mechanism
	pool, err := NewRedisPool(1, 1, 0, factory, testOnBorrow, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Get the only connection
	c1, _ := pool.Get()

	// This Get should block.
	done := make(chan struct{})
	go func() {
		c2, _ := pool.Get() // This will block
		pool.Release(c2)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond) // Give the goroutine time to block

	// Now release the first connection. This should unblock the second Get.
	pool.Release(c1)

	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("Get() did not unblock after Release(). The semaphore/channel mechanism is likely broken.")
	}
}

// TestRelease_HonorsMaxIdle tests Bug #4
func TestRelease_HonorsMaxIdle(t *testing.T) {
	pool, err := NewRedisPool(5, 2, 0, factory, testOnBorrow, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Get 3 connections, exceeding maxIdle
	conns := make([]redis.Conn, 3)
	for i := 0; i < 3; i++ {
		conns[i], _ = pool.Get()
	}

	// Now release all 3. The pool should only keep 2 idle.
	for _, c := range conns {
		pool.Release(c)
	}

	if pool.idleQueue.Len() != 2 {
		t.Errorf("Expected idle queue length to be capped at maxIdle(2), but got %d", pool.idleQueue.Len())
	}
	// Total active connections should also be 2, as one was closed.
	if pool.active != 2 {
		t.Errorf("Expected active connections to be 2 after capping, but got %d", pool.active)
	}
}

// ===== Performance Benchmarks =====

// Benchmark with contention, more goroutines than available connections
func BenchmarkContention(b *testing.B) {
	pool, err := NewRedisPool(50, 50, time.Minute, factory, testOnBorrow, false)
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close()
	b.ResetTimer()

	// Run with 4x more goroutines than connections to ensure high contention
	b.SetParallelism(4)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Get()
			if err != nil {
				b.Fatalf("Get failed: %v", err)
			}
			// Simulate tiny work
			time.Sleep(1 * time.Microsecond)
			if err := pool.Release(conn); err != nil {
				b.Fatalf("Release failed: %v", err)
			}
		}
	})
}

// TestRelease_AfterClose 专门测试 在Close之后紧接着调用Release是否会panic
func TestRelease_AfterClose(t *testing.T) {
	pool, err := NewRedisPool(1, 1, 0, factory, func(redis.Conn, context.Context, time.Time) error { return nil }, true)
	if err != nil {
		t.Fatal(err)
	}

	conn, _ := pool.Get()

	// 关闭连接池
	pool.Close()

	// 在一个已关闭的池上调用 Release，不应该 panic
	// 使用一个 goroutine 以防万一原始代码发生死锁而不是 panic
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Release panicked after Close: %v", r)
			}
			close(done)
		}()
		pool.Release(conn)
	}()

	select {
	case <-done:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("Release deadlocked after Close")
	}
}

// TestGetContext_PropagatesContext 专门测试 context是否正确传递并发挥作用
func TestGetContext_PropagatesContext(t *testing.T) {
	// 创建一个 testOnBorrow 函数，它会检查 context 是否被取消
	testFunc := func(c redis.Conn, ctx context.Context, lastUsed time.Time) error {
		select {
		case <-ctx.Done():
			return ctx.Err() // 如果 context 被取消，测试失败
		default:
			return nil // 否则测试成功
		}
	}

	pool, err := NewRedisPool(1, 1, 0, factory, testFunc, true)
	if err != nil {
		t.Fatal(err)
	}

	// 先取走唯一的连接，让下一个 Get 必须从 idle 队列里拿
	conn, _ := pool.Get()
	pool.Release(conn)

	// 创建一个已经被取消的 context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 使用被取消的 context 调用 GetContext。
	// 如果 bug 存在 (传递的是 context.Background)，testOnBorrow 会成功，GetContext 会返回一个连接。
	// 如果 bug 被修复，testOnBorrow 会失败，GetContext 会尝试创建新连接（或返回错误）。
	// 在这个场景下，它会关闭坏连接并创建一个新的。
	_, err = pool.GetContext(ctx)

	// 我们不关心具体的错误，只关心它有没有返回我们预期的错误。
	// 在一个更复杂的场景中，我们可能期望它重试。
	// 但对于这个测试，我们主要验证 context 是否被检查了。
	// 如果Get成功了，说明context没传下去。
	if err == nil {
		t.Error("GetContext succeeded with a canceled context, indicating the context was not propagated to testOnBorrow")
	}
}

// TestConcurrency_WithFailingConnections 这是一个综合压力测试
// 它模拟了真实世界中连接会随机失败的场景，考验池的恢复能力和状态一致性
func TestConcurrency_WithFailingConnections(t *testing.T) {
	atomic.StoreInt32(&openConnections, 0)

	// testOnBorrow 会有 30% 的几率失败
	var failCount int32
	testFunc := func(c redis.Conn, ctx context.Context, lastUsed time.Time) error {
		if atomic.AddInt32(&failCount, 1)%3 == 0 {
			return errors.New("connection failed health check")
		}
		return nil
	}

	maxActive := 20
	pool, err := NewRedisPool(maxActive, 10, time.Second, factory, testFunc, true)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				conn, err := pool.Get()
				if err != nil {
					continue
				}
				// 模拟工作
				time.Sleep(time.Duration(1+j%5) * time.Millisecond)
				pool.Release(conn)
			}
		}()
	}
	wg.Wait()

	// 在测试结束后，池的状态应该是健康的
	if pool.active > maxActive {
		t.Errorf("active connection count (%d) exceeded maxActive (%d)", pool.active, maxActive)
	}

	// openConnections 应该等于 active
	// 由于并发，给一点时间让状态同步
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&openConnections) != int32(pool.active) {
		t.Errorf("mismatch between open connections (%d) and pool's active count (%d)", atomic.LoadInt32(&openConnections), pool.active)
	}
}

// TestActiveCount_NeverExceedsMaxActive 专门测试在极端并发下 active 计数器是否会超过 maxActive
func TestActiveCount_NeverExceedsMaxActive(t *testing.T) {
	// 使用一个较小的 maxActive 以便轻松制造竞争
	maxActive := 10
	pool, err := NewRedisPool(
		maxActive,
		maxActive, // maxIdle = maxActive for simplicity
		time.Minute,
		factory,
		func(c redis.Conn, ctx context.Context, t time.Time) error { return nil }, // no-op health check
		true, // wait = true
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// 使用一个可以被取消的 context 来控制 workers 的生命周期
	// 测试将运行2秒，如果在这期间没有发现问题，则认为通过
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	// 启动 20 倍于 maxActive 数量的 workers 来制造高竞争
	numWorkers := maxActive * 20
	wg.Add(numWorkers)

	// 启动所有 worker goroutines
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			// 在 context 的控制下持续 Get/Release
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				conn, err := pool.GetContext(ctx)
				if err != nil {
					// 如果 context 被取消，这里会报错，循环将在下一次迭代中退出
					continue
				}

				// 模拟非常短暂的持有时间，以最大化 Get/Release 的频率
				time.Sleep(10 * time.Microsecond)

				pool.Release(conn)
			}
		}()
	}

	// 启动 Inspector goroutine
	// 它会持续检查 active 的值，直到 context 结束
	var maxObservedActive int
	var mu sync.Mutex // Mutex to protect maxObservedActive

	inspectorDone := make(chan struct{})
	go func() {
		defer close(inspectorDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			pool.mu.Lock()
			currentActive := pool.active
			pool.mu.Unlock()

			mu.Lock()
			if currentActive > maxObservedActive {
				maxObservedActive = currentActive
			}
			mu.Unlock()

			// 检查是否超过上限
			if currentActive > maxActive {
				// 立即取消所有操作并标记测试失败
				cancel()
				t.Errorf("FATAL: active count (%d) exceeded maxActive (%d)", currentActive, maxActive)
				return // 尽早退出 inspector
			}
			// 短暂休眠，让出 CPU
			time.Sleep(100 * time.Microsecond)
		}
	}()

	// 等待测试时间结束或被 inspector 提前终止
	<-ctx.Done()
	// 等待所有 worker goroutines 优雅地退出
	wg.Wait()
	// 等待 inspector 退出
	<-inspectorDone

	mu.Lock()
	finalMaxActive := maxObservedActive
	mu.Unlock()

	// 做最后的报告
	if t.Failed() {
		t.Logf("Test failed. Maximum observed active count was: %d", finalMaxActive)
	} else {
		t.Logf("Test passed. Maximum observed active count was: %d (maxActive: %d)", finalMaxActive, maxActive)
	}
}
