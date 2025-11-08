package redis_pool

import (
	"context"
	"errors"
	"sync"
	"time"

	"MyPools/list"

	"github.com/gomodule/redigo/redis"
)

var (
	ErrRedisPoolCreate = errors.New("create RedisPool Error")
	ErrConnStatus      = errors.New("connection status error")
	ErrRedisPoolClose  = errors.New("redis pool has been closed")
	ErrPoolExhausted   = errors.New("redis pool: connection pool exhausted")
)

type idleConn struct {
	conn redis.Conn
	ts   time.Time // 标记了其进入空闲队列的时间
}

// RedisPool 管理Redis连接的核心：连接池
type RedisPool struct {
	idleQueue *list.List[*idleConn] // fix: 将原始的channel修改为双向链表，双向链表的头部存储最新加入的空闲连接，尾部存放最老的空闲连接
	ch        chan struct{}         //  额外提供一个channel，用于实现阻塞式的等待和唤醒（它的大小和maxActive一样），它代表了一个permit，maxActive-len(ch)表示当前已经使用了多少个

	factory func() (redis.Conn, error) // 连接创建工厂，用于创建新的连接

	testOnBorrow func(conn redis.Conn, ctx context.Context, lastUsed time.Time) error

	maxActive int // 连接池中能够容纳的最大连接数量
	maxIdle   int // 连接池中能够容纳的最大空闲连接数量

	active int        // 当前已经创建的连接的数量（包括正在被线程使用的和空闲链表中的），active = maxActive-len(ch) + len(idleQueue)
	closed bool       // 连接池是否关闭
	mu     sync.Mutex // 用于保护currentSize和numActive的互斥锁

	idleTimeout time.Duration // 连接空闲的最大时间，设置为0表示永不过期

	wait bool // 当连接池的所有连接耗尽时，是否等待
}

// NewRedisPool 创建一个Redis连接池
// maxActive: 连接池的最大连接数量
// maxIdle: 连接池的最大空闲数量
func NewRedisPool(
	maxActive int,
	maxIdle int,
	idleTimeout time.Duration,
	factory func() (redis.Conn, error),
	testOnBorrow func(redis.Conn, context.Context, time.Time) error,
	wait bool,
) (*RedisPool, error) {
	if maxActive <= 0 || maxIdle <= 0 || maxActive < maxIdle || factory == nil || testOnBorrow == nil {
		return nil, ErrRedisPoolCreate
	}

	r := &RedisPool{
		idleQueue:    list.New[*idleConn](), // 创建一个缓冲大小为maxIdle的channel
		ch:           make(chan struct{}, maxActive),
		factory:      factory,
		testOnBorrow: testOnBorrow,
		maxActive:    maxActive,
		maxIdle:      maxIdle,
		active:       maxIdle,
		idleTimeout:  idleTimeout,
		wait:         wait,
	}

	//  提前创建maxIdle个连接放到空闲队列中
	for i := 0; i < maxIdle; i++ {
		conn, err := factory()
		if err != nil {
			// 发生错误，将之前的所有已经创建的连接都关闭
			for e := r.idleQueue.Begin(); e != nil; e = e.Next() {
				e.Value().conn.Close()
			}
			return nil, err
		}

		// 向list中加入连接，注意需要加在开头
		r.idleQueue.PushFront(&idleConn{
			conn: conn,
			ts:   time.Now(),
		})
	}

	// 为ch中加入maxActive个元素，用于实现Get和Release的同步（生产者、消费者模型）
	for i := 0; i < maxActive; i++ {
		r.ch <- struct{}{}
	}

	return r, nil
}

// Get 从连接池中（阻塞）获取一个链接
// 我们实现“懒汉式”检查策略，每当Get的时候触发过期连接的检查
// 内部通过testOnBorrow来检查连接是否“健康”，确保返回的连接一定是健康的（否则返回error）
func (p *RedisPool) Get() (redis.Conn, error) {
	return p.GetContext(context.Background())
}

// GetContext 从连接池中获取一个连接（如果开启了阻塞模式，则也会同步等待ctx）
// 我们实现“懒汉式”检查策略，每当Get的时候触发过期连接的检查
// 内部通过testOnBorrow来检查连接是否“健康”，确保返回的连接一定是健康的（否则返回error）
func (p *RedisPool) GetContext(ctx context.Context) (redis.Conn, error) {
	if !p.wait {
		// no-blocking style
		select {
		case <-p.ch:
			// got a permit, proceed
		default:
			// no permit available, return error immediately
			return nil, ErrPoolExhausted
		}
	} else {
		// blocking style
		select {
		case <-p.ch:
			// got a permit, proceed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// 1. 加锁
	p.mu.Lock()

	// 2. 清理过期连接
	if p.idleTimeout > 0 {
		// 重复操作：如果链表非空，则查看链表尾部的连接，判断它是否超时，如果超时则移除。直到所有连接都不超时
		for !p.idleQueue.Empty() {
			e := p.idleQueue.Back()
			if time.Since(e.ts) > p.idleTimeout {
				e = p.idleQueue.PopBack()
				// 注意要关闭
				e.conn.Close()
				p.active--
			} else {
				break
			}
		}
	}

	// 3. 获取连接
	// change 在引入了testOnBorrow之后，这里就不能只提取一个了，而是应当从队列中“循环”获取队首，直到获取到的连接是“健康”的、或者连接为空
	for !p.idleQueue.Empty() {
		conn := p.idleQueue.PopFront().conn
		// 测试实际上就是要发起一次请求，所以也需要解锁
		p.mu.Unlock()
		if p.testOnBorrow == nil || p.testOnBorrow(conn, ctx, time.Now()) == nil {
			// 测试成功，直接返回这个连接，由于我们使用defer进行了Unlock，如果这里不加锁就会导致两次Unlock
			// note 即使panic了也不用担心，此时所处于unlock状态，不会造成死锁
			return conn, nil
		}
		// 测试失败，需要关闭这个连接，专注以再次上锁
		conn.Close()
		p.mu.Lock()
		p.active--
	}

	// 到此说明没有空闲的连接了（或者连接都是不健康的），总之空闲队列为空了
	// 而且能够到此，说明p.ch之前一定有一个permit（len(ch)>0），那么active(= maxActive- len(ch) + 0) < p.maxActive == true。可以创建新的连接
	p.active++

	// 再次检查是否关闭
	if p.closed {
		p.mu.Unlock()
		return nil, ErrRedisPoolClose
	}
	p.mu.Unlock() // 在调用factory之前解锁（因为网络请求的时延较大）

	// 调用factory执行实际的创建链接
	conn, err := p.factory()
	if err != nil {
		// note 创建失败，说明本次Get()失败，所以需要将状态退回到Get之前， 这里主要关注的是：
		// 1. p.active--，而且要重新上锁
		// 2. p.ch必须新增一个位置
		p.mu.Lock()
		p.active--
		if !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
		return nil, err
	}
	return conn, nil
}

// isClosed 是一个辅助方法，用于安全地检查连接池是否关闭
func (p *RedisPool) isClosed() bool {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	return closed
}

// Release 释放一个连接
// 注意当关闭一个连接时，就多了一个permit，那么相应的就一定需要 p.ch <- struct{}{}
func (p *RedisPool) Release(conn redis.Conn) error {
	p.mu.Lock()
	defer func() {
		if !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
	}()

	if p.closed { // 提前检查连接池是否被关闭，如果已经被关闭，则直接报错
		return ErrRedisPoolClose
	}

	if conn == nil {
		return ErrConnStatus
	}

	// change 损坏的连接不再由Release负责，而是应当由Get的TestOnBorrow负责检查，如果检查失败说明Get失败，那时再进行close
	//// 如果连接已经损坏，则应该直接关闭它
	//if conn.Err() != nil {
	//	conn.Close()
	//	p.active--
	//	return conn.Err()
	//}

	// 到此说明连接健康，应当尝试将其放入空闲队列
	// 将新归还的连接插入队列的头部
	p.idleQueue.PushFront(&idleConn{
		conn: conn,
		ts:   time.Now(),
	})
	// 如果队列的长度大于maxIdle，则应当从尾部弹出一个最老的连接并关闭它
	if p.idleQueue.Len() > p.maxIdle {
		back := p.idleQueue.PopBack()
		back.conn.Close()
		p.active--
	}

	return nil
}

// Close 关闭连接池
func (p *RedisPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 清空队列
	for !p.idleQueue.Empty() {
		e := p.idleQueue.PopFront()
		e.conn.Close()
	}

	// 清空ch
	close(p.ch)
	p.ch = nil
	p.active = 0
	// 标记为closed
	p.closed = true
}
