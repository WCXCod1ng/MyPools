package redis_pool

import (
	"errors"
	"sync"

	"github.com/gomodule/redigo/redis"
)

var (
	RedisPoolCreateErr = errors.New("create RedisPool Error")
	ConnStatusErr      = errors.New("connection status error")
	RedisPoolCloseErr  = errors.New("redis pool has been closed")
)

// RedisPool 管理Redis连接的核心：连接池
type RedisPool struct {
	idleQueue   chan redis.Conn            // 管理所有空闲的连接，它的大小就是空闲连接的最大数量
	factory     func() (redis.Conn, error) // 连接创建工厂，用于创建新的连接
	maxActive   int                        // 连接池中能够容纳的最大连接数量
	currentSize int                        // 当前已经创建的连接的数量（包括正在使用的和空闲的）
	closed      bool                       // 连接池是否关闭
	mu          sync.Mutex                 // 用于保护currentSize和numActive的互斥锁
}

// NewRedisPool 创建一个Redis连接池
// maxActive: 连接池的最大连接数量
// maxIdle: 连接池的最大空闲数量
func NewRedisPool(maxActive int, maxIdle int, factory func() (redis.Conn, error)) (*RedisPool, error) {
	if maxActive <= 0 || maxIdle <= 0 || maxActive < maxIdle || factory == nil {
		return nil, RedisPoolCreateErr
	}

	r := &RedisPool{
		idleQueue:   make(chan redis.Conn, maxIdle), // 创建一个缓冲大小为maxIdle的channel
		factory:     factory,
		maxActive:   maxActive,
		currentSize: 0,
	}

	//  提前创建maxIdle个连接放到空闲队列中
	for i := 0; i < maxIdle; i++ {
		conn, err := factory()
		if err != nil {
			// 发生错误，将之前的所有已经创建的连接都关闭
			close(r.idleQueue)
			for conn = range r.idleQueue {
				conn.Close()
			}
			return nil, err
		}
		r.idleQueue <- conn // 向channel中加入连接
	}

	// 创建成功后再更新currentSize
	r.currentSize = maxIdle

	return r, nil
}

// Get 从连接池中（阻塞）获取一个链接
func (p *RedisPool) Get() (redis.Conn, error) {
	// 首先，尝试非阻塞地从空闲队列获取
	select {
	case conn := <-p.idleQueue:
		// 如果连接关闭，则直接返回错误
		if p.isClosed() { // 需要一个辅助方法来安全地检查closed状态
			if conn != nil {
				conn.Close()
			}
			return nil, RedisPoolCloseErr
		}
		return conn, nil
	default:
		// 当没有空闲连接时，继续往下走
	}

	p.mu.Lock()
	// 再一次判断是为了防止在上述select语句结束后恰好连接池被关闭
	if p.closed {
		p.mu.Unlock()
		return nil, RedisPoolCloseErr
	}

	// 如果当前丽娜姐未达到上限，则创建新的连接
	if p.currentSize < p.maxActive {
		p.currentSize++
		p.mu.Unlock() // 在调用factory之前解锁（因为网络请求的时延较大）

		// 调用factory执行实际的创建链接
		conn, err := p.factory()
		if err != nil {
			// 创建失败，需要把加上的currentSize减回去
			p.mu.Lock()
			p.currentSize--
			p.mu.Unlock()
			return nil, err
		}
		return conn, nil
	}

	// 到此说明连接已达上限
	// note 在当前的实现中需要阻塞等待一个连接被归还
	// note 注意，阻塞之前应当解锁
	p.mu.Unlock()

	conn, ok := <-p.idleQueue // 这里直接使用channel的特性阻塞等待
	// 同样再一次检查连接池是否被关闭，这里可以直接使用ok来表示，如果为false表示已经关闭
	if !ok {
		if conn != nil {
			conn.Close()
		}
		return nil, RedisPoolCloseErr
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
func (p *RedisPool) Release(conn redis.Conn) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed { // 提前检查连接池是否被关闭，如果已经被关闭，则直接报错
		return RedisPoolCloseErr
	}

	if conn == nil {
		return ConnStatusErr
	}

	// 如果连接已经损坏，则应该直接关闭它
	if conn.Err() != nil {
		conn.Close()
		p.currentSize -= 1
		return conn.Err()
	}

	// 到此说明连接健康，应当尝试将其非阻塞地放入conns中
	select {
	case p.idleQueue <- conn:
		// 放入成功，说明池中的空闲链接还没有满（< maxIdle），连接被成功复用。直接返回
		return nil
	default:
		// 放入失败，说明空闲池已满，多余的连接应当直接关闭，同时总活跃数应当-1
		if err := conn.Close(); err != nil {
			return err
		}
		p.currentSize -= 1
	}

	return nil
}

// Close 关闭连接池
func (p *RedisPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭channel
	close(p.idleQueue)
	// 关闭channel中的所有链接
	for conn := range p.idleQueue {
		conn.Close()
	}

	// 标记为closed
	p.closed = true
}
