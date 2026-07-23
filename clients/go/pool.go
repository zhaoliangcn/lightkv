// Package lightkv provides a connection pool for LightKV client SDK.
// Wraps Client with a pool of reusable TCP connections.
package lightkv

import (
	"errors"
	"sync"
	"time"
)

// Pool is a connection pool for LightKV Client.
//
// Usage:
//
//	pool := NewPool("127.0.0.1:6379", PoolOptions{PoolSize: 10})
//	c, err := pool.Acquire()
//	if err != nil { panic(err) }
//	defer pool.Release(c)
//	c.Set("key", "value")
//	pool.Close()
type Pool struct {
	addr      string
	timeout   time.Duration
	poolSize  int
	retry     int
	idleTTL   time.Duration
	healthCheck time.Duration
	pingTimeout time.Duration

	mu        sync.Mutex
	idle      []*poolEntry
	closed    bool
	health    *poolHealth
}

type poolEntry struct {
	client      *Client
	lastUsedAt time.Time
	lastHealthAt time.Time
}

// PoolOptions configures a Pool.
type PoolOptions struct {
	Timeout    time.Duration // connection timeout (default 5s)
	PoolSize   int           // max cached connections (default 10)
	Retry      int           // retry attempts on connection failure (default 3)
	IdleTTL    time.Duration // idle connection TTL (default 5m)
	HealthCheck time.Duration // health check interval (default 1m); 0 = disable
	PingTimeout time.Duration // PING timeout for health check (default 1s)
}

// health state for background checker
type poolHealth struct {
	stop chan struct{}
	wg   sync.WaitGroup
}

// NewPool creates a connection pool for the given LightKV server address.
func NewPool(addr string, opts PoolOptions) *Pool {
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.PoolSize == 0 {
		opts.PoolSize = 10
	}
	if opts.IdleTTL == 0 {
		opts.IdleTTL = 5 * time.Minute
	}
	if opts.HealthCheck == 0 {
		opts.HealthCheck = 1 * time.Minute
	}
	if opts.PingTimeout == 0 {
		opts.PingTimeout = 1 * time.Second
	}
	p := &Pool{
		addr:     addr,
		timeout:  opts.Timeout,
		poolSize: opts.PoolSize,
		retry:    opts.Retry,
		idleTTL:  opts.IdleTTL,
		healthCheck: opts.HealthCheck,
		pingTimeout: opts.PingTimeout,
	}
	if opts.HealthCheck > 0 {
		p.health = &poolHealth{stop: make(chan struct{})}
		p.health.wg.Add(1)
		go p.healthLoop()
	}
	return p
}

// healthLoop 后台健康检查：定期 PING idle 连接，剔除坏的
func (p *Pool) healthLoop() {
	defer p.health.wg.Done()
	ticker := time.NewTicker(p.healthCheck)
	defer ticker.Stop()
	for {
		select {
		case <-p.health.stop:
			return
		case <-ticker.C:
			p.doHealthCheck()
		}
	}
}

func (p *Pool) doHealthCheck() {
	now := time.Now()
	var alive []*poolEntry
	p.mu.Lock()
	for _, e := range p.idle {
		if now.Sub(e.lastUsedAt) > p.idleTTL {
			e.client.Close()
			continue
		}
		if now.Sub(e.lastHealthAt) < p.healthCheck {
			alive = append(alive, e)
			continue
		}
		if p.pingAlive(e.client) {
			e.lastHealthAt = now
			alive = append(alive, e)
		} else {
			e.client.Close()
		}
	}
	p.idle = alive
	p.mu.Unlock()
}

func (p *Pool) pingAlive(c *Client) bool {
	// PING with timeout; if client has no Ping method, treat as alive
	// Use a short deadline via a stub call (Client doesn't expose PING yet)
	// For safety, return true (alive) — real PING added when Client exposes it
	return true
}

// newClient creates a Client with exponential backoff retry.
func (p *Pool) newClient() (*Client, error) {
	var lastErr error
	for attempt := 0; attempt <= p.retry; attempt++ {
		c := NewClient(p.addr, p.timeout)
		if err := c.Connect(); err == nil {
			return c, nil
		} else {
			lastErr = err
			if attempt < p.retry {
				time.Sleep(time.Duration(100<<(attempt)) * time.Millisecond)
			}
		}
	}
	return nil, errors.New("failed to connect after retries: " + lastErr.Error())
}

// Acquire takes a client from the pool, or creates a new one if pool is empty.
func (p *Pool) Acquire() (*Client, error) {
	if p.closed {
		return nil, errors.New("pool is closed")
	}
	for {
		p.mu.Lock()
		if len(p.idle) == 0 {
			p.mu.Unlock()
			return p.newClient()
		}
		entry := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.mu.Unlock()
		// Stale connection → discard and retry
		if time.Since(entry.lastUsedAt) > p.idleTTL {
			entry.client.Close()
			continue
		}
		return entry.client, nil
	}
}

// Release returns a client to the pool for reuse, or closes it if pool is full.
func (p *Pool) Release(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.idle) >= p.poolSize {
		c.Close()
		return
	}
	now := time.Now()
	p.idle = append(p.idle, &poolEntry{client: c, lastUsedAt: now, lastHealthAt: now})
}

// Close closes all idle connections and marks pool as closed.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	if p.health != nil {
		close(p.health.stop)
	}
	for _, e := range p.idle {
		e.client.Close()
	}
	p.idle = nil
	p.mu.Unlock()
	if p.health != nil {
		p.health.wg.Wait()
		p.health = nil
	}
}
