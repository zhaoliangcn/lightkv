"""
LightKV Connection Pool for Python SDK.
Wraps LightKVClient with a pool of reusable TCP connections.

Zero dependencies — uses only Python standard library.
"""
import threading
import time
from typing import Optional
from .client import LightKVClient


class _PooledClient:
    """A LightKVClient wrapper that returns itself to the pool on release."""
    def __init__(self, client: LightKVClient, pool: 'LightKVPool'):
        self.client = client
        self._pool = pool
        self._released = False

    def __enter__(self):
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False

    def release(self):
        if self._released:
            return
        self._released = True
        self._pool._release(self.client)


class LightKVPool:
    """
    Connection pool for LightKVClient.

    Args:
        host: Server host (default: '127.0.0.1')
        port: Server port (default: 6379)
        timeout: Connection timeout in seconds (default: 5.0)
        pool_size: Maximum number of cached connections (default: 10)
        retry: Number of retry attempts on connection failure (default: 3)
        idle_timeout: Seconds before an idle connection is considered stale (default: 300)

    Usage:
        pool = LightKVPool(host='127.0.0.1', port=6379, pool_size=10)
        with pool.acquire() as client:
            client.set('key', 'value')
            print(client.get('key'))
        # connection returned to pool automatically
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 6379,
                 timeout: float = 5.0, pool_size: int = 10,
                 retry: int = 3, idle_timeout: float = 300.0,
                 health_check_interval: float = 60.0,
                 ping_timeout: float = 1.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._pool_size = pool_size
        self._retry = retry
        self._idle_timeout = idle_timeout
        self._health_check_interval = health_check_interval
        self._ping_timeout = ping_timeout

        self._idle: list = []  # list of (client, last_used_ts, last_health_ts)
        self._mu = threading.Lock()
        self._closed = False
        self._health_thread = None
        if health_check_interval > 0:
            self._health_thread = threading.Thread(
                target=self._health_loop, daemon=True, name='LightKV-pool-health')
            self._health_thread.start()

    def _health_loop(self):
        """后台健康检查线程：定期 PING idle 连接，剔除坏的"""
        while not self._closed:
            time.sleep(self._health_check_interval)
            now = time.time()
            alive = []
            with self._mu:
                for c, ts, last_h in self._idle:
                    if now - ts > self._idle_timeout:
                        try: c.disconnect()
                        except Exception: pass
                        continue
                    if now - last_h < self._health_check_interval:
                        alive.append((c, ts, last_h))
                        continue
                    # PING 健康检查
                    if self._ping_alive(c):
                        alive.append((c, ts, now))
                    else:
                        try: c.disconnect()
                        except Exception: pass
                self._idle = alive

    def _ping_alive(self, client) -> bool:
        """对 client 跑 PING，超时或失败返回 False"""
        try:
            ok = client.ping() if hasattr(client, 'ping') else True
            return ok
        except Exception:
            return False

    def _new_client(self) -> LightKVClient:
        last_err = None
        for attempt in range(self._retry + 1):
            c = LightKVClient(self._host, self._port, self._timeout)
            if c.connect():
                return c
            last_err = c.get_last_error()
            if attempt < self._retry:
                time.sleep(0.1 * (2 ** attempt))  # exponential backoff
        raise ConnectionError(f"failed to connect after {self._retry + 1} attempts: {last_err}")

    def acquire(self) -> _PooledClient:
        """Acquire a client from the pool (or create a new one)."""
        if self._closed:
            raise RuntimeError("pool is closed")
        while True:
            with self._mu:
                if self._idle:
                    c, ts, last_h = self._idle.pop()
                    # Stale connection → discard and retry
                    if time.time() - ts > self._idle_timeout:
                        try:
                            c.disconnect()
                        except Exception:
                            pass
                        continue
                    return _PooledClient(c, self)
            # Pool empty → create new
            c = self._new_client()
            return _PooledClient(c, self)

    def _release(self, client: LightKVClient):
        with self._mu:
            if self._closed or len(self._idle) >= self._pool_size:
                try:
                    client.disconnect()
                except Exception:
                    pass
                return
            self._idle.append((client, time.time(), time.time()))

    def close(self):
        """Close all idle connections and mark pool as closed."""
        with self._mu:
            self._closed = True
            for c, _, _ in self._idle:
                try:
                    c.disconnect()
                except Exception:
                    pass
            self._idle.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
