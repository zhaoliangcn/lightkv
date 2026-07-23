'use strict';

const LightKVClient = require('./client.js');

/**
 * LightKV Connection Pool for Node.js SDK.
 * Wraps LightKVClient with a pool of reusable TCP connections.
 *
 * @example
 * const { LightKVPool } = require('./pool.js');
 * const pool = new LightKVPool({ host: '127.0.0.1', port: 6379, poolSize: 10 });
 * const client = await pool.acquire();
 * try {
 *   await client.set('key', 'value');
 *   console.log(await client.get('key'));
 * } finally {
 *   await pool.release(client);
 * }
 * await pool.close();
 */
class LightKVPool {
  /**
   * @param {Object} [options]
   * @param {string} [options.host='127.0.0.1']
   * @param {number} [options.port=6379]
   * @param {number} [options.timeout=5000] - Connection timeout in ms
   * @param {number} [options.poolSize=10] - Maximum cached connections
   * @param {number} [options.retry=3] - Retry attempts on connection failure
   * @param {number} [options.idleTimeout=300000] - Idle timeout in ms
   */
  constructor(options = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 6379;
    this.timeout = options.timeout || 5000;
    this.poolSize = options.poolSize || 10;
    this.retry = options.retry || 3;
    this.idleTimeout = options.idleTimeout || 300000;
    this.healthCheckInterval = options.healthCheckInterval || 60000;
    this.pingTimeout = options.pingTimeout || 1000;

    this._idle = []; // [{ client, lastUsedAt, lastHealthAt }
    this._closed = false;
    this._healthTimer = null;
    if (this.healthCheckInterval > 0) {
      this._healthTimer = setInterval(() => this._healthCheck(), this.healthCheckInterval);
      if (this._healthTimer.unref) this._healthTimer.unref();
    }
  }

  // 后台健康检查：定期 PING idle 连接，剔除坏的
  async _healthCheck() {
    if (this._closed) return;
    const now = Date.now();
    const alive = [];
    for (const entry of this._idle) {
      const { client, lastUsedAt, lastHealthAt } = entry;
      if (now - lastUsedAt > this.idleTimeout) {
        try { await client.disconnect(); } catch (e) {}
        continue;
      }
      if (now - lastHealthAt < this.healthCheckInterval) {
        alive.push(entry);
        continue;
      }
      if (await this._pingAlive(client)) {
        entry.lastHealthAt = now;
        alive.push(entry);
      } else {
        try { await client.disconnect(); } catch (e) {}
      }
    }
    this._idle = alive;
  }

  async _pingAlive(client) {
    try {
      if (typeof client.ping === 'function') {
        return await client.ping();
      }
      return true;
    } catch (e) {
      return false;
    }
  }

  async _newClient() {
    let lastErr = null;
    for (let attempt = 0; attempt <= this.retry; attempt++) {
      const c = new LightKVClient({ host: this.host, port: this.port, timeout: this.timeout });
      try {
        await c.connect();
        return c;
      } catch (e) {
        lastErr = e;
        if (attempt < this.retry) {
          await new Promise(r => setTimeout(r, 100 * (2 ** attempt)));
        }
      }
    }
    throw new Error(`failed to connect after ${this.retry + 1} attempts: ${lastErr && lastErr.message}`);
  }

  /**
   * Acquire a client from the pool (or create a new one).
   * @returns {Promise<LightKVClient>}
   */
  async acquire() {
    if (this._closed) throw new Error('pool is closed');
    while (this._idle.length > 0) {
      const { client, lastUsedAt } = this._idle.pop();
      // Stale connection → discard and retry
      if (Date.now() - lastUsedAt > this.idleTimeout) {
        try { await client.disconnect(); } catch (e) {}
        continue;
      }
      return client;
    }
    return this._newClient();
  }

  /**
   * Release a client back to the pool for reuse.
   * @param {LightKVClient} client
   */
  async release(client) {
    if (this._closed || this._idle.length >= this.poolSize) {
      try { await client.disconnect(); } catch (e) {}
      return;
    }
    this._idle.push({ client, lastUsedAt: Date.now(), lastHealthAt: Date.now() });
  }

  /**
   * Close all idle connections and mark pool as closed.
   */
  async close() {
    this._closed = true;
    if (this._healthTimer) {
      clearInterval(this._healthTimer);
      this._healthTimer = null;
    }
    for (const { client } of this._idle) {
      try { await client.disconnect(); } catch (e) {}
    }
    this._idle = [];
  }
}

module.exports = { LightKVPool };
