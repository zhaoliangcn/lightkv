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

    this._idle = []; // [{ client, lastUsedAt }
    this._closed = false;
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
    this._idle.push({ client, lastUsedAt: Date.now() });
  }

  /**
   * Close all idle connections and mark pool as closed.
   */
  async close() {
    this._closed = true;
    for (const { client } of this._idle) {
      try { await client.disconnect(); } catch (e) {}
    }
    this._idle = [];
  }
}

module.exports = { LightKVPool };
