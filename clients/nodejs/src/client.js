'use strict';

const net = require('net');

/**
 * LightKV Client SDK for Node.js
 * Connects to LightKV Server via TCP using Redis RESP protocol.
 * Zero dependencies — uses only Node.js built-in `net` module.
 */
class LightKVClient {
  /**
   * @param {Object} [options]
   * @param {string} [options.host='127.0.0.1']
   * @param {number} [options.port=6379]
   * @param {number} [options.timeout=5000] - Connection timeout in ms
   */
  constructor(options = {}) {
    this.host = options.host || '127.0.0.1';
    this.port = options.port || 6379;
    this.timeout = options.timeout || 5000;
    this._socket = null;
    this._buffer = '';
    this._connected = false;
    this._lastError = null;
    this._pendingResolve = null;
    this._pendingReject = null;
  }

  /**
   * Connect to LightKV server.
   * @returns {Promise<void>}
   */
  connect() {
    return new Promise((resolve, reject) => {
      if (this._connected) {
        resolve();
        return;
      }

      this._buffer = '';
      this._lastError = null;

      this._socket = net.createConnection({
        host: this.host,
        port: this.port,
      });

      this._socket.setTimeout(this.timeout);

      this._socket.on('connect', () => {
        this._connected = true;
        this._socket.setTimeout(0);
        resolve();
      });

      this._socket.on('data', (data) => {
        this._buffer += data.toString();
        // Try to parse response
        if (this._pendingResolve) {
          const result = this._tryParseResponse();
          if (result !== undefined) {
            const resolveFn = this._pendingResolve;
            this._pendingResolve = null;
            this._pendingReject = null;
            resolveFn(result);
          }
        }
      });

      this._socket.on('error', (err) => {
        this._lastError = err.message;
        this._connected = false;
        if (this._pendingReject) {
          const rejectFn = this._pendingReject;
          this._pendingResolve = null;
          this._pendingReject = null;
          rejectFn(err);
        }
      });

      this._socket.on('timeout', () => {
        this._lastError = 'Connection timeout';
        this._socket.destroy();
        if (this._pendingReject) {
          const rejectFn = this._pendingReject;
          this._pendingResolve = null;
          this._pendingReject = null;
          rejectFn(new Error('Connection timeout'));
        }
      });

      this._socket.on('close', () => {
        this._connected = false;
        if (this._pendingReject) {
          const rejectFn = this._pendingReject;
          this._pendingResolve = null;
          this._pendingReject = null;
          rejectFn(new Error('Connection closed'));
        }
      });
    });
  }

  /**
   * Disconnect from server.
   */
  disconnect() {
    if (this._socket) {
      this._socket.destroy();
      this._socket = null;
    }
    this._connected = false;
    if (this._pendingReject) {
      this._pendingReject(new Error('Disconnected'));
      this._pendingResolve = null;
      this._pendingReject = null;
    }
  }

  /**
   * Check if connected.
   * @returns {boolean}
   */
  isConnected() {
    return this._connected;
  }

  /**
   * Get last error message.
   * @returns {string|null}
   */
  getLastError() {
    return this._lastError;
  }

  /**
   * Send a RESP command and parse the response.
   * @param {string[]} args
   * @returns {Promise<any>}
   */
  _sendCommand(args) {
    return new Promise((resolve, reject) => {
      if (!this._connected || !this._socket) {
        reject(new Error('Not connected'));
        return;
      }

      const cmd = this._buildResp(args);
      this._pendingResolve = resolve;
      this._pendingReject = reject;

      this._socket.write(cmd, (err) => {
        if (err) {
          this._lastError = err.message;
          this._pendingResolve = null;
          this._pendingReject = null;
          reject(err);
        }
      });
    });
  }

  /**
   * Try to parse a complete RESP response from the buffer.
   * @returns {any|undefined}
   */
  _tryParseResponse() {
    if (!this._buffer || this._buffer.length < 3) return undefined;

    const type = this._buffer[0];

    if (type === '+') {
      // Simple string: +OK\r\n
      const cr = this._buffer.indexOf('\r\n');
      if (cr === -1) return undefined;
      const value = this._buffer.substring(1, cr);
      this._buffer = this._buffer.substring(cr + 2);
      return value;
    }

    if (type === '-') {
      // Error: -ERR message\r\n
      const cr = this._buffer.indexOf('\r\n');
      if (cr === -1) return undefined;
      this._lastError = this._buffer.substring(1, cr);
      this._buffer = this._buffer.substring(cr + 2);
      return null;
    }

    if (type === ':') {
      // Integer: :123\r\n
      const cr = this._buffer.indexOf('\r\n');
      if (cr === -1) return undefined;
      const value = parseInt(this._buffer.substring(1, cr), 10);
      this._buffer = this._buffer.substring(cr + 2);
      return value;
    }

    if (type === '$') {
      // Bulk string: $5\r\nhello\r\n or $-1\r\n
      const cr = this._buffer.indexOf('\r\n');
      if (cr === -1) return undefined;
      const len = parseInt(this._buffer.substring(1, cr), 10);
      if (len < 0) {
        this._buffer = this._buffer.substring(cr + 2);
        return null; // nil
      }
      const dataStart = cr + 2;
      const dataEnd = dataStart + len;
      if (dataEnd + 2 > this._buffer.length) return undefined;
      const value = this._buffer.substring(dataStart, dataEnd);
      this._buffer = this._buffer.substring(dataEnd + 2);
      return value;
    }

    if (type === '*') {
      // Array: *N\r\n...
      return this._parseArray();
    }

    return undefined;
  }

  /**
   * Parse RESP array from buffer.
   * @returns {any|undefined}
   */
  _parseArray() {
    const cr = this._buffer.indexOf('\r\n');
    if (cr === -1) return undefined;

    const count = parseInt(this._buffer.substring(1, cr), 10);
    let pos = cr + 2;

    if (count < 0) {
      this._buffer = this._buffer.substring(pos);
      return null;
    }

    const result = [];
    for (let i = 0; i < count; i++) {
      if (pos >= this._buffer.length) return undefined;
      const elemType = this._buffer[pos];

      if (elemType === '$') {
        const elemCr = this._buffer.indexOf('\r\n', pos);
        if (elemCr === -1) return undefined;
        const elemLen = parseInt(this._buffer.substring(pos + 1, elemCr), 10);
        if (elemLen < 0) {
          result.push(null);
          pos = elemCr + 2;
        } else {
          const dataStart = elemCr + 2;
          const dataEnd = dataStart + elemLen;
          if (dataEnd + 2 > this._buffer.length) return undefined;
          result.push(this._buffer.substring(dataStart, dataEnd));
          pos = dataEnd + 2;
        }
      } else {
        // Unknown type, skip
        return undefined;
      }
    }

    this._buffer = this._buffer.substring(pos);
    return result;
  }

  /**
   * Build RESP array command.
   * @param {string[]} args
   * @returns {string}
   */
  _buildResp(args) {
    let r = '*' + args.length + '\r\n';
    for (const arg of args) {
      r += '$' + arg.length + '\r\n' + arg + '\r\n';
    }
    return r;
  }

  // ─── Core Operations ───

  /**
   * Set a key-value pair.
   * @param {string} key
   * @param {string} value
   * @returns {Promise<boolean>}
   */
  async set(key, value) {
    const resp = await this._sendCommand(['SET', key, value]);
    return resp === 'OK';
  }

  /**
   * Get value by key.
   * @param {string} key
   * @returns {Promise<string|null>}
   */
  async get(key) {
    return await this._sendCommand(['GET', key]);
  }

  /**
   * Delete a key.
   * @param {string} key
   * @returns {Promise<boolean>}
   */
  async delete(key) {
    const resp = await this._sendCommand(['DEL', key]);
    return resp === 1;
  }

  /**
   * Delete a range of keys [begin, end).
   * @param {string} begin
   * @param {string} end
   * @returns {Promise<boolean>}
   */
  async deleteRange(begin, end) {
    const resp = await this._sendCommand(['DELRANGE', begin, end]);
    return resp === 1;
  }

  /**
   * Ping the server.
   * @returns {Promise<boolean>}
   */
  async ping() {
    const resp = await this._sendCommand(['PING']);
    return resp === 'PONG';
  }

  /**
   * Get server stats.
   * @returns {Promise<Object>}
   */
  async stats() {
    const resp = await this._sendCommand(['STATS']);
    if (!Array.isArray(resp)) return {};

    const result = {};
    for (let i = 0; i < resp.length; i += 2) {
      result[resp[i]] = resp[i + 1];
    }
    return result;
  }

  /**
   * Quit and disconnect.
   * @returns {Promise<void>}
   */
  async quit() {
    try {
      await this._sendCommand(['QUIT']);
    } catch (e) {
      // Ignore errors during quit
    }
    this.disconnect();
  }

  // ─── String Extension Commands ───

  /**
   * Increment key by 1.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async incr(key) {
    return await this._sendCommand(['INCR', key]);
  }

  /**
   * Decrement key by 1.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async decr(key) {
    return await this._sendCommand(['DECR', key]);
  }

  /**
   * Increment key by delta.
   * @param {string} key
   * @param {number} delta
   * @returns {Promise<number>}
   */
  async incrBy(key, delta) {
    return await this._sendCommand(['INCRBY', key, String(delta)]);
  }

  /**
   * Decrement key by delta.
   * @param {string} key
   * @param {number} delta
   * @returns {Promise<number>}
   */
  async decrBy(key, delta) {
    return await this._sendCommand(['DECRBY', key, String(delta)]);
  }

  /**
   * Increment key by float delta.
   * @param {string} key
   * @param {number} delta
   * @returns {Promise<string>}
   */
  async incrByFloat(key, delta) {
    return await this._sendCommand(['INCRBYFLOAT', key, String(delta)]);
  }

  /**
   * Set multiple key-value pairs.
   * @param {Array<[string, string]>} kvs - Array of [key, value] pairs
   * @returns {Promise<boolean>}
   */
  async mset(kvs) {
    const args = ['MSET'];
    for (const [k, v] of kvs) {
      args.push(k, v);
    }
    const resp = await this._sendCommand(args);
    return resp === 'OK';
  }

  /**
   * Get multiple keys at once.
   * @param {string[]} keys
   * @returns {Promise<Array<string|null>>}
   */
  async mget(keys) {
    const args = ['MGET', ...keys];
    return await this._sendCommand(args);
  }

  /**
   * Set key with expiration in seconds.
   * @param {string} key
   * @param {number} seconds
   * @param {string} value
   * @returns {Promise<boolean>}
   */
  async setEx(key, seconds, value) {
    const resp = await this._sendCommand(['SETEX', key, String(seconds), value]);
    return resp === 'OK';
  }

  /**
   * Set key only if it does not exist.
   * @param {string} key
   * @param {string} value
   * @returns {Promise<boolean>}
   */
  async setNx(key, value) {
    const resp = await this._sendCommand(['SETNX', key, value]);
    return resp === 1;
  }

  /**
   * Set key and return the old value.
   * @param {string} key
   * @param {string} value
   * @returns {Promise<string|null>}
   */
  async getSet(key, value) {
    return await this._sendCommand(['GETSET', key, value]);
  }

  /**
   * Append value to key's current value.
   * @param {string} key
   * @param {string} value
   * @returns {Promise<number>} New length
   */
  async append(key, value) {
    return await this._sendCommand(['APPEND', key, value]);
  }

  /**
   * Get string length of key's value.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async strLen(key) {
    return await this._sendCommand(['STRLEN', key]);
  }

  // ─── General Commands ───

  /**
   * Check if keys exist.
   * @param {string[]} keys
   * @returns {Promise<number>}
   */
  async exists(keys) {
    const args = ['EXISTS', ...keys];
    return await this._sendCommand(args);
  }

  /**
   * Set key expiration in seconds.
   * @param {string} key
   * @param {number} seconds
   * @returns {Promise<boolean>}
   */
  async expire(key, seconds) {
    const resp = await this._sendCommand(['EXPIRE', key, String(seconds)]);
    return resp === 1;
  }

  /**
   * Get key's time-to-live in seconds.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async ttl(key) {
    return await this._sendCommand(['TTL', key]);
  }

  /**
   * Get key's time-to-live in milliseconds.
   * @param {string} key
   * @returns {Promise<number>}
   */
  async pttl(key) {
    return await this._sendCommand(['PTTL', key]);
  }

  /**
   * Remove key's expiration.
   * @param {string} key
   * @returns {Promise<boolean>}
   */
  async persist(key) {
    const resp = await this._sendCommand(['PERSIST', key]);
    return resp === 1;
  }

  /**
   * Get key's data type.
   * @param {string} key
   * @returns {Promise<string>}
   */
  async type(key) {
    return await this._sendCommand(['TYPE', key]);
  }

  /**
   * Rename key.
   * @param {string} key
   * @param {string} newKey
   * @returns {Promise<boolean>}
   */
  async rename(key, newKey) {
    const resp = await this._sendCommand(['RENAME', key, newKey]);
    return resp === 'OK';
  }

  /**
   * Rename key only if new key does not exist.
   * @param {string} key
   * @param {string} newKey
   * @returns {Promise<boolean>}
   */
  async renameNx(key, newKey) {
    const resp = await this._sendCommand(['RENAMENX', key, newKey]);
    return resp === 1;
  }

  /**
   * Get all keys matching a pattern.
   * @param {string} pattern - Glob pattern (e.g. "*" or "prefix*")
   * @returns {Promise<string[]>}
   */
  async keys(pattern) {
    return await this._sendCommand(['KEYS', pattern]);
  }

  // ─── Pipeline Support ───

  /**
   * Begin buffering commands for pipeline execution.
   */
  pipeline() {
    this._pipelineBuf = [];
  }

  /**
   * Add a command to the pipeline buffer.
   * @param {string[]} args
   */
  queue(args) {
    this._pipelineBuf.push(this._buildResp(args));
  }

  /**
   * Send all queued commands and return their responses.
   * @returns {Promise<any[]>}
   */
  async execPipeline() {
    if (!this._connected || !this._socket) {
      throw new Error('Not connected');
    }
    if (!this._pipelineBuf || this._pipelineBuf.length === 0) {
      return [];
    }

    const allCmds = this._pipelineBuf.join('');
    const count = this._pipelineBuf.length;
    this._pipelineBuf = [];

    return new Promise((resolve, reject) => {
      const results = [];
      let pending = count;

      const tryRead = () => {
        try {
          while (pending > 0) {
            const resp = this._tryParseResponse();
            if (resp === undefined) break;
            results.push(resp);
            pending--;
          }
          if (pending === 0) {
            this._socket.removeListener('data', tryRead);
            resolve(results);
          }
        } catch (e) {
          this._socket.removeListener('data', tryRead);
          reject(e);
        }
      };

      // Listen for data events (same as regular command flow)
      this._socket.on('data', tryRead);

      // Send commands
      this._socket.write(allCmds, (err) => {
        if (err) {
          this._socket.removeListener('data', tryRead);
          reject(err);
        }
      });

      // Safety timeout
      setTimeout(() => {
        if (pending > 0) {
          this._socket.removeListener('data', tryRead);
          reject(new Error('Pipeline timeout'));
        }
      }, 10000);
    });
  }
}

module.exports = LightKVClient;
