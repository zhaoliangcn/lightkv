"""
LightKV Client SDK for Python.
Connects to LightKV Server via TCP using Redis RESP protocol.
Zero dependencies — uses only Python standard library.
"""

import socket
import threading
from typing import Optional, Dict, List, Any

# Sentinel value to distinguish "not enough data" from "nil response"
_NOT_ENOUGH_DATA = object()


class LightKVClient:
    """
    LightKV Client SDK.

    Args:
        host: Server host (default: '127.0.0.1')
        port: Server port (default: 6379)
        timeout: Connection timeout in seconds (default: 5.0)
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 6379, timeout: float = 5.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._buffer = bytearray()
        self._connected = False
        self._last_error: Optional[str] = None
        self._lock = threading.Lock()

    def connect(self) -> bool:
        """Connect to LightKV server."""
        try:
            self._socket = socket.create_connection(
                (self._host, self._port), timeout=self._timeout
            )
            # Remove timeout after connecting — use blocking mode for reads
            self._socket.settimeout(None)
            self._buffer = bytearray()
            self._connected = True
            self._last_error = None
            return True
        except Exception as e:
            self._last_error = str(e)
            self._connected = False
            return False

    def disconnect(self) -> None:
        """Disconnect from server."""
        if self._socket:
            try:
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        self._connected = False

    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    def get_last_error(self) -> Optional[str]:
        """Get last error message."""
        return self._last_error

    def _send_command(self, args: List[str]) -> Any:
        """Send a RESP command and parse the response."""
        if not self._connected or not self._socket:
            raise ConnectionError('Not connected')

        cmd = self._build_resp(args)
        with self._lock:
            self._socket.sendall(cmd)
            return self._read_response()

    def _read_response(self) -> Any:
        """Read and parse a RESP response from the socket."""
        while True:
            result = self._try_parse_response()
            if result is not _NOT_ENOUGH_DATA:
                return result

            try:
                data = self._socket.recv(4096)
                if not data:
                    raise ConnectionError('Connection closed by server')
                self._buffer.extend(data)
            except socket.timeout:
                raise TimeoutError('Read timeout')

    def _try_parse_response(self) -> Any:
        """Try to parse a complete RESP response from the buffer.
        Returns _NOT_ENOUGH_DATA if buffer is incomplete, otherwise the parsed value.
        """
        if len(self._buffer) < 3:
            return _NOT_ENOUGH_DATA

        resp_type = chr(self._buffer[0])

        if resp_type == '+':
            cr = self._buffer.find(b'\r\n')
            if cr == -1:
                return _NOT_ENOUGH_DATA
            value = self._buffer[1:cr].decode('utf-8')
            del self._buffer[:cr + 2]
            return value

        if resp_type == '-':
            cr = self._buffer.find(b'\r\n')
            if cr == -1:
                return _NOT_ENOUGH_DATA
            self._last_error = self._buffer[1:cr].decode('utf-8')
            del self._buffer[:cr + 2]
            return None

        if resp_type == ':':
            cr = self._buffer.find(b'\r\n')
            if cr == -1:
                return _NOT_ENOUGH_DATA
            value = int(self._buffer[1:cr])
            del self._buffer[:cr + 2]
            return value

        if resp_type == '$':
            cr = self._buffer.find(b'\r\n')
            if cr == -1:
                return _NOT_ENOUGH_DATA
            length = int(self._buffer[1:cr])
            if length < 0:
                del self._buffer[:cr + 2]
                return None  # nil
            data_start = cr + 2
            data_end = data_start + length
            if data_end + 2 > len(self._buffer):
                return _NOT_ENOUGH_DATA
            value = self._buffer[data_start:data_end].decode('utf-8')
            del self._buffer[:data_end + 2]
            return value

        if resp_type == '*':
            return self._parse_array()

        return _NOT_ENOUGH_DATA

    def _parse_array(self) -> Any:
        """Parse RESP array from buffer.
        Returns _NOT_ENOUGH_DATA if buffer is incomplete, otherwise the parsed array.
        """
        cr = self._buffer.find(b'\r\n')
        if cr == -1:
            return _NOT_ENOUGH_DATA

        count = int(self._buffer[1:cr])
        pos = cr + 2

        if count < 0:
            del self._buffer[:pos]
            return None

        result = []
        for _ in range(count):
            if pos >= len(self._buffer):
                return _NOT_ENOUGH_DATA
            elem_type = chr(self._buffer[pos])

            if elem_type == '$':
                elem_cr = self._buffer.find(b'\r\n', pos)
                if elem_cr == -1:
                    return _NOT_ENOUGH_DATA
                elem_len = int(self._buffer[pos + 1:elem_cr])
                if elem_len < 0:
                    result.append(None)
                    pos = elem_cr + 2
                else:
                    data_start = elem_cr + 2
                    data_end = data_start + elem_len
                    if data_end + 2 > len(self._buffer):
                        return _NOT_ENOUGH_DATA
                    result.append(self._buffer[data_start:data_end].decode('utf-8'))
                    pos = data_end + 2
            else:
                return _NOT_ENOUGH_DATA

        del self._buffer[:pos]
        return result

    @staticmethod
    def _build_resp(args: List[str]) -> bytes:
        """Build RESP array command."""
        r = f'*{len(args)}\r\n'
        for arg in args:
            r += f'${len(arg)}\r\n{arg}\r\n'
        return r.encode('utf-8')

    # ─── Core Operations ───

    def set(self, key: str, value: str) -> bool:
        """Set a key-value pair."""
        resp = self._send_command(['SET', key, value])
        return resp == 'OK'

    def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return self._send_command(['GET', key])

    def delete(self, key: str) -> bool:
        """Delete a key."""
        resp = self._send_command(['DEL', key])
        return resp == 1

    def delete_range(self, begin: str, end: str) -> bool:
        """Delete a range of keys [begin, end)."""
        resp = self._send_command(['DELRANGE', begin, end])
        return resp == 1

    def ping(self) -> bool:
        """Ping the server."""
        resp = self._send_command(['PING'])
        return resp == 'PONG'

    def stats(self) -> Dict[str, str]:
        """Get server stats."""
        resp = self._send_command(['STATS'])
        if not isinstance(resp, list):
            return {}
        result = {}
        for i in range(0, len(resp), 2):
            result[resp[i]] = resp[i + 1]
        return result

    def quit(self) -> None:
        """Quit and disconnect."""
        try:
            self._send_command(['QUIT'])
        except Exception:
            pass
        self.disconnect()

    # ─── String Extension Commands ───

    def incr(self, key: str) -> Optional[int]:
        """Increment key by 1."""
        return self._send_command(['INCR', key])

    def decr(self, key: str) -> Optional[int]:
        """Decrement key by 1."""
        return self._send_command(['DECR', key])

    def incr_by(self, key: str, delta: int) -> Optional[int]:
        """Increment key by delta."""
        return self._send_command(['INCRBY', key, str(delta)])

    def decr_by(self, key: str, delta: int) -> Optional[int]:
        """Decrement key by delta."""
        return self._send_command(['DECRBY', key, str(delta)])

    def incr_by_float(self, key: str, delta: float) -> Optional[str]:
        """Increment key by float delta."""
        return self._send_command(['INCRBYFLOAT', key, str(delta)])

    def mset(self, kvs: List[List[str]]) -> bool:
        """Set multiple key-value pairs.
        Args:
            kvs: List of [key, value] pairs.
        """
        args = ['MSET']
        for kv in kvs:
            args.append(kv[0])
            args.append(kv[1])
        resp = self._send_command(args)
        return resp == 'OK'

    def mget(self, keys: List[str]) -> List[Optional[str]]:
        """Get multiple keys at once."""
        return self._send_command(['MGET'] + keys)

    def set_ex(self, key: str, seconds: int, value: str) -> bool:
        """Set key with expiration in seconds."""
        resp = self._send_command(['SETEX', key, str(seconds), value])
        return resp == 'OK'

    def set_nx(self, key: str, value: str) -> bool:
        """Set key only if it does not exist."""
        resp = self._send_command(['SETNX', key, value])
        return resp == 1

    def get_set(self, key: str, value: str) -> Optional[str]:
        """Set key and return the old value."""
        return self._send_command(['GETSET', key, value])

    def append(self, key: str, value: str) -> Optional[int]:
        """Append value to key's current value. Returns new length."""
        return self._send_command(['APPEND', key, value])

    def str_len(self, key: str) -> Optional[int]:
        """Get string length of key's value."""
        return self._send_command(['STRLEN', key])

    # ─── General Commands ───

    def exists(self, keys: List[str]) -> Optional[int]:
        """Check if keys exist. Returns count."""
        return self._send_command(['EXISTS'] + keys)

    def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration in seconds."""
        resp = self._send_command(['EXPIRE', key, str(seconds)])
        return resp == 1

    def ttl(self, key: str) -> Optional[int]:
        """Get key's time-to-live in seconds."""
        return self._send_command(['TTL', key])

    def pttl(self, key: str) -> Optional[int]:
        """Get key's time-to-live in milliseconds."""
        return self._send_command(['PTTL', key])

    def persist(self, key: str) -> bool:
        """Remove key's expiration."""
        resp = self._send_command(['PERSIST', key])
        return resp == 1

    def type(self, key: str) -> Optional[str]:
        """Get key's data type."""
        return self._send_command(['TYPE', key])

    def rename(self, key: str, new_key: str) -> bool:
        """Rename key."""
        resp = self._send_command(['RENAME', key, new_key])
        return resp == 'OK'

    def rename_nx(self, key: str, new_key: str) -> bool:
        """Rename key only if new key does not exist."""
        resp = self._send_command(['RENAMENX', key, new_key])
        return resp == 1

    def keys(self, pattern: str) -> List[str]:
        """Get all keys matching a pattern."""
        resp = self._send_command(['KEYS', pattern])
        if isinstance(resp, list):
            return resp
        return []

    # ─── Hash Commands ───

    def hset(self, key: str, fields: List[List[str]]) -> int:
        """Set one or more hash fields. Returns number of new fields."""
        args = ['HSET', key]
        for f in fields:
            args.extend(f)
        resp = self._send_command(args)
        return resp if isinstance(resp, int) else 0

    def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value."""
        return self._send_command(['HGET', key, field])

    def hmset(self, key: str, fields: List[List[str]]) -> bool:
        """Set multiple hash fields."""
        args = ['HMSET', key]
        for f in fields:
            args.extend(f)
        resp = self._send_command(args)
        return resp == 'OK'

    def hmget(self, key: str, fields: List[str]) -> List[Optional[str]]:
        """Get multiple hash field values."""
        resp = self._send_command(['HMGET', key] + fields)
        if isinstance(resp, list):
            return resp
        return []

    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields and values."""
        resp = self._send_command(['HGETALL', key])
        if not isinstance(resp, list):
            return {}
        result = {}
        for i in range(0, len(resp), 2):
            result[resp[i]] = resp[i + 1]
        return result

    def hdel(self, key: str, fields: List[str]) -> int:
        """Delete hash fields. Returns number of deleted fields."""
        resp = self._send_command(['HDEL', key] + fields)
        return resp if isinstance(resp, int) else 0

    def hexists(self, key: str, field: str) -> bool:
        """Check if hash field exists."""
        resp = self._send_command(['HEXISTS', key, field])
        return resp == 1

    def hlen(self, key: str) -> int:
        """Get number of fields in hash."""
        resp = self._send_command(['HLEN', key])
        return resp if isinstance(resp, int) else 0

    def hkeys(self, key: str) -> List[str]:
        """Get all field names in hash."""
        resp = self._send_command(['HKEYS', key])
        if isinstance(resp, list):
            return resp
        return []

    def hvals(self, key: str) -> List[str]:
        """Get all field values in hash."""
        resp = self._send_command(['HVALS', key])
        if isinstance(resp, list):
            return resp
        return []

    def hincrby(self, key: str, field: str, delta: int = 1) -> int:
        """Increment hash field by integer delta."""
        resp = self._send_command(['HINCRBY', key, field, str(delta)])
        return resp if isinstance(resp, int) else 0

    def hstrlen(self, key: str, field: str) -> int:
        """Get string length of hash field value."""
        resp = self._send_command(['HSTRLEN', key, field])
        return resp if isinstance(resp, int) else 0

    # ─── List Commands ───

    def lpush(self, key: str, values: List[str]) -> int:
        """Prepend values to list. Returns list length."""
        resp = self._send_command(['LPUSH', key] + values)
        return resp if isinstance(resp, int) else 0

    def rpush(self, key: str, values: List[str]) -> int:
        """Append values to list. Returns list length."""
        resp = self._send_command(['RPUSH', key] + values)
        return resp if isinstance(resp, int) else 0

    def lpop(self, key: str) -> Optional[str]:
        """Remove and get first element of list."""
        return self._send_command(['LPOP', key])

    def rpop(self, key: str) -> Optional[str]:
        """Remove and get last element of list."""
        return self._send_command(['RPOP', key])

    def lrange(self, key: str, start: int, stop: int) -> List[str]:
        """Get range of elements from list."""
        resp = self._send_command(['LRANGE', key, str(start), str(stop)])
        if isinstance(resp, list):
            return resp
        return []

    def llen(self, key: str) -> int:
        """Get list length."""
        resp = self._send_command(['LLEN', key])
        return resp if isinstance(resp, int) else 0

    def lindex(self, key: str, idx: int) -> Optional[str]:
        """Get element at index in list."""
        return self._send_command(['LINDEX', key, str(idx)])

    def lset(self, key: str, idx: int, value: str) -> bool:
        """Set element at index in list."""
        resp = self._send_command(['LSET', key, str(idx), value])
        return resp == 'OK'

    def ltrim(self, key: str, start: int, stop: int) -> bool:
        """Trim list to specified range."""
        resp = self._send_command(['LTRIM', key, str(start), str(stop)])
        return resp == 'OK'

    def lrem(self, key: str, count: int, value: str) -> int:
        """Remove elements from list. Returns number of removed elements."""
        resp = self._send_command(['LREM', key, str(count), value])
        return resp if isinstance(resp, int) else 0

    # ─── Set Commands ───

    def sadd(self, key: str, members: List[str]) -> int:
        """Add members to set. Returns number of added members."""
        resp = self._send_command(['SADD', key] + members)
        return resp if isinstance(resp, int) else 0

    def srem(self, key: str, members: List[str]) -> int:
        """Remove members from set. Returns number of removed members."""
        resp = self._send_command(['SREM', key] + members)
        return resp if isinstance(resp, int) else 0

    def smembers(self, key: str) -> List[str]:
        """Get all members of set."""
        resp = self._send_command(['SMEMBERS', key])
        if isinstance(resp, list):
            return resp
        return []

    def sismember(self, key: str, member: str) -> bool:
        """Check if member is in set."""
        resp = self._send_command(['SISMEMBER', key, member])
        return resp == 1

    def scard(self, key: str) -> int:
        """Get number of members in set."""
        resp = self._send_command(['SCARD', key])
        return resp if isinstance(resp, int) else 0

    def spop(self, key: str) -> Optional[str]:
        """Remove and return a random member from set."""
        return self._send_command(['SPOP', key])

    def srandmember(self, key: str) -> Optional[str]:
        """Get a random member from set without removing it."""
        return self._send_command(['SRANDMEMBER', key])

    def smove(self, src: str, dst: str, member: str) -> bool:
        """Move member from source set to destination set."""
        resp = self._send_command(['SMOVE', src, dst, member])
        return resp == 1

    # ─── Bitmap Commands ───

    def setbit(self, key: str, offset: int, value: int) -> int:
        """Set bit at offset. Returns old bit value."""
        resp = self._send_command(['SETBIT', key, str(offset), str(value)])
        return resp if isinstance(resp, int) else 0

    def getbit(self, key: str, offset: int) -> int:
        """Get bit at offset."""
        resp = self._send_command(['GETBIT', key, str(offset)])
        return resp if isinstance(resp, int) else 0

    def bitcount(self, key: str, start: int = 0, end: int = -1) -> int:
        """Count set bits in range."""
        resp = self._send_command(['BITCOUNT', key, str(start), str(end)])
        return resp if isinstance(resp, int) else 0

    def bitpos(self, key: str, bit: int, start: int = 0, end: int = -1) -> int:
        """Find first bit with value in range."""
        resp = self._send_command(['BITPOS', key, str(bit), str(start), str(end)])
        return resp if isinstance(resp, int) else -1

    # ─── HyperLogLog Commands ───

    def pfadd(self, key: str, elements: List[str]) -> int:
        """Add elements to HLL. Returns 1 if changed."""
        resp = self._send_command(['PFADD', key] + elements)
        return resp if isinstance(resp, int) else 0

    def pfcount(self, keys: List[str]) -> int:
        """Get approximate cardinality of HLL(s)."""
        resp = self._send_command(['PFCOUNT'] + keys)
        return resp if isinstance(resp, int) else 0

    def pfmerge(self, dest: str, sources: List[str]) -> bool:
        """Merge multiple HLLs into dest."""
        resp = self._send_command(['PFMERGE', dest] + sources)
        return resp == 'OK'

    # ─── Pipeline Support ───

    def pipeline(self) -> None:
        """Begin buffering commands for pipeline execution."""
        self._pipeline_buf: List[List[str]] = []

    def queue(self, args: List[str]) -> None:
        """Add a command to the pipeline buffer."""
        if not hasattr(self, '_pipeline_buf'):
            self._pipeline_buf = []
        self._pipeline_buf.append(args)

    def exec_pipeline(self) -> List[Any]:
        """Send all queued commands and return their responses."""
        if not self._connected or not self._socket:
            raise ConnectionError('Not connected')
        if not hasattr(self, '_pipeline_buf') or not self._pipeline_buf:
            return []

        # Build all commands
        all_cmds = b''
        for args in self._pipeline_buf:
            all_cmds += self._build_resp(args)
        count = len(self._pipeline_buf)
        self._pipeline_buf = []

        with self._lock:
            self._socket.sendall(all_cmds)

            results = []
            for _ in range(count):
                results.append(self._read_response())
            return results

    # ─── Utility Commands ───

    def auth(self, password: str) -> bool:
        """Authenticate with password."""
        resp = self._send_command(['AUTH', password])
        return resp == 'OK'

    def config_get(self, param: str) -> Dict[str, str]:
        """Get server configuration."""
        resp = self._send_command(['CONFIG', 'GET', param])
        if not isinstance(resp, list):
            return {}
        result = {}
        for i in range(0, len(resp), 2):
            result[resp[i]] = resp[i + 1]
        return result

    def config_set(self, param: str, value: str) -> bool:
        """Set server configuration."""
        resp = self._send_command(['CONFIG', 'SET', param, value])
        return resp == 'OK'

    # ─── ZSet Commands ───

    def zadd(self, key: str, members: List[tuple]) -> int:
        """Add members to sorted set. Returns number of added members.
        Args:
            key: Sorted set key
            members: List of (score, member) tuples
        """
        args = ['ZADD', key]
        for score, member in members:
            args.append(str(score))
            args.append(member)
        resp = self._send_command(args)
        return resp if isinstance(resp, int) else 0

    def zrem(self, key: str, members: List[str]) -> int:
        """Remove members from sorted set. Returns number of removed members."""
        resp = self._send_command(['ZREM', key] + members)
        return resp if isinstance(resp, int) else 0

    def zscore(self, key: str, member: str) -> Optional[float]:
        """Get score of member in sorted set."""
        resp = self._send_command(['ZSCORE', key, member])
        if resp is None:
            return None
        if isinstance(resp, str):
            return float(resp)
        return None

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List[str]:
        """Get range of members from sorted set by index."""
        args = ['ZRANGE', key, str(start), str(stop)]
        if withscores:
            args.append('WITHSCORES')
        resp = self._send_command(args)
        if not isinstance(resp, list):
            return []
        if withscores:
            return resp  # Returns [member, score, member, score, ...]
        return resp

    def zrange_withscores(self, key: str, start: int, stop: int) -> List[tuple]:
        """Get range of members with scores from sorted set."""
        resp = self.zrange(key, start, stop, withscores=True)
        result = []
        for i in range(0, len(resp), 2):
            result.append((resp[i], float(resp[i + 1])))
        return result

    def zcard(self, key: str) -> int:
        """Get number of members in sorted set."""
        resp = self._send_command(['ZCARD', key])
        return resp if isinstance(resp, int) else 0

    def zcount(self, key: str, min_score: str, max_score: str) -> int:
        """Count members with scores in range."""
        resp = self._send_command(['ZCOUNT', key, min_score, max_score])
        return resp if isinstance(resp, int) else 0

    def zrangebyscore(self, key: str, min_score: str, max_score: str,
                      offset: int = 0, count: int = -1, withscores: bool = False) -> List[str]:
        """Get members by score range."""
        args = ['ZRANGEBYSCORE', key, min_score, max_score]
        if offset != 0 or count != -1:
            args.extend(['LIMIT', str(offset), str(count)])
        if withscores:
            args.append('WITHSCORES')
        resp = self._send_command(args)
        if not isinstance(resp, list):
            return []
        return resp

    def zrevrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List[str]:
        """Get range of members from sorted set in reverse order."""
        args = ['ZREVRANGE', key, str(start), str(stop)]
        if withscores:
            args.append('WITHSCORES')
        resp = self._send_command(args)
        if not isinstance(resp, list):
            return []
        return resp

    # ─── Geo Commands ───

    def geoadd(self, key: str, members: List[tuple]) -> int:
        """Add geo members. Returns number of added members.
        Args:
            key: Geo key
            members: List of (longitude, latitude, member) tuples
        """
        args = ['GEOADD', key]
        for longitude, latitude, member in members:
            args.append(str(longitude))
            args.append(str(latitude))
            args.append(member)
        resp = self._send_command(args)
        return resp if isinstance(resp, int) else 0

    def geopos(self, key: str, members: List[str]) -> List[Optional[tuple]]:
        """Get positions of geo members."""
        resp = self._send_command(['GEOPOS', key] + members)
        if not isinstance(resp, list):
            return []
        result = []
        for pos in resp:
            if pos is None:
                result.append(None)
            elif isinstance(pos, list) and len(pos) == 2:
                result.append((float(pos[0]), float(pos[1])))
            else:
                result.append(None)
        return result

    def geodist(self, key: str, member1: str, member2: str, unit: str = 'm') -> Optional[float]:
        """Get distance between two geo members."""
        resp = self._send_command(['GEODIST', key, member1, member2, unit])
        if resp is None:
            return None
        if isinstance(resp, str):
            return float(resp)
        return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
