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

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        return False
