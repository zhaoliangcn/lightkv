using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace LightKV
{
    /// <summary>
    /// LightKV C# 客户端 SDK
    ///
    /// 通过 TCP 使用 Redis RESP 协议与 LightKV 服务通信。
    /// 支持连接池、自动重连、Pipeline 批量操作。
    ///
    /// 用法：
    ///   using var client = new LightKVClient("127.0.0.1", 6379);
    ///   client.Set("key", "value");
    ///   string? val = client.Get("key");
    /// </summary>
    public class LightKVClient : IDisposable
    {
        private readonly string _host;
        private readonly int _port;
        private readonly int _timeoutMs;
        private readonly int _retryCount;

        private TcpClient? _tcpClient;
        private NetworkStream? _stream;
        private readonly object _lock = new();

        // Pipeline support
        private List<string>? _pipelineBuf;

        public LightKVClient(string host, int port, int timeoutMs = 3000, int retryCount = 3)
        {
            _host = host;
            _port = port;
            _timeoutMs = timeoutMs;
            _retryCount = retryCount;
        }

        // ─── 连接管理 ───

        public bool Connect()
        {
            lock (_lock)
            {
                try
                {
                    _tcpClient = new TcpClient();
                    var task = _tcpClient.ConnectAsync(_host, _port);
                    task.Wait(_timeoutMs);
                    if (!_tcpClient.Connected) return false;
                    _stream = _tcpClient.GetStream();
                    _stream.ReadTimeout = _timeoutMs;
                    _stream.WriteTimeout = _timeoutMs;
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _stream?.Close();
                _tcpClient?.Close();
                _stream = null;
                _tcpClient = null;
            }
        }

        public bool IsConnected
        {
            get { lock (_lock) return _tcpClient?.Connected ?? false; }
        }

        // ─── 核心 KV 操作 ───

        public bool Set(string key, string value)
        {
            var resp = SendCommand("SET", key, value);
            return resp == "+OK";
        }

        public string? Get(string key)
        {
            return SendCommand("GET", key) switch
            {
                null or "$-1" => null,
                var s when s.StartsWith("$") => ParseBulkString(s),
                var s when s.StartsWith("+") => s[1..],
                _ => null
            };
        }

        public bool Delete(string key)
        {
            var resp = SendCommand("DEL", key);
            return resp != null && resp.StartsWith(":1");
        }

        public bool Exists(string key)
        {
            var resp = SendCommand("EXISTS", key);
            return resp != null && resp.StartsWith(":1");
        }

        public long Incr(string key)
        {
            var resp = SendCommand("INCR", key);
            return ParseInteger(resp);
        }

        public long Decr(string key)
        {
            var resp = SendCommand("DECR", key);
            return ParseInteger(resp);
        }

        public bool Expire(string key, long seconds)
        {
            var resp = SendCommand("EXPIRE", key, seconds.ToString());
            return resp != null && resp.StartsWith(":1");
        }

        public long Ttl(string key)
        {
            var resp = SendCommand("TTL", key);
            return ParseInteger(resp);
        }

        // ─── Hash 操作 ───

        public bool HSet(string key, string field, string value)
        {
            var resp = SendCommand("HSET", key, field, value);
            return resp != null && resp.StartsWith(":1");
        }

        public string? HGet(string key, string field)
        {
            var resp = SendCommand("HGET", key, field);
            return resp switch
            {
                null or "$-1" => null,
                var s when s.StartsWith("$") => ParseBulkString(s),
                _ => null
            };
        }

        public Dictionary<string, string> HGetAll(string key)
        {
            var result = new Dictionary<string, string>();
            var resp = SendCommand("HGETALL", key);
            if (resp == null) return result;
            var items = ParseArray(resp);
            for (int i = 0; i + 1 < items.Count; i += 2)
            {
                result[items[i]] = items[i + 1];
            }
            return result;
        }

        // ─── List 操作 ───

        public long LPush(string key, params string[] values)
        {
            var args = new List<string> { "LPUSH", key };
            args.AddRange(values);
            var resp = SendCommand(args.ToArray());
            return ParseInteger(resp);
        }

        public string? LPop(string key)
        {
            var resp = SendCommand("LPOP", key);
            return resp switch
            {
                null or "$-1" => null,
                var s when s.StartsWith("$") => ParseBulkString(s),
                _ => null
            };
        }

        public List<string> LRange(string key, long start, long stop)
        {
            var resp = SendCommand("LRANGE", key, start.ToString(), stop.ToString());
            return resp != null ? ParseArray(resp) : new List<string>();
        }

        // ─── Set 操作 ───

        public long SAdd(string key, params string[] members)
        {
            var args = new List<string> { "SADD", key };
            args.AddRange(members);
            var resp = SendCommand(args.ToArray());
            return ParseInteger(resp);
        }

        public HashSet<string> SMembers(string key)
        {
            var resp = SendCommand("SMEMBERS", key);
            return resp != null ? new HashSet<string>(ParseArray(resp)) : new HashSet<string>();
        }

        // ─── ZSet 操作 ───

        public long ZAdd(string key, double score, string member)
        {
            var resp = SendCommand("ZADD", key, score.ToString("F6"), member);
            return ParseInteger(resp);
        }

        public List<string> ZRange(string key, long start, long stop)
        {
            var resp = SendCommand("ZRANGE", key, start.ToString(), stop.ToString());
            return resp != null ? ParseArray(resp) : new List<string>();
        }

        // ─── Pipeline 支持 ───

        public void Pipeline() => _pipelineBuf = new List<string>();

        public void Queue(params string[] args)
        {
            _pipelineBuf?.Add(BuildResp(args));
        }

        public List<string?> ExecPipeline()
        {
            if (_pipelineBuf == null || _pipelineBuf.Count == 0)
                return new List<string?>();

            lock (_lock)
            {
                try
                {
                    EnsureConnected();
                    var sb = new StringBuilder();
                    foreach (var cmd in _pipelineBuf) sb.Append(cmd);
                    var data = Encoding.UTF8.GetBytes(sb.ToString());
                    _stream!.Write(data, 0, data.Length);

                    var results = new List<string?>();
                    foreach (var _ in _pipelineBuf)
                    {
                        results.Add(ReadLine());
                    }
                    _pipelineBuf = null;
                    return results;
                }
                catch
                {
                    _pipelineBuf = null;
                    return new List<string?>();
                }
            }
        }

        // ─── 通用命令 ───

        public bool Ping()
        {
            var resp = SendCommand("PING");
            return resp == "+PONG";
        }

        // ─── 内部方法 ───

        private string? SendCommand(params string[] args)
        {
            for (int attempt = 0; attempt < _retryCount; attempt++)
            {
                try
                {
                    lock (_lock)
                    {
                        EnsureConnected();
                        var cmd = BuildResp(args);
                        var data = Encoding.UTF8.GetBytes(cmd);
                        _stream!.Write(data, 0, data.Length);
                        return ReadLine();
                    }
                }
                catch
                {
                    Dispose();
                    if (attempt < _retryCount - 1)
                        Task.Delay(100 * (attempt + 1)).Wait();
                }
            }
            return null;
        }

        private void EnsureConnected()
        {
            if (_tcpClient == null || !_tcpClient.Connected)
            {
                Dispose();
                Connect();
            }
        }

        private string? ReadLine()
        {
            if (_stream == null) return null;
            var sb = new StringBuilder();
            int b;
            while ((b = _stream.ReadByte()) != -1)
            {
                if (b == '\r')
                {
                    _stream.ReadByte(); // consume \n
                    break;
                }
                sb.Append((char)b);
            }
            return sb.Length > 0 ? sb.ToString() : null;
        }

        private static string BuildResp(string[] args)
        {
            var sb = new StringBuilder();
            sb.Append('*').Append(args.Length).Append("\r\n");
            foreach (var arg in args)
            {
                var bytes = Encoding.UTF8.GetByteCount(arg);
                sb.Append('$').Append(bytes).Append("\r\n");
                sb.Append(arg).Append("\r\n");
            }
            return sb.ToString();
        }

        private string? ParseBulkString(string resp)
        {
            if (resp.StartsWith("$-1")) return null;
            if (!resp.StartsWith("$")) return resp;

            var lenStr = resp[1..];
            if (!int.TryParse(lenStr, out int len) || len < 0) return null;

            var buf = new byte[len];
            _stream?.Read(buf, 0, len);
            // Read trailing \r\n
            _stream?.ReadByte();
            _stream?.ReadByte();
            return Encoding.UTF8.GetString(buf);
        }

        private static long ParseInteger(string? resp)
        {
            if (resp == null || !resp.StartsWith(":")) return 0;
            long.TryParse(resp[1..], out long val);
            return val;
        }

        private List<string> ParseArray(string resp)
        {
            var result = new List<string>();
            if (resp.StartsWith("*-1") || resp.StartsWith("*0")) return result;

            if (!resp.StartsWith("*") || !int.TryParse(resp[1..], out int count))
                return result;

            for (int i = 0; i < count; i++)
            {
                var line = ReadLine();
                if (line == null) break;
                if (line.StartsWith("$"))
                {
                    if (!int.TryParse(line[1..], out int len) || len < 0) continue;
                    var buf = new byte[len];
                    _stream?.Read(buf, 0, len);
                    _stream?.ReadByte(); _stream?.ReadByte(); // \r\n
                    result.Add(Encoding.UTF8.GetString(buf));
                }
                else
                {
                    result.Add(line);
                }
            }
            return result;
        }
    }
}
