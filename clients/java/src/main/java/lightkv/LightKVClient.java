package lightkv;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * LightKV Java 客户端 SDK
 *
 * 通过 TCP 使用 Redis RESP 协议与 LightKV 服务通信。
 * 支持连接池、自动重连、Pipeline 批量操作。
 *
 * 用法：
 *   LightKVClient client = new LightKVClient("127.0.0.1", 6379);
 *   client.set("key", "value");
 *   String val = client.get("key").orElse(null);
 *   client.close();
 */
public class LightKVClient implements AutoCloseable {
    private final String host;
    private final int port;
    private final int timeoutMs;
    private final int retryCount;

    private Socket socket;
    private BufferedReader reader;
    private OutputStream writer;
    private String lastError;
    private final Object lock = new Object();

    // Pipeline support
    private List<String> pipelineBuf;

    public LightKVClient(String host, int port) {
        this(host, port, 3000, 3);
    }

    public LightKVClient(String host, int port, int timeoutMs, int retryCount) {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.retryCount = retryCount;
    }

    // ─── 连接管理 ───

    public boolean connect() {
        synchronized (lock) {
            try {
                socket = new Socket();
                socket.connect(new InetSocketAddress(host, port), timeoutMs);
                socket.setSoTimeout(timeoutMs);
                reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
                writer = socket.getOutputStream();
                lastError = null;
                return true;
            } catch (IOException e) {
                lastError = e.getMessage();
                return false;
            }
        }
    }

    public void close() {
        synchronized (lock) {
            try { if (socket != null) socket.close(); } catch (IOException ignored) {}
            socket = null;
            reader = null;
            writer = null;
        }
    }

    public boolean isConnected() {
        synchronized (lock) {
            return socket != null && socket.isConnected() && !socket.isClosed();
        }
    }

    public String getLastError() { return lastError; }

    // ─── 核心 KV 操作 ───

    public boolean set(String key, String value) {
        return sendCommand("SET", key, value).map(r -> r.equals("+OK\r\n")).orElse(false);
    }

    public Optional<String> get(String key) {
        return sendCommand("GET", key).flatMap(this::parseBulkString);
    }

    public boolean delete(String key) {
        return sendCommand("DEL", key).map(r -> r.startsWith(":1")).orElse(false);
    }

    public boolean exists(String key) {
        return sendCommand("EXISTS", key).map(r -> r.startsWith(":1")).orElse(false);
    }

    public long incr(String key) {
        return sendCommand("INCR", key).map(this::parseInteger).orElse(0L);
    }

    public long decr(String key) {
        return sendCommand("DECR", key).map(this::parseInteger).orElse(0L);
    }

    public boolean expire(String key, long seconds) {
        return sendCommand("EXPIRE", key, String.valueOf(seconds))
                .map(r -> r.startsWith(":1")).orElse(false);
    }

    public long ttl(String key) {
        return sendCommand("TTL", key).map(this::parseInteger).orElse(-2L);
    }

    // ─── Hash 操作 ───

    public boolean hset(String key, String field, String value) {
        return sendCommand("HSET", key, field, value).map(r -> r.startsWith(":1")).orElse(false);
    }

    public Optional<String> hget(String key, String field) {
        return sendCommand("HGET", key, field).flatMap(this::parseBulkString);
    }

    public Map<String, String> hgetAll(String key) {
        return sendCommand("HGETALL", key).map(this::parseArray).orElse(Collections.emptyList())
                .stream().collect(HashMap::new, (m, v) -> {
                    // pairs: k1, v1, k2, v2, ...
                }, HashMap::putAll);
    }

    // ─── List 操作 ───

    public long lpush(String key, String... values) {
        List<String> args = new ArrayList<>(Arrays.asList("LPUSH", key));
        args.addAll(Arrays.asList(values));
        return sendCommand(args.toArray(new String[0])).map(this::parseInteger).orElse(0L);
    }

    public Optional<String> lpop(String key) {
        return sendCommand("LPOP", key).flatMap(this::parseBulkString);
    }

    public List<String> lrange(String key, long start, long stop) {
        return sendCommand("LRANGE", key, String.valueOf(start), String.valueOf(stop))
                .map(this::parseArray).orElse(Collections.emptyList());
    }

    // ─── Set 操作 ───

    public long sadd(String key, String... members) {
        List<String> args = new ArrayList<>(Arrays.asList("SADD", key));
        args.addAll(Arrays.asList(members));
        return sendCommand(args.toArray(new String[0])).map(this::parseInteger).orElse(0L);
    }

    public Set<String> smembers(String key) {
        return new HashSet<>(sendCommand("SMEMBERS", key)
                .map(this::parseArray).orElse(Collections.emptyList()));
    }

    // ─── ZSet 操作 ───

    public long zadd(String key, double score, String member) {
        return sendCommand("ZADD", key, String.valueOf(score), member)
                .map(this::parseInteger).orElse(0L);
    }

    public List<String> zrange(String key, long start, long stop) {
        return sendCommand("ZRANGE", key, String.valueOf(start), String.valueOf(stop))
                .map(this::parseArray).orElse(Collections.emptyList());
    }

    // ─── Pipeline 支持 ───

    public void pipeline() {
        pipelineBuf = new ArrayList<>();
    }

    public void queue(String... args) {
        if (pipelineBuf != null) {
            pipelineBuf.add(buildResp(args));
        }
    }

    public List<Optional<String>> execPipeline() {
        if (pipelineBuf == null || pipelineBuf.isEmpty()) return Collections.emptyList();
        synchronized (lock) {
            try {
                StringBuilder sb = new StringBuilder();
                for (String cmd : pipelineBuf) sb.append(cmd);
                writer.write(sb.toString().getBytes("UTF-8"));
                writer.flush();

                List<Optional<String>> results = new ArrayList<>();
                for (int i = 0; i < pipelineBuf.size(); i++) {
                    String line = reader.readLine();
                    results.add(Optional.ofNullable(line));
                }
                pipelineBuf = null;
                return results;
            } catch (IOException e) {
                lastError = e.getMessage();
                pipelineBuf = null;
                return Collections.emptyList();
            }
        }
    }

    // ─── 内部方法 ───

    private Optional<String> sendCommand(String... args) {
        synchronized (lock) {
            for (int attempt = 0; attempt < retryCount; attempt++) {
                try {
                    if (socket == null || !socket.isConnected()) {
                        if (!connect()) continue;
                    }
                    String cmd = buildResp(args);
                    writer.write(cmd.getBytes("UTF-8"));
                    writer.flush();
                    return Optional.of(reader.readLine());
                } catch (IOException e) {
                    lastError = e.getMessage();
                    close();
                    try { Thread.sleep(100L * (attempt + 1)); } catch (InterruptedException ignored) {}
                }
            }
            return Optional.empty();
        }
    }

    private String buildResp(String... args) {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(args.length).append("\r\n");
        for (String arg : args) {
            byte[] bytes = arg.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(arg).append("\r\n");
        }
        return sb.toString();
    }

    private Optional<String> parseBulkString(String resp) {
        if (resp == null || resp.startsWith("$-1")) return Optional.empty();
        if (resp.startsWith("+")) return Optional.of(resp.substring(1));
        if (resp.startsWith("$")) {
            // Read the actual data following the length
            try {
                int len = Integer.parseInt(resp.substring(1));
                if (len < 0) return Optional.empty();
                char[] data = new char[len];
                int read = reader.read(data, 0, len);
                reader.readLine(); // consume trailing \r\n
                return Optional.of(new String(data, 0, read));
            } catch (IOException e) {
                lastError = e.getMessage();
                return Optional.empty();
            }
        }
        return Optional.of(resp);
    }

    private long parseInteger(String resp) {
        if (resp == null || resp.isEmpty()) return 0;
        try {
            if (resp.startsWith(":")) return Long.parseLong(resp.substring(1));
        } catch (NumberFormatException ignored) {}
        return 0;
    }

    private List<String> parseArray(String resp) {
        List<String> result = new ArrayList<>();
        if (resp == null || resp.isEmpty() || resp.startsWith("*-1") || resp.startsWith("*0")) return result;
        try {
            int count = Integer.parseInt(resp.substring(1));
            for (int i = 0; i < count; i++) {
                String line = reader.readLine();
                if (line != null && line.startsWith("$")) {
                    int len = Integer.parseInt(line.substring(1));
                    if (len >= 0) {
                        char[] data = new char[len];
                        reader.read(data, 0, len);
                        result.add(new String(data));
                        reader.readLine(); // consume trailing \r\n
                    }
                }
            }
        } catch (IOException | NumberFormatException e) {
            lastError = e.getMessage();
        }
        return result;
    }
}
