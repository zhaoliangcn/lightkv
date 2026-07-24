/// LightKV Rust 客户端 SDK
///
/// 通过 TCP 使用 Redis RESP 协议与 LightKV 服务通信。
/// 支持连接池、自动重连、Pipeline 批量操作。
///
/// 用法：
/// ```rust,no_run
/// use lightkv_client::LightKVClient;
/// let mut client = LightKVClient::new("127.0.0.1:6379").unwrap();
/// client.set("key", "value").unwrap();
/// let val = client.get("key").unwrap();
/// println!("Got: {:?}", val);
/// ```

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// LightKV 客户端
pub struct LightKVClient {
    stream: Option<TcpStream>,
    reader: Option<BufReader<TcpStream>>,
    addr: String,
    timeout_ms: u64,
    retry_count: u32,
    // Pipeline support
    pipeline_buf: Vec<Vec<u8>>,
}

impl LightKVClient {
    /// 创建新的客户端连接
    pub fn new<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self> {
        let addr_str = format!("{:?}", addr);
        let mut client = LightKVClient {
            stream: None,
            reader: None,
            addr: addr_str,
            timeout_ms: 3000,
            retry_count: 3,
            pipeline_buf: Vec::new(),
        };
        client.connect()?;
        Ok(client)
    }

    /// 连接到服务器
    pub fn connect(&mut self) -> std::io::Result<()> {
        let timeout = Duration::from_millis(self.timeout_ms);
        let addrs = self.addr.to_socket_addrs()?;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, timeout) {
                Ok(stream) => {
                    stream.set_read_timeout(Some(timeout))?;
                    stream.set_write_timeout(Some(timeout))?;
                    self.reader = Some(BufReader::new(stream.try_clone()?));
                    self.stream = Some(stream);
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::NotConnected, "连接失败"))
    }

    /// 断开连接
    pub fn disconnect(&mut self) {
        self.stream = None;
        self.reader = None;
    }

    /// 是否已连接
    pub fn is_connected(&self) -> bool {
        self.stream.is_some()
    }

    // ─── 核心 KV 操作 ───

    /// SET key value
    pub fn set(&mut self, key: &str, value: &str) -> Result<bool, String> {
        let resp = self.send_command(&["SET", key, value])?;
        Ok(resp == "+OK")
    }

    /// GET key
    pub fn get(&mut self, key: &str) -> Result<Option<String>, String> {
        let resp = self.send_command(&["GET", key])?;
        self.parse_bulk_string(&resp)
    }

    /// DEL key
    pub fn delete(&mut self, key: &str) -> Result<bool, String> {
        let resp = self.send_command(&["DEL", key])?;
        Ok(resp.starts_with(":1"))
    }

    /// EXISTS key
    pub fn exists(&mut self, key: &str) -> Result<bool, String> {
        let resp = self.send_command(&["EXISTS", key])?;
        Ok(resp.starts_with(":1"))
    }

    /// INCR key
    pub fn incr(&mut self, key: &str) -> Result<i64, String> {
        let resp = self.send_command(&["INCR", key])?;
        self.parse_integer(&resp)
    }

    /// DECR key
    pub fn decr(&mut self, key: &str) -> Result<i64, String> {
        let resp = self.send_command(&["DECR", key])?;
        self.parse_integer(&resp)
    }

    /// EXPIRE key seconds
    pub fn expire(&mut self, key: &str, seconds: i64) -> Result<bool, String> {
        let resp = self.send_command(&["EXPIRE", key, &seconds.to_string()])?;
        Ok(resp.starts_with(":1"))
    }

    /// TTL key
    pub fn ttl(&mut self, key: &str) -> Result<i64, String> {
        let resp = self.send_command(&["TTL", key])?;
        self.parse_integer(&resp)
    }

    // ─── Hash 操作 ───

    /// HSET key field value
    pub fn hset(&mut self, key: &str, field: &str, value: &str) -> Result<bool, String> {
        let resp = self.send_command(&["HSET", key, field, value])?;
        Ok(resp.starts_with(":1"))
    }

    /// HGET key field
    pub fn hget(&mut self, key: &str, field: &str) -> Result<Option<String>, String> {
        let resp = self.send_command(&["HGET", key, field])?;
        self.parse_bulk_string(&resp)
    }

    /// HGETALL key
    pub fn hgetall(&mut self, key: &str) -> Result<HashMap<String, String>, String> {
        let resp = self.send_command(&["HGETALL", key])?;
        let items = self.parse_array(&resp)?;
        let mut map = HashMap::new();
        for chunk in items.chunks(2) {
            if chunk.len() == 2 {
                map.insert(chunk[0].clone(), chunk[1].clone());
            }
        }
        Ok(map)
    }

    // ─── List 操作 ───

    /// LPUSH key value [value ...]
    pub fn lpush(&mut self, key: &str, values: &[&str]) -> Result<i64, String> {
        let mut args = vec!["LPUSH", key];
        for v in values { args.push(v); }
        let resp = self.send_command(&args)?;
        self.parse_integer(&resp)
    }

    /// LPOP key
    pub fn lpop(&mut self, key: &str) -> Result<Option<String>, String> {
        let resp = self.send_command(&["LPOP", key])?;
        self.parse_bulk_string(&resp)
    }

    /// LRANGE key start stop
    pub fn lrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let resp = self.send_command(&["LRANGE", key, &start.to_string(), &stop.to_string()])?;
        self.parse_array(&resp)
    }

    // ─── Set 操作 ───

    /// SADD key member [member ...]
    pub fn sadd(&mut self, key: &str, members: &[&str]) -> Result<i64, String> {
        let mut args = vec!["SADD", key];
        for m in members { args.push(m); }
        let resp = self.send_command(&args)?;
        self.parse_integer(&resp)
    }

    /// SMEMBERS key
    pub fn smembers(&mut self, key: &str) -> Result<HashSet<String>, String> {
        let resp = self.send_command(&["SMEMBERS", key])?;
        let items = self.parse_array(&resp)?;
        Ok(items.into_iter().collect())
    }

    // ─── ZSet 操作 ───

    /// ZADD key score member
    pub fn zadd(&mut self, key: &str, score: f64, member: &str) -> Result<i64, String> {
        let resp = self.send_command(&["ZADD", key, &score.to_string(), member])?;
        self.parse_integer(&resp)
    }

    /// ZRANGE key start stop
    pub fn zrange(&mut self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let resp = self.send_command(&["ZRANGE", key, &start.to_string(), &stop.to_string()])?;
        self.parse_array(&resp)
    }

    // ─── 通用命令 ───

    /// PING
    pub fn ping(&mut self) -> Result<bool, String> {
        let resp = self.send_command(&["PING"])?;
        Ok(resp == "+PONG")
    }

    // ─── Pipeline 支持 ───

    /// 开始 pipeline 模式
    pub fn pipeline(&mut self) {
        self.pipeline_buf.clear();
    }

    /// 添加命令到 pipeline
    pub fn queue(&mut self, args: &[&str]) {
        self.pipeline_buf.push(build_resp(args));
    }

    /// 执行 pipeline 并返回响应
    pub fn exec_pipeline(&mut self) -> Result<Vec<String>, String> {
        if self.pipeline_buf.is_empty() {
            return Ok(Vec::new());
        }

        let stream = self.stream.as_mut().ok_or("未连接")?;
        let data: Vec<u8> = self.pipeline_buf.iter().flat_map(|b| b.clone()).collect();
        stream.write_all(&data).map_err(|e| e.to_string())?;

        let mut results = Vec::new();
        for _ in 0..self.pipeline_buf.len() {
            results.push(self.read_line()?);
        }
        self.pipeline_buf.clear();
        Ok(results)
    }

    // ─── 内部方法 ───

    fn send_command(&mut self, args: &[&str]) -> Result<String, String> {
        for attempt in 0..self.retry_count {
            // 检查连接状态
            if self.stream.is_none() {
                if attempt > 0 {
                    std::thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                }
                if self.connect().is_err() {
                    continue;
                }
            }

            let stream = self.stream.as_mut().ok_or("未连接")?;
            let cmd = build_resp(args);

            match stream.write_all(&cmd) {
                Ok(_) => {
                    stream.flush().map_err(|e| e.to_string())?;
                    return self.read_line();
                }
                Err(e) => {
                    self.disconnect();
                    if attempt == self.retry_count - 1 {
                        return Err(e.to_string());
                    }
                }
            }
        }
        Err("发送命令失败".to_string())
    }

    fn read_line(&mut self) -> Result<String, String> {
        let reader = self.reader.as_mut().ok_or("读取器未初始化")?;
        let mut line = String::new();
        reader.read_line(&mut line).map_err(|e| e.to_string())?;
        Ok(line.trim_end_matches("\r\n").to_string())
    }

    fn parse_bulk_string(&self, resp: &str) -> Result<Option<String>, String> {
        if resp == "$-1" || resp.is_empty() {
            return Ok(None);
        }
        if resp.starts_with('$') {
            let len: usize = resp[1..].parse().map_err(|_| "解析长度失败")?;
            // 从 reader 读取实际数据
            let reader = self.reader.as_ref().ok_or("读取器未初始化")?;
            let mut buf = reader.get_ref().try_clone().map_err(|e| e.to_string())?;
            let mut data = vec![0u8; len + 2]; // +2 for \r\n
            buf.read_exact(&mut data).map_err(|e| e.to_string())?;
            data.truncate(len);
            return Ok(Some(String::from_utf8_lossy(&data).to_string()));
        }
        if resp.starts_with('+') {
            return Ok(Some(resp[1..].to_string()));
        }
        Ok(Some(resp.to_string()))
    }

    fn parse_integer(&self, resp: &str) -> Result<i64, String> {
        if resp.starts_with(':') {
            resp[1..].parse().map_err(|e| format!("解析整数失败: {}", e))
        } else {
            Ok(0)
        }
    }

    fn parse_array(&mut self, resp: &str) -> Result<Vec<String>, String> {
        let mut result = Vec::new();
        if resp == "*-1" || resp == "*0" || !resp.starts_with('*') {
            return Ok(result);
        }

        let count: usize = resp[1..].parse().map_err(|_| "解析数组长度失败")?;
        for _ in 0..count {
            let line = self.read_line()?;
            if line.starts_with('$') {
                let len: isize = line[1..].parse().map_err(|_| "解析批量字符串长度失败")?;
                if len < 0 { continue; }
                let mut data = vec![0u8; len as usize + 2]; // +2 for \r\n
                let stream = self.stream.as_mut().ok_or("未连接")?;
                stream.read_exact(&mut data).map_err(|e| e.to_string())?;
                data.truncate(len as usize);
                result.push(String::from_utf8_lossy(&data).to_string());
            } else {
                result.push(line);
            }
        }
        Ok(result)
    }
}

fn build_resp(args: &[&str]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(b"*");
    buf.extend_from_slice(args.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    for arg in args {
        buf.extend_from_slice(b"$");
        buf.extend_from_slice(arg.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(arg.as_bytes());
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_resp() {
        let resp = build_resp(&["SET", "key", "value"]);
        let expected = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(&resp[..], &expected[..]);
    }
}
