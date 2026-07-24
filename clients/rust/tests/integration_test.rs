/// Rust SDK 集成测试 — 连接运行中的 LightKV 服务器
///
/// 前提：LightKV 服务器运行在 127.0.0.1:16379，密码 benchpass123
#[cfg(test)]
mod integration_tests {
    use lightkv_client::LightKVClient;

    fn connect() -> LightKVClient {
        let mut client = LightKVClient::new("127.0.0.1:16379").expect("连接失败");
        client.auth("benchpass123").expect("AUTH失败");
        client
    }

    #[test]
    fn test_ping() {
        let mut client = connect();
        assert!(client.ping().unwrap(), "PING should return true");
    }

    #[test]
    fn test_set_get() {
        let mut client = connect();
        assert!(client.set("rust:hello", "world").unwrap(), "SET should succeed");
        let val = client.get("rust:hello").unwrap();
        assert_eq!(val, Some("world".to_string()), "GET should return 'world'");
        client.delete("rust:hello").unwrap();
    }

    #[test]
    fn test_get_non_existent() {
        let mut client = connect();
        let val = client.get("rust:nonexistent").unwrap();
        assert_eq!(val, None, "GET non-existent should return None");
    }

    #[test]
    fn test_delete() {
        let mut client = connect();
        client.set("rust:tmp", "val").unwrap();
        assert!(client.delete("rust:tmp").unwrap(), "DELETE should succeed");
        let val = client.get("rust:tmp").unwrap();
        assert_eq!(val, None, "GET after DELETE should return None");
    }

    #[test]
    fn test_incr() {
        let mut client = connect();
        let key = "rust:counter";
        assert_eq!(client.incr(key).unwrap(), 1, "INCR should return 1");
        assert_eq!(client.incr(key).unwrap(), 2, "INCR should return 2");
        client.delete(key).unwrap();
    }

    #[test]
    fn test_expire_ttl() {
        let mut client = connect();
        client.set("rust:exp", "val").unwrap();
        assert!(client.expire("rust:exp", 10).unwrap(), "EXPIRE should succeed");
        let ttl = client.ttl("rust:exp").unwrap();
        assert!(ttl > 0, "TTL should be > 0, got {}", ttl);
        client.delete("rust:exp").unwrap();
    }

    #[test]
    fn test_hash() {
        let mut client = connect();
        assert!(client.hset("rust:hash", "field1", "val1").unwrap());
        let val = client.hget("rust:hash", "field1").unwrap();
        assert_eq!(val, Some("val1".to_string()));
        client.delete("rust:hash").unwrap();
    }

    #[test]
    fn test_list() {
        let mut client = connect();
        let list_key = format!("rust:list:{}", std::process::id());
        let r = client.lpush(&list_key, &["a", "b"]);
        assert!(r.is_ok(), "LPUSH error: {:?}", r.err());
        let items = client.lrange(&list_key, 0, -1).unwrap();
        assert_eq!(items.len(), 2, "LRANGE should return 2 items");
        let popped = client.lpop(&list_key).unwrap();
        assert!(popped.is_some());
        client.delete(&list_key).unwrap();
    }

    #[test]
    fn test_zset() {
        let mut client = connect();
        let zset_key = format!("rust:zset:{}", std::process::id());
        client.zadd(&zset_key, 100.0, "p1").unwrap();
        client.zadd(&zset_key, 200.0, "p2").unwrap();
        let items = client.zrange(&zset_key, 0, -1).unwrap();
        assert_eq!(items.len(), 2);
        client.delete(&zset_key).unwrap();
    }

    #[test]
    fn test_pipeline() {
        let mut client = connect();
        client.pipeline();
        client.queue(&["SET", "rust:p1", "v1"]);
        client.queue(&["SET", "rust:p2", "v2"]);
        let results = client.exec_pipeline().unwrap();
        assert_eq!(results.len(), 2, "Pipeline should return 2 results");
        client.delete("rust:p1").unwrap();
        client.delete("rust:p2").unwrap();
    }
}
