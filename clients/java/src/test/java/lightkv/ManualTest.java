package lightkv;

/// 手动测试 LightKV Java SDK（无需 JUnit）
public class ManualTest {
    public static void main(String[] args) throws Exception {
        LightKVClient client = new LightKVClient("127.0.0.1", 16379, 3000, 3);
        
        // 连接
        assert client.connect() : "连接失败";
        System.out.println("[PASS] connect()");

        // AUTH
        assert client.auth("benchpass123") : "AUTH失败";
        System.out.println("[PASS] auth()");

        // PING
        assert client.ping() : "PING失败";
        System.out.println("[PASS] ping()");

        // SET / GET
        assert client.set("hello", "world") : "SET失败";
        System.out.println("[PASS] set()");
        var val = client.get("hello");
        assert val.isPresent() && val.get().equals("world") : "GET返回值不匹配";
        System.out.println("[PASS] get() -> " + val.get());

        // GET 不存在
        var nil = client.get("nonexistent");
        assert nil.isEmpty() : "GET不存在的key应返回empty";
        System.out.println("[PASS] get(nonexistent) -> empty");

        // DELETE
        assert client.delete("hello") : "DELETE失败";
        assert client.get("hello").isEmpty() : "DELETE后GET应为empty";
        System.out.println("[PASS] delete()");

        // INCR
        assert client.incr("counter") == 1 : "INCR应为1";
        assert client.incr("counter") == 2 : "INCR应为2";
        client.delete("counter");
        System.out.println("[PASS] incr()");

        // EXPIRE / TTL
        client.set("tmp", "val");
        client.expire("tmp", 10);
        assert client.ttl("tmp") > 0 : "TTL应>0";
        client.delete("tmp");
        System.out.println("[PASS] expire()/ttl()");

        // Hash
        client.hset("hash1", "field1", "val1");
        var hval = client.hget("hash1", "field1");
        assert hval.isPresent() && hval.get().equals("val1") : "HGET失败";
        client.delete("hash1");
        System.out.println("[PASS] hset()/hget()");

        // List
        client.lpush("list1", "a", "b");
        var items = client.lrange("list1", 0, -1);
        assert items.size() == 2 : "LRANGE应返回2个元素";
        client.delete("list1");
        System.out.println("[PASS] lpush()/lrange()");

        // ZSet
        client.zadd("zset1", 100.0, "player1");
        client.zadd("zset1", 200.0, "player2");
        var zitems = client.zrange("zset1", 0, -1);
        assert zitems.size() == 2 : "ZRANGE应返回2个元素";
        client.delete("zset1");
        System.out.println("[PASS] zadd()/zrange()");

        // Pipeline
        client.pipeline();
        client.queue("SET", "p1", "v1");
        client.queue("SET", "p2", "v2");
        var results = client.execPipeline();
        assert results.size() == 2 : "Pipeline应返回2个结果";
        client.delete("p1");
        client.delete("p2");
        System.out.println("[PASS] pipeline()");

        client.close();
        System.out.println("\n[All Java SDK tests passed!]");
    }
}
