package lightkv;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * LightKV Java 客户端集成测试
 *
 * 前提：LightKV 服务运行在 localhost:6379
 */
public class LightKVClientTest {
    private static LightKVClient client;

    @BeforeAll
    static void setup() {
        client = new LightKVClient("127.0.0.1", 6379, 3000, 3);
        assertTrue(client.connect(), "Should connect to LightKV server");
    }

    @AfterAll
    static void teardown() {
        client.close();
    }

    @Test
    void testPing() {
        assertTrue(client.ping(), "PING should return true");
    }

    @Test
    void testSetGet() {
        String key = "java:test:" + System.currentTimeMillis();
        assertTrue(client.set(key, "hello"), "SET should succeed");
        assertEquals(Optional.of("hello"), client.get(key), "GET should return value");
        assertTrue(client.delete(key), "DEL should succeed");
    }

    @Test
    void testIncr() {
        String key = "java:counter:" + System.currentTimeMillis();
        assertEquals(1, client.incr(key), "INCR should return 1");
        assertEquals(2, client.incr(key), "INCR should return 2");
        client.delete(key);
    }

    @Test
    void testExpire() {
        String key = "java:exp:" + System.currentTimeMillis();
        assertTrue(client.set(key, "temp"), "SET should succeed");
        assertTrue(client.expire(key, 10), "EXPIRE should return true");
        assertTrue(client.ttl(key) > 0, "TTL should be > 0");
        client.delete(key);
    }
}
