package lightkv

import (
	"testing"
	"time"
)

func newTestClient(t *testing.T) *Client {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPing(t *testing.T) {
	c := newTestClient(t)
	ok, err := c.Ping()
	if err != nil {
		t.Fatalf("Ping error: %v", err)
	}
	if !ok {
		t.Fatal("Ping should return true")
	}
	t.Log("PONG received")
}

func TestSetGet(t *testing.T) {
	c := newTestClient(t)

	ok, err := c.Set("hello", "world")
	if err != nil || !ok {
		t.Fatalf("Set failed: %v", err)
	}

	val, found, err := c.Get("hello")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found || val != "world" {
		t.Fatalf("Get should return 'world', got '%s' (found=%v)", val, found)
	}
	t.Logf("GET hello = %s", val)
}

func TestGetNonExistent(t *testing.T) {
	c := newTestClient(t)

	val, found, err := c.Get("nonexistent_key_xyz")
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if found {
		t.Fatalf("Get non-existent should return found=false, got val='%s'", val)
	}
	t.Log("GET nonexistent = not found")
}

func TestDelete(t *testing.T) {
	c := newTestClient(t)

	c.Set("to_delete", "value")
	ok, err := c.Delete("to_delete")
	if err != nil || !ok {
		t.Fatalf("Delete failed: %v", err)
	}

	_, found, _ := c.Get("to_delete")
	if found {
		t.Fatal("Key should be deleted")
	}
	t.Log("DELETE succeeded")
}

func TestDeleteRange(t *testing.T) {
	c := newTestClient(t)

	c.Set("a", "1")
	c.Set("b", "2")
	c.Set("c", "3")
	c.Set("d", "4")

	ok, err := c.DeleteRange("a", "c")
	if err != nil || !ok {
		t.Fatalf("DeleteRange failed: %v", err)
	}

	_, foundB, _ := c.Get("b")
	if foundB {
		t.Fatal("b should be deleted")
	}

	valD, foundD, _ := c.Get("d")
	if !foundD || valD != "4" {
		t.Fatal("d should still exist")
	}
	t.Log("DELRANGE a-c succeeded, d still exists")
}

func TestStats(t *testing.T) {
	c := newTestClient(t)

	stats, err := c.Stats()
	if err != nil {
		t.Fatalf("Stats error: %v", err)
	}
	if len(stats) == 0 {
		t.Fatal("Stats should not be empty")
	}
	for k, v := range stats {
		t.Logf("  %s = %s", k, v)
	}
}

func TestQuit(t *testing.T) {
	c := newTestClient(t)
	err := c.Quit()
	if err != nil {
		t.Fatalf("Quit error: %v", err)
	}
	if c.IsConnected() {
		t.Fatal("Should be disconnected after quit")
	}
	t.Log("QUIT succeeded")
}
