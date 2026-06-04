package lightkv

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func BenchmarkSet(b *testing.B) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		b.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("go_key_%d", i), fmt.Sprintf("value_%d", i))
	}
}

func BenchmarkGet(b *testing.B) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		b.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("go_key_%d", i), fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(fmt.Sprintf("go_key_%d", i))
	}
}

func BenchmarkDelete(b *testing.B) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		b.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	// Pre-populate
	for i := 0; i < b.N; i++ {
		c.Set(fmt.Sprintf("go_key_%d", i), fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Delete(fmt.Sprintf("go_key_%d", i))
	}
}

func BenchmarkMixed(b *testing.B) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		b.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("go_mixed_%d", i)
		c.Set(k, "v")
		c.Get(k)
		c.Delete(k)
	}
}

// Run a summary benchmark with fixed count
func TestBenchmarkSummary(t *testing.T) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	const N = 10000

	t.Log("\n[Go Client Benchmark]")
	t.Logf("  %-25s %10s  |  %10s  |  %10s", "Operation", "Count", "Time(ms)", "Ops/sec")
	t.Log("  " + strings.Repeat("-", 75))

	run := func(label string, fn func()) {
		start := time.Now()
		fn()
		ms := time.Since(start).Seconds() * 1000
		ops := float64(N) / (ms / 1000)
		t.Logf("  %-25s %10d  |  %10.1f  |  %10.0f", label, N, ms, ops)
	}

	run("SET", func() {
		for i := 0; i < N; i++ {
			c.Set(fmt.Sprintf("go_key_%d", i), fmt.Sprintf("value_%d", i))
		}
	})

	run("GET", func() {
		for i := 0; i < N; i++ {
			c.Get(fmt.Sprintf("go_key_%d", i))
		}
	})

	run("DELETE", func() {
		for i := 0; i < N; i++ {
			c.Delete(fmt.Sprintf("go_key_%d", i))
		}
	})

	run("MIXED (SET+GET+DEL)", func() {
		for i := 0; i < N; i++ {
			k := fmt.Sprintf("go_mixed_%d", i)
			c.Set(k, "v")
			c.Get(k)
			c.Delete(k)
		}
	})

	t.Log("\n[Go Benchmark Complete]")
}
