package lightkv

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPipelineBenchmark(t *testing.T) {
	c := NewClient("127.0.0.1:16379", 5*time.Second)
	if err := c.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer c.Close()

	const N = 10000
	const BATCH = 100

	t.Log("\n[Go Pipeline Benchmark]")
	t.Logf("  %-25s %10s  |  %10s  |  %12s", "Operation", "Count", "Time(ms)", "Ops/sec")
	t.Log("  " + strings.Repeat("-", 80))

	run := func(label string, fn func()) {
		start := time.Now()
		fn()
		ms := time.Since(start).Seconds() * 1000
		ops := float64(N) / (ms / 1000)
		t.Logf("  %-25s %10d  |  %10.1f  |  %12.0f", label, N, ms, ops)
	}

	// Regular SET
	run("SET (regular)", func() {
		for i := 0; i < N; i++ {
			c.Set(fmt.Sprintf("go_pipe_%d", i), fmt.Sprintf("value_%d", i))
		}
	})

	// Pipeline SET
	run("SET (pipeline)", func() {
		for i := 0; i < N; i += BATCH {
			c.Pipeline()
			end := i + BATCH
			if end > N {
				end = N
			}
			for j := i; j < end; j++ {
				c.Queue([]string{"SET", fmt.Sprintf("go_pipe_%d", j), fmt.Sprintf("value_%d", j)})
			}
			c.ExecPipeline()
		}
	})

	// Regular GET
	run("GET (regular)", func() {
		for i := 0; i < N; i++ {
			c.Get(fmt.Sprintf("go_pipe_%d", i))
		}
	})

	// Pipeline GET
	run("GET (pipeline)", func() {
		for i := 0; i < N; i += BATCH {
			c.Pipeline()
			end := i + BATCH
			if end > N {
				end = N
			}
			for j := i; j < end; j++ {
				c.Queue([]string{"GET", fmt.Sprintf("go_pipe_%d", j)})
			}
			c.ExecPipeline()
		}
	})

	// Regular DELETE
	run("DELETE (regular)", func() {
		for i := 0; i < N; i++ {
			c.Delete(fmt.Sprintf("go_pipe_%d", i))
		}
	})

	// Pipeline DELETE
	run("DELETE (pipeline)", func() {
		for i := 0; i < N; i += BATCH {
			c.Pipeline()
			end := i + BATCH
			if end > N {
				end = N
			}
			for j := i; j < end; j++ {
				c.Queue([]string{"DEL", fmt.Sprintf("go_pipe_%d", j)})
			}
			c.ExecPipeline()
		}
	})

	t.Log("\n[Go Pipeline Benchmark Complete]")
}
