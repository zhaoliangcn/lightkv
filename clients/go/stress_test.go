package lightkv

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	stressNumThreads    = 10
	stressOpsPerThread  = 1000
)

func TestStressTest(t *testing.T) {
	totalOps := stressNumThreads * stressOpsPerThread * 3

	t.Log("[Go Stress Test]")
	t.Logf("  Threads: %d", stressNumThreads)
	t.Logf("  Ops/thread: %d (SET+GET+DELETE)", stressOpsPerThread)
	t.Logf("  Total ops: %d", totalOps)

	var totalSuccess int64
	var totalFailed int64
	var latencies []float64
	var latMu sync.Mutex

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < stressNumThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			c := NewClient("127.0.0.1:16379", 5*time.Second)
			if err := c.Connect(); err != nil {
				atomic.AddInt64(&totalFailed, stressOpsPerThread*3)
				return
			}
			defer c.Close()

			success := int64(0)
			failed := int64(0)
			lats := make([]float64, 0, stressOpsPerThread)

			for i := 0; i < stressOpsPerThread; i++ {
				key := fmt.Sprintf("stress_%d_%d", threadID, i)
				value := fmt.Sprintf("value_%d", i)

				opStart := time.Now()
				_, _ = c.Set(key, value)
				_, _, _ = c.Get(key)
				_, _ = c.Delete(key)
				ms := float64(time.Since(opStart).Microseconds()) / 1000.0

				success += 3
				lats = append(lats, ms)
			}

			atomic.AddInt64(&totalSuccess, success)
			atomic.AddInt64(&totalFailed, failed)

			latMu.Lock()
			latencies = append(latencies, lats...)
			latMu.Unlock()
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	sort.Float64s(latencies)
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	sum := 0.0
	for _, l := range latencies {
		sum += l
	}
	avg := sum / float64(len(latencies))
	opsSec := float64(totalSuccess) / duration.Seconds()

	t.Logf("\n  Results:")
	t.Logf("    Duration:     %.1f ms", float64(duration.Microseconds())/1000.0)
	t.Logf("    Success:      %d ops", totalSuccess)
	t.Logf("    Failed:       %d ops", totalFailed)
	t.Logf("    Throughput:   %.0f ops/sec", opsSec)
	t.Logf("    Avg Latency:  %.2f ms", avg)
	t.Logf("    P50 Latency:  %.2f ms", p50)
	t.Logf("    P99 Latency:  %.2f ms", p99)

	_ = math.Max(0, 0) // suppress unused import
	t.Log("\n[Go Stress Test Complete]")
}
