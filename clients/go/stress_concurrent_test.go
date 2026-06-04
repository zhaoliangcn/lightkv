package lightkv

import (
	"math"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestStressConcurrent(t *testing.T) {
	testCases := []struct {
		name         string
		numThreads   int
		opsPerThread int
	}{
		{"50 concurrent", 50, 1000},
		{"100 concurrent", 100, 1000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			totalOps := tc.numThreads * tc.opsPerThread * 3

			t.Logf("\n[Go Stress Test]")
			t.Logf("  Threads: %d", tc.numThreads)
			t.Logf("  Ops/thread: %d (SET+GET+DELETE)", tc.opsPerThread)
			t.Logf("  Total ops: %d", totalOps)

			var wg sync.WaitGroup
			var mu sync.Mutex
			var totalSuccess int
			var totalFailed int
			var allLatencies []float64

			start := time.Now()

			for i := 0; i < tc.numThreads; i++ {
				wg.Add(1)
				go func(threadID int) {
					defer wg.Done()

					c := NewClient("127.0.0.1:16379", 5*time.Second)
					if err := c.Connect(); err != nil {
						mu.Lock()
						totalFailed += tc.opsPerThread * 3
						mu.Unlock()
						return
					}
					defer c.Close()

					success := 0
					failed := 0
					latencies := make([]float64, 0, tc.opsPerThread)

					for j := 0; j < tc.opsPerThread; j++ {
						key := "go_stress_" + strconv.Itoa(threadID) + "_" + strconv.Itoa(j)
						value := "value_" + strconv.Itoa(j)

						opStart := time.Now()
						ok, _ := c.Set(key, value)
						c.Get(key)
						c.Delete(key)
						elapsed := time.Since(opStart).Seconds() * 1000

						if ok {
							success += 3
						} else {
							failed += 3
						}
						latencies = append(latencies, elapsed)
					}

					mu.Lock()
					totalSuccess += success
					totalFailed += failed
					allLatencies = append(allLatencies, latencies...)
					mu.Unlock()
				}(i)
			}

			wg.Wait()
			durationMs := time.Since(start).Seconds() * 1000

			sort.Float64s(allLatencies)
			p50 := percentile(allLatencies, 50)
			p99 := percentile(allLatencies, 99)
			avg := average(allLatencies)
			opsSec := float64(totalSuccess) / (durationMs / 1000)

			t.Logf("\n  Results:")
			t.Logf("    Duration:     %.1f ms", durationMs)
			t.Logf("    Success:      %d ops", totalSuccess)
			t.Logf("    Failed:       %d ops", totalFailed)
			t.Logf("    Throughput:   %.0f ops/sec", opsSec)
			t.Logf("    Avg Latency:  %.2f ms", avg)
			t.Logf("    P50 Latency:  %.2f ms", p50)
			t.Logf("    P99 Latency:  %.2f ms", p99)
			t.Logf("\n[Go Stress Test Complete]")
		})
	}
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*float64(p)/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func average(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}
