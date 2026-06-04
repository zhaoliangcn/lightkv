"""LightKV Python Client Stress Test."""

import time
import sys
import os
import threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lightkv'))

from client import LightKVClient

NUM_THREADS = 10
OPS_PER_THREAD = 1000

results_lock = threading.Lock()
total_success = 0
total_failed = 0
all_latencies = []


def worker(thread_id, ops_count):
    global total_success, total_failed

    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        with results_lock:
            total_failed += ops_count * 3
        return

    success = 0
    failed = 0
    latencies = []

    for i in range(ops_count):
        key = f'stress_{thread_id}_{i}'
        value = f'value_{i}'

        start = time.time()
        try:
            client.set(key, value)
            client.get(key)
            client.delete(key)
            success += 3
        except Exception:
            failed += 3
        ms = (time.time() - start) * 1000
        latencies.append(ms)

    client.quit()

    with results_lock:
        total_success += success
        total_failed += failed
        all_latencies.extend(latencies)


def main():
    total_ops = NUM_THREADS * OPS_PER_THREAD * 3

    print('[Python Stress Test]')
    print(f'  Threads: {NUM_THREADS}')
    print(f'  Ops/thread: {OPS_PER_THREAD} (SET+GET+DELETE)')
    print(f'  Total ops: {total_ops}')

    start = time.time()

    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(i, OPS_PER_THREAD))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = (time.time() - start) * 1000

    all_latencies.sort()
    p50 = all_latencies[len(all_latencies) // 2]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]
    avg = sum(all_latencies) / len(all_latencies)
    ops_sec = total_success / (duration / 1000)

    print(f'\n  Results:')
    print(f'    Duration:     {duration:.1f} ms')
    print(f'    Success:      {total_success} ops')
    print(f'    Failed:       {total_failed} ops')
    print(f'    Throughput:   {int(ops_sec):,} ops/sec')
    print(f'    Avg Latency:  {avg:.2f} ms')
    print(f'    P50 Latency:  {p50:.2f} ms')
    print(f'    P99 Latency:  {p99:.2f} ms')

    print(f'\n[Python Stress Test Complete]')


if __name__ == '__main__':
    main()
