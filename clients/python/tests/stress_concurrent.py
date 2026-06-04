"""LightKV Python Client Stress Test (Concurrent)."""

import time
import sys
import os
import threading
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lightkv'))

from client import LightKVClient


def worker(thread_id, ops_per_thread, results, lock):
    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        with lock:
            results['failed'] += ops_per_thread * 3
        return

    success = 0
    failed = 0
    latencies = []

    for i in range(ops_per_thread):
        key = f'py_stress_{thread_id}_{i}'
        value = f'value_{i}'

        start = time.time()
        ok = client.set(key, value)
        val = client.get(key)
        client.delete(key)
        elapsed = (time.time() - start) * 1000

        if ok:
            success += 3
        else:
            failed += 3
        latencies.append(elapsed)

    client.quit()

    with lock:
        results['success'] += success
        results['failed'] += failed
        results['latencies'].extend(latencies)


def main():
    num_threads = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    ops_per_thread = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    total_ops = num_threads * ops_per_thread * 3

    print(f'\n[Python Stress Test]')
    print(f'  Threads: {num_threads}')
    print(f'  Ops/thread: {ops_per_thread} (SET+GET+DELETE)')
    print(f'  Total ops: {total_ops}')

    results = {'success': 0, 'failed': 0, 'latencies': []}
    lock = threading.Lock()
    threads = []

    start = time.time()
    for i in range(num_threads):
        t = threading.Thread(target=worker, args=(i, ops_per_thread, results, lock))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    duration_ms = (time.time() - start) * 1000
    latencies = sorted(results['latencies'])
    p50 = latencies[len(latencies) * 50 // 100] if latencies else 0
    p99 = latencies[len(latencies) * 99 // 100] if latencies else 0
    avg = sum(latencies) / len(latencies) if latencies else 0
    ops_sec = results['success'] / (duration_ms / 1000.0)

    print(f'\n  Results:')
    print(f'    Duration:     {duration_ms:.1f} ms')
    print(f'    Success:      {results["success"]} ops')
    print(f'    Failed:       {results["failed"]} ops')
    print(f'    Throughput:   {ops_sec:.0f} ops/sec')
    print(f'    Avg Latency:  {avg:.2f} ms')
    print(f'    P50 Latency:  {p50:.2f} ms')
    print(f'    P99 Latency:  {p99:.2f} ms')
    print(f'\n[Python Stress Test Complete]')


if __name__ == '__main__':
    main()
