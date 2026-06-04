"""LightKV vs Redis Concurrent Stress Test Comparison.

Requirements:
  - LightKV server running on port 16379
  - Redis server running on port 6379
  - redis-py: pip install redis
"""

import time
import sys
import os
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lightkv'))
from client import LightKVClient

try:
    import redis
except ImportError:
    print("redis-py not installed. Run: pip install redis")
    sys.exit(1)


def lightkv_worker(thread_id, ops_per_thread, results, lock):
    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        with lock:
            results['failed'] += ops_per_thread * 3
        return

    success = 0
    failed = 0
    latencies = []

    for i in range(ops_per_thread):
        key = f'stress_{thread_id}_{i}'
        value = f'value_{i}'

        start = time.time()
        ok = client.set(key, value)
        client.get(key)
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


def redis_worker(thread_id, ops_per_thread, results, lock):
    r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError:
        with lock:
            results['failed'] += ops_per_thread * 3
        return

    success = 0
    failed = 0
    latencies = []

    for i in range(ops_per_thread):
        key = f'stress_{thread_id}_{i}'
        value = f'value_{i}'

        start = time.time()
        ok = r.set(key, value)
        r.get(key)
        r.delete(key)
        elapsed = (time.time() - start) * 1000

        if ok:
            success += 3
        else:
            failed += 3
        latencies.append(elapsed)

    with lock:
        results['success'] += success
        results['failed'] += failed
        results['latencies'].extend(latencies)


def run_stress(name, worker_fn, num_threads, ops_per_thread):
    total_ops = num_threads * ops_per_thread * 3
    print(f'\n[{name} Stress Test]')
    print(f'  Threads: {num_threads}')
    print(f'  Ops/thread: {ops_per_thread} (SET+GET+DELETE)')
    print(f'  Total ops: {total_ops}')

    results = {'success': 0, 'failed': 0, 'latencies': []}
    lock = threading.Lock()
    threads = []

    start = time.time()
    for i in range(num_threads):
        t = threading.Thread(target=worker_fn, args=(i, ops_per_thread, results, lock))
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

    return {
        'duration_ms': duration_ms,
        'success': results['success'],
        'failed': results['failed'],
        'ops_sec': ops_sec,
        'avg': avg,
        'p50': p50,
        'p99': p99,
    }


def print_comparison(lk, rd, threads):
    print(f'\n{"="*80}')
    print(f'  LightKV vs Redis Stress Test Comparison ({threads} threads)')
    print(f'{"="*80}')
    print(f'  {"Metric":<20} {"LightKV":>12} {"Redis":>12} {"Ratio":>8}')
    print(f'  {"-"*60}')
    print(f'  {"Throughput (ops/s)":<20} {fmt(int(lk["ops_sec"])):>12} {fmt(int(rd["ops_sec"])):>12} {lk["ops_sec"]/rd["ops_sec"]:>7.2f}x')
    print(f'  {"Avg Latency (ms)":<20} {lk["avg"]:>12.2f} {rd["avg"]:>12.2f} {"":>8}')
    print(f'  {"P50 Latency (ms)":<20} {lk["p50"]:>12.2f} {rd["p50"]:>12.2f} {"":>8}')
    print(f'  {"P99 Latency (ms)":<20} {lk["p99"]:>12.2f} {rd["p99"]:>12.2f} {"":>8}')
    print(f'  {"Success":<20} {lk["success"]:>12} {rd["success"]:>12} {"":>8}')
    print(f'  {"Failed":<20} {lk["failed"]:>12} {rd["failed"]:>12} {"":>8}')
    print(f'{"="*80}\n')


def fmt(n):
    return f'{n:,}'


def main():
    threads = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    ops_per_thread = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

    print('\n[LightKV vs Redis Concurrent Stress Test]')

    lk = run_stress('LightKV', lightkv_worker, threads, ops_per_thread)
    rd = run_stress('Redis', redis_worker, threads, ops_per_thread)

    print_comparison(lk, rd, threads)


if __name__ == '__main__':
    main()
