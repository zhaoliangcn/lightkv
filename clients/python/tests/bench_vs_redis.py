"""LightKV vs Redis Performance Comparison Benchmark.

This script runs identical benchmarks against both LightKV and Redis servers
to provide a fair performance comparison.

Requirements:
  - LightKV server running on port 16379
  - Redis server running on port 6379
  - redis-py: pip install redis
"""

import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lightkv'))
from client import LightKVClient

try:
    import redis
except ImportError:
    print("redis-py not installed. Run: pip install redis")
    sys.exit(1)


def fmt(n):
    return f'{n:,}'


def benchmark(label, count, fn):
    start = time.time()
    fn()
    ms = (time.time() - start) * 1000
    ops = count / (ms / 1000)
    return ms, ops


def run_lightkv_bench(N, BATCH):
    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        print(f'Failed to connect to LightKV: {client.get_last_error()}')
        return None

    results = {}

    # Regular SET
    ms, ops = benchmark('SET (regular)', N, lambda: [client.set(f'bench_{i}', f'value_{i}') for i in range(N)])
    results['SET'] = (ms, ops)

    # Pipeline SET
    def pipe_set():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['SET', f'bench_{j}', f'value_{j}'])
            client.exec_pipeline()
    ms, ops = benchmark('SET (pipeline)', N, pipe_set)
    results['SET_pipeline'] = (ms, ops)

    # Regular GET
    ms, ops = benchmark('GET (regular)', N, lambda: [client.get(f'bench_{i}') for i in range(N)])
    results['GET'] = (ms, ops)

    # Pipeline GET
    def pipe_get():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['GET', f'bench_{j}'])
            client.exec_pipeline()
    ms, ops = benchmark('GET (pipeline)', N, pipe_get)
    results['GET_pipeline'] = (ms, ops)

    # Regular DELETE
    ms, ops = benchmark('DELETE (regular)', N, lambda: [client.delete(f'bench_{i}') for i in range(N)])
    results['DELETE'] = (ms, ops)

    # Pipeline DELETE
    def pipe_del():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['DEL', f'bench_{j}'])
            client.exec_pipeline()
    ms, ops = benchmark('DELETE (pipeline)', N, pipe_del)
    results['DELETE_pipeline'] = (ms, ops)

    client.quit()
    return results


def run_redis_bench(N, BATCH):
    r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
    try:
        r.ping()
    except redis.ConnectionError:
        print('Failed to connect to Redis on port 6379')
        return None

    results = {}

    # Regular SET
    ms, ops = benchmark('SET (regular)', N, lambda: [r.set(f'bench_{i}', f'value_{i}') for i in range(N)])
    results['SET'] = (ms, ops)

    # Pipeline SET
    def pipe_set():
        pipe = r.pipeline()
        for i in range(N):
            pipe.set(f'bench_{i}', f'value_{i}')
        pipe.execute()
    ms, ops = benchmark('SET (pipeline)', N, pipe_set)
    results['SET_pipeline'] = (ms, ops)

    # Regular GET
    ms, ops = benchmark('GET (regular)', N, lambda: [r.get(f'bench_{i}') for i in range(N)])
    results['GET'] = (ms, ops)

    # Pipeline GET
    def pipe_get():
        pipe = r.pipeline()
        for i in range(N):
            pipe.get(f'bench_{i}')
        pipe.execute()
    ms, ops = benchmark('GET (pipeline)', N, pipe_get)
    results['GET_pipeline'] = (ms, ops)

    # Regular DELETE
    ms, ops = benchmark('DELETE (regular)', N, lambda: [r.delete(f'bench_{i}') for i in range(N)])
    results['DELETE'] = (ms, ops)

    # Pipeline DELETE
    def pipe_del():
        pipe = r.pipeline()
        for i in range(N):
            pipe.delete(f'bench_{i}')
        pipe.execute()
    ms, ops = benchmark('DELETE (pipeline)', N, pipe_del)
    results['DELETE_pipeline'] = (ms, ops)

    return results


def print_comparison(lightkv_results, redis_results, N):
    print(f'\n{"="*80}')
    print(f'  LightKV vs Redis Performance Comparison (N={fmt(N)})')
    print(f'{"="*80}')
    print(f'  {"Operation":<20} {"LightKV":>12} {"Redis":>12} {"Ratio":>8}')
    print(f'  {"":20} {"ops/sec":>12} {"ops/sec":>12} {"LKV/Redis":>8}')
    print(f'  {"-"*60}')

    ops = ['SET', 'SET_pipeline', 'GET', 'GET_pipeline', 'DELETE', 'DELETE_pipeline']
    labels = ['SET', 'SET (pipeline)', 'GET', 'GET (pipeline)', 'DELETE', 'DELETE (pipeline)']

    for op, label in zip(ops, labels):
        lk = lightkv_results.get(op, (0, 0))
        rd = redis_results.get(op, (0, 0))
        ratio = lk[1] / rd[1] if rd[1] > 0 else 0
        print(f'  {label:<20} {fmt(int(lk[1])):>12} {fmt(int(rd[1])):>12} {ratio:>7.2f}x')

    print(f'{"="*80}\n')


def main():
    N = 10000
    BATCH = 100

    print('\n[LightKV vs Redis Benchmark]')
    print(f'  Operations: {fmt(N)}')
    print(f'  Pipeline batch size: {BATCH}')

    print('\n  Running LightKV benchmarks (port 16379)...')
    lightkv_results = run_lightkv_bench(N, BATCH)
    if lightkv_results is None:
        print('  LightKV benchmark failed!')
        return

    print('  Running Redis benchmarks (port 6379)...')
    redis_results = run_redis_bench(N, BATCH)
    if redis_results is None:
        print('  Redis benchmark failed!')
        return

    print_comparison(lightkv_results, redis_results, N)

    # Print individual results
    print('\n[LightKV Results]')
    print(f'  {"Operation":<20} {"Time(ms)":>10} {"Ops/sec":>12}')
    for op in ['SET', 'SET_pipeline', 'GET', 'GET_pipeline', 'DELETE', 'DELETE_pipeline']:
        ms, ops = lightkv_results[op]
        print(f'  {op:<20} {ms:>10.1f} {fmt(int(ops)):>12}')

    print('\n[Redis Results]')
    print(f'  {"Operation":<20} {"Time(ms)":>10} {"Ops/sec":>12}')
    for op in ['SET', 'SET_pipeline', 'GET', 'GET_pipeline', 'DELETE', 'DELETE_pipeline']:
        ms, ops = redis_results[op]
        print(f'  {op:<20} {ms:>10.1f} {fmt(int(ops)):>12}')


if __name__ == '__main__':
    main()
