"""LightKV Python Client Pipeline Benchmark."""

import time
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lightkv'))

from client import LightKVClient


def fmt(n):
    return f'{n:,}'


def benchmark(label, count, fn):
    start = time.time()
    fn()
    ms = (time.time() - start) * 1000
    ops = count / (ms / 1000)
    print(f'  {label:<25} {fmt(count):>10} ops  |  {ms:>10.1f} ms  |  {fmt(int(ops)):>12} ops/sec')


def main():
    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        print(f'Failed to connect: {client.get_last_error()}')
        sys.exit(1)
    print('Connected to LightKV server')

    N = 10000
    BATCH = 100

    print('\n[Python Pipeline Benchmark]')
    print(f'  {"Operation":<25} {"Count":>10}  |  {"Time(ms)":>10}  |  {"Ops/sec":>12}')
    print('  ' + '-' * 80)

    # Regular SET
    benchmark('SET (regular)', N, lambda: [client.set(f'py_pipe_{i}', f'value_{i}') for i in range(N)])

    # Pipeline SET
    def pipe_set():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['SET', f'py_pipe_{j}', f'value_{j}'])
            client.exec_pipeline()
    benchmark('SET (pipeline)', N, pipe_set)

    # Regular GET
    benchmark('GET (regular)', N, lambda: [client.get(f'py_pipe_{i}') for i in range(N)])

    # Pipeline GET
    def pipe_get():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['GET', f'py_pipe_{j}'])
            client.exec_pipeline()
    benchmark('GET (pipeline)', N, pipe_get)

    # Regular DELETE
    benchmark('DELETE (regular)', N, lambda: [client.delete(f'py_pipe_{i}') for i in range(N)])

    # Pipeline DELETE
    def pipe_del():
        for i in range(0, N, BATCH):
            client.pipeline()
            end = min(i + BATCH, N)
            for j in range(i, end):
                client.queue(['DEL', f'py_pipe_{j}'])
            client.exec_pipeline()
    benchmark('DELETE (pipeline)', N, pipe_del)

    client.quit()
    print('\n[Python Pipeline Benchmark Complete]')


if __name__ == '__main__':
    main()
