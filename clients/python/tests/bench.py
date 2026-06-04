"""LightKV Python Client Benchmark."""

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
    print(f'  {label:<25} {fmt(count):>10} ops  |  {ms:>10.1f} ms  |  {fmt(int(ops)):>10} ops/sec')


def main():
    client = LightKVClient(host='127.0.0.1', port=16379)
    if not client.connect():
        print(f'Failed to connect: {client.get_last_error()}')
        sys.exit(1)
    print('Connected to LightKV server')

    N = 10000

    print('\n[Python Client Benchmark]')
    print(f'  {"Operation":<25} {"Count":>10}  |  {"Time(ms)":>10}  |  {"Ops/sec":>10}')
    print('  ' + '-' * 75)

    benchmark('SET', N, lambda: [client.set(f'py_key_{i}', f'value_{i}') for i in range(N)])
    benchmark('GET', N, lambda: [client.get(f'py_key_{i}') for i in range(N)])
    benchmark('DELETE', N, lambda: [client.delete(f'py_key_{i}') for i in range(N)])
    benchmark('MIXED (SET+GET+DEL)', N, lambda: [
        (client.set(f'py_mixed_{i}', 'v'), client.get(f'py_mixed_{i}'), client.delete(f'py_mixed_{i}'))
        for i in range(N)
    ])

    client.quit()
    print('\n[Python Benchmark Complete]')


if __name__ == '__main__':
    main()
