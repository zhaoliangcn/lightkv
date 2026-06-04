"""Tests for LightKV Python Client SDK."""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from lightkv.client import LightKVClient


def run_tests():
    client = LightKVClient(host='127.0.0.1', port=16379)

    print('[Test] Connecting to server...')
    assert client.connect(), 'Should connect'
    assert client.is_connected(), 'Should be connected'
    print('[Test] Connected')

    # Test 1: Ping
    print('[Test] PING...')
    assert client.ping(), 'PING should return True'
    print('[Test] PONG received')

    # Test 2: Set/Get
    print('[Test] SET/GET...')
    assert client.set('hello', 'world'), 'SET should return True'
    val = client.get('hello')
    assert val == 'world', f'GET should return "world", got "{val}"'
    print(f'[Test] GET hello = {val}')

    # Test 3: Get non-existent key
    print('[Test] GET non-existent...')
    nil = client.get('nonexistent')
    assert nil is None, 'GET non-existent should return None'
    print('[Test] GET nonexistent = None')

    # Test 4: Delete
    print('[Test] DELETE...')
    assert client.delete('hello'), 'DELETE should return True'
    nil2 = client.get('hello')
    assert nil2 is None, 'GET after DELETE should return None'
    print('[Test] DELETE hello succeeded')

    # Test 5: Set multiple keys
    print('[Test] SET multiple...')
    client.set('a', '1')
    client.set('b', '2')
    client.set('c', '3')
    client.set('d', '4')
    print('[Test] Set 4 keys')

    # Test 6: DeleteRange
    print('[Test] DELRANGE...')
    assert client.delete_range('a', 'c'), 'DELRANGE should return True'
    b = client.get('b')
    assert b is None, 'b should be deleted'
    d = client.get('d')
    assert d == '4', 'd should still exist'
    print('[Test] DELRANGE a-c succeeded, d still exists')

    # Test 7: Stats
    print('[Test] STATS...')
    stats = client.stats()
    assert isinstance(stats, dict), 'STATS should return a dict'
    assert len(stats) > 0, 'STATS should not be empty'
    for k, v in stats.items():
        print(f'  {k} = {v}')

    # Test 8: Context manager
    print('[Test] Context manager...')
    with LightKVClient(host='127.0.0.1', port=16379) as ctx_client:
        assert ctx_client.is_connected()
        ctx_client.set('ctx_key', 'ctx_val')
        assert ctx_client.get('ctx_key') == 'ctx_val'
    assert not ctx_client.is_connected(), 'Should be disconnected after context exit'
    print('[Test] Context manager succeeded')

    # Test 9: Quit
    print('[Test] QUIT...')
    client.quit()
    assert not client.is_connected(), 'Should be disconnected after quit'
    print('[Test] Disconnected')

    print('\n[All tests passed!]')


if __name__ == '__main__':
    run_tests()
