const LightKVClient = require('../src/client');

function formatNumber(n) {
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function benchmark(label, count, fn) {
    const start = Date.now();
    await fn();
    const ms = Date.now() - start;
    const ops = Math.round(count / (ms / 1000));
    console.log(`  ${label.padEnd(25)} ${formatNumber(count).padStart(10)} ops  |  ${ms.toFixed(1).padStart(10)} ms  |  ${formatNumber(ops).padStart(10)} ops/sec`);
}

async function main() {
    const client = new LightKVClient({ host: '127.0.0.1', port: 16379 });
    await client.connect();
    console.log('Connected to LightKV server');

    const N = 10000;

    console.log('\n[Node.js Client Benchmark]');
    console.log(`  ${'Operation'.padEnd(25)} ${'Count'.padStart(10)}  |  ${'Time(ms)'.padStart(10)}  |  ${'Ops/sec'.padStart(10)}`);
    console.log('  ' + '-'.repeat(75));

    await benchmark('SET', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.set(`node_key_${i}`, `value_${i}`);
        }
    });

    await benchmark('GET', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.get(`node_key_${i}`);
        }
    });

    await benchmark('DELETE', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.delete(`node_key_${i}`);
        }
    });

    await benchmark('MIXED (SET+GET+DEL)', N, async () => {
        for (let i = 0; i < N; i++) {
            const k = `node_mixed_${i}`;
            await client.set(k, 'v');
            await client.get(k);
            await client.delete(k);
        }
    });

    await client.quit();
    console.log('\n[Node.js Benchmark Complete]');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
