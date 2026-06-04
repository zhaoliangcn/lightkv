const LightKVClient = require('../src/client');

const NUM_THREADS = 10;
const OPS_PER_THREAD = 1000;

async function worker(threadId, opsCount) {
    const client = new LightKVClient({ host: '127.0.0.1', port: 16379 });
    await client.connect();

    let success = 0;
    let failed = 0;
    const latencies = [];

    for (let i = 0; i < opsCount; i++) {
        const key = `stress_${threadId}_${i}`;
        const value = `value_${i}`;

        const start = Date.now();
        try {
            await client.set(key, value);
            await client.get(key);
            await client.delete(key);
            success += 3;
        } catch (e) {
            failed += 3;
        }
        const ms = Date.now() - start;
        latencies.push(ms);
    }

    await client.quit();
    return { success, failed, latencies };
}

async function main() {
    const totalOps = NUM_THREADS * OPS_PER_THREAD * 3;

    console.log('[Node.js Stress Test]');
    console.log(`  Threads: ${NUM_THREADS}`);
    console.log(`  Ops/thread: ${OPS_PER_THREAD} (SET+GET+DELETE)`);
    console.log(`  Total ops: ${totalOps}`);

    const start = Date.now();

    const promises = [];
    for (let i = 0; i < NUM_THREADS; i++) {
        promises.push(worker(i, OPS_PER_THREAD));
    }

    const results = await Promise.all(promises);
    const duration = Date.now() - start;

    let totalSuccess = 0;
    let totalFailed = 0;
    const allLatencies = [];

    for (const r of results) {
        totalSuccess += r.success;
        totalFailed += r.failed;
        allLatencies.push(...r.latencies);
    }

    allLatencies.sort((a, b) => a - b);
    const p50 = allLatencies[Math.floor(allLatencies.length * 0.5)];
    const p99 = allLatencies[Math.floor(allLatencies.length * 0.99)];
    const avg = allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length;
    const opsSec = Math.round(totalSuccess / (duration / 1000));

    console.log('\n  Results:');
    console.log(`    Duration:     ${duration} ms`);
    console.log(`    Success:      ${totalSuccess} ops`);
    console.log(`    Failed:       ${totalFailed} ops`);
    console.log(`    Throughput:   ${opsSec.toLocaleString()} ops/sec`);
    console.log(`    Avg Latency:  ${avg.toFixed(2)} ms`);
    console.log(`    P50 Latency:  ${p50} ms`);
    console.log(`    P99 Latency:  ${p99} ms`);

    console.log('\n[Node.js Stress Test Complete]');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
