const LightKVClient = require('../src/client');

async function worker(threadId, opsPerThread, results) {
    const client = new LightKVClient({ host: '127.0.0.1', port: 16379 });
    await client.connect();

    let success = 0;
    let failed = 0;
    const latencies = [];

    for (let i = 0; i < opsPerThread; i++) {
        const key = `node_stress_${threadId}_${i}`;
        const value = `value_${i}`;

        const start = Date.now();
        const ok = await client.set(key, value);
        await client.get(key);
        await client.delete(key);
        const elapsed = Date.now() - start;

        if (ok) success += 3;
        else failed += 3;
        latencies.push(elapsed);
    }

    await client.quit();
    return { success, failed, latencies };
}

async function main() {
    const numThreads = parseInt(process.argv[2]) || 10;
    const opsPerThread = parseInt(process.argv[3]) || 1000;
    const totalOps = numThreads * opsPerThread * 3;

    console.log('\n[Node.js Stress Test]');
    console.log(`  Threads: ${numThreads}`);
    console.log(`  Ops/thread: ${opsPerThread} (SET+GET+DELETE)`);
    console.log(`  Total ops: ${totalOps}`);

    const start = Date.now();
    const promises = [];
    for (let i = 0; i < numThreads; i++) {
        promises.push(worker(i, opsPerThread));
    }

    const workerResults = await Promise.all(promises);
    const durationMs = Date.now() - start;

    let totalSuccess = 0;
    let totalFailed = 0;
    const allLatencies = [];
    for (const r of workerResults) {
        totalSuccess += r.success;
        totalFailed += r.failed;
        allLatencies.push(...r.latencies);
    }

    allLatencies.sort((a, b) => a - b);
    const p50 = allLatencies[Math.floor(allLatencies.length * 0.5)] || 0;
    const p99 = allLatencies[Math.floor(allLatencies.length * 0.99)] || 0;
    const avg = allLatencies.reduce((a, b) => a + b, 0) / allLatencies.length || 0;
    const opsSec = totalSuccess / (durationMs / 1000);

    console.log('\n  Results:');
    console.log(`    Duration:     ${durationMs.toFixed(1)} ms`);
    console.log(`    Success:      ${totalSuccess} ops`);
    console.log(`    Failed:       ${totalFailed} ops`);
    console.log(`    Throughput:   ${opsSec.toFixed(0)} ops/sec`);
    console.log(`    Avg Latency:  ${avg.toFixed(2)} ms`);
    console.log(`    P50 Latency:  ${p50.toFixed(2)} ms`);
    console.log(`    P99 Latency:  ${p99.toFixed(2)} ms`);
    console.log('\n[Node.js Stress Test Complete]');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
