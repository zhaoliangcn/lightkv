const LightKVClient = require('../src/client');

function formatNumber(n) {
    return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

async function benchmark(label, count, fn) {
    const start = Date.now();
    await fn();
    const ms = Date.now() - start;
    const ops = Math.round(count / (ms / 1000));
    console.log(`  ${label.padEnd(25)} ${formatNumber(count).padStart(10)} ops  |  ${ms.toFixed(1).padStart(10)} ms  |  ${formatNumber(ops).padStart(12)} ops/sec`);
}

async function main() {
    const client = new LightKVClient({ host: '127.0.0.1', port: 16379 });
    await client.connect();
    console.log('Connected to LightKV server');

    const N = 10000;
    const BATCH = 100;

    console.log('\n[Node.js Pipeline Benchmark]');
    console.log(`  ${'Operation'.padEnd(25)} ${'Count'.padStart(10)}  |  ${'Time(ms)'.padStart(10)}  |  ${'Ops/sec'.padStart(12)}`);
    console.log('  ' + '-'.repeat(80));

    // Regular SET
    await benchmark('SET (regular)', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.set(`node_pipe_${i}`, `value_${i}`);
        }
    });

    // Pipeline SET
    await benchmark('SET (pipeline)', N, async () => {
        for (let i = 0; i < N; i += BATCH) {
            client.pipeline();
            const end = Math.min(i + BATCH, N);
            for (let j = i; j < end; j++) {
                client.queue(['SET', `node_pipe_${j}`, `value_${j}`]);
            }
            await client.execPipeline();
        }
    });

    // Regular GET
    await benchmark('GET (regular)', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.get(`node_pipe_${i}`);
        }
    });

    // Pipeline GET
    await benchmark('GET (pipeline)', N, async () => {
        for (let i = 0; i < N; i += BATCH) {
            client.pipeline();
            const end = Math.min(i + BATCH, N);
            for (let j = i; j < end; j++) {
                client.queue(['GET', `node_pipe_${j}`]);
            }
            await client.execPipeline();
        }
    });

    // Regular DELETE
    await benchmark('DELETE (regular)', N, async () => {
        for (let i = 0; i < N; i++) {
            await client.delete(`node_pipe_${i}`);
        }
    });

    // Pipeline DELETE
    await benchmark('DELETE (pipeline)', N, async () => {
        for (let i = 0; i < N; i += BATCH) {
            client.pipeline();
            const end = Math.min(i + BATCH, N);
            for (let j = i; j < end; j++) {
                client.queue(['DEL', `node_pipe_${j}`]);
            }
            await client.execPipeline();
        }
    });

    await client.quit();
    console.log('\n[Node.js Pipeline Benchmark Complete]');
}

main().catch(err => {
    console.error('Error:', err);
    process.exit(1);
});
