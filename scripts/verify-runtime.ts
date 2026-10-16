import assert from 'node:assert/strict';

// Local Compose only: deliberately interrupts dependencies and restores them in finally.
const bases = [3000, 3001, 3002].map((port) => `http://localhost:${port}`);
async function compose(...args: string[]) {
  const process = Bun.spawn(['docker', 'compose', ...args], {
    stdout: 'inherit',
    stderr: 'inherit',
  });
  assert.equal(await process.exited, 0, `docker compose ${args.join(' ')}`);
}
async function probe(base: string, path: string, expected: number) {
  const startedAt = Date.now();
  const response = await fetch(base + path, {
    signal: AbortSignal.timeout(20000),
  });
  const body = await response.text();
  console.log(
    JSON.stringify({
      base,
      path,
      status: response.status,
      durationMs: Date.now() - startedAt,
      body,
    }),
  );
  assert.equal(response.status, expected, `${base}${path}`);
}
async function healthy() {
  await Promise.all(
    bases.flatMap((base) => [
      probe(base, '/health/live', 200),
      probe(base, '/health/ready', 200),
    ]),
  );
}
async function main() {
  await healthy();
  for (const base of bases) {
    const response = await fetch(`${base}/metrics`);
    assert.equal(response.status, 200);
    const metrics = await response.text();
    for (const name of [
      'wager_transactions_total',
      'wager_duplicates_total',
      'wager_retries_total',
      'wager_dlq_total',
      'wager_lock_conflicts_total',
      'wager_outbox_lag_seconds',
      'wager_processing_seconds',
    ]) {
      assert.ok(metrics.includes(`# HELP ${name} `), `${base}: ${name}`);
    }
    console.log(
      JSON.stringify({ base, metrics: 'all seven families present' }),
    );
  }
  try {
    await compose('pause', 'localstack');
    console.log(JSON.stringify({ dependency: 'SQS', state: 'paused' }));
    await Promise.all(
      bases.flatMap((base) => [
        probe(base, '/health/live', 200),
        probe(base, '/health/ready', 503),
      ]),
    );
  } finally {
    await compose('unpause', 'localstack');
  }
  await healthy();
  try {
    await compose('stop', '-t', '5', 'postgres');
    console.log(JSON.stringify({ dependency: 'PostgreSQL', state: 'stopped' }));
    await Promise.all(
      bases.flatMap((base) => [
        probe(base, '/health/live', 200),
        probe(base, '/health/ready', 503),
      ]),
    );
  } finally {
    await compose('up', '-d', '--wait', 'postgres');
  }
  await healthy();
  console.log(JSON.stringify({ result: 'PASS', instances: bases.length }));
}
void main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
