import {
  beforeAll,
  afterAll,
  afterEach,
  describe,
  expect,
  test,
} from 'bun:test';
import { Client } from 'pg';
import type { DataSource } from 'typeorm';
import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
} from '@aws-sdk/client-sqs';
import { loadConfig, type Config } from '../../src/infrastructure/config';
import { createDatabase } from '../../src/infrastructure/database';
import { SqsQueue, QUEUES } from '../../src/infrastructure/sqs';
import { Telemetry } from '../../src/infrastructure/telemetry';
import {
  WageringService,
  type TransactionResult,
} from '../../src/application/wagering';
import { QueryService } from '../../src/application/queries';
import { WagerConsumer, OutboxPublisher } from '../../src/application/workers';
import type { QueuePort, QueueMessage } from '../../src/application/ports';
import type { Kind } from '../../src/domain/transaction';
import { createHttpApp } from '../../src/http/app';

let db: DataSource,
  admin: Client,
  config: Config,
  queue: SqsQueue,
  service: WageringService,
  queries: QueryService;
const telemetry = new Telemetry();
const namespace = `it_${crypto.randomUUID().replaceAll('-', '').slice(0, 12)}_`;
const databaseName = `${namespace}db`;
const children: ReturnType<typeof Bun.spawn>[] = [];
const ctx = () => ({ correlationId: crypto.randomUUID() });
async function eventually(
  predicate: () => boolean | Promise<boolean>,
  timeout = 15_000,
) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(25);
  }
  throw new Error('Condition did not become true within deadline');
}
function child(mode: string, overrides: Partial<Config> = {}) {
  const instance = Bun.spawn(
    mode === 'main'
      ? [process.execPath, 'src/main.ts']
      : [process.execPath, 'tests/fixtures/process.ts', mode],
    {
      env: {
        ...process.env,
        ...Object.fromEntries(
          Object.entries({ ...config, ...overrides }).map(([k, v]) => [
            k,
            String(v),
          ]),
        ),
      },
      stdout: 'pipe',
      stderr: 'pipe',
    },
  );
  children.push(instance);
  let output = '',
    errors = '';
  void (async () => {
    for await (const chunk of instance.stdout as ReadableStream<Uint8Array>)
      output += new TextDecoder().decode(chunk);
  })();
  void (async () => {
    for await (const chunk of instance.stderr as ReadableStream<Uint8Array>)
      errors += new TextDecoder().decode(chunk);
  })();
  return {
    instance,
    output: () => output,
    errors: () => errors,
    async ready() {
      await eventually(
        () =>
          output.includes(
            mode === 'main' ? '"event":"service_started"' : '"marker":"READY"',
          ) || instance.exitCode !== null,
      );
      if (instance.exitCode !== null) throw new Error(errors || output);
      if (mode === 'main') return { port: overrides.PORT };
      return JSON.parse(
        output.split('\n').find((line) => line.includes('"marker":"READY"'))!,
      );
    },
  };
}
async function wallet(amount = '100.00') {
  return service.openWallet(
    {
      playerId: crypto.randomUUID(),
      initialBalance: { amount, currency: 'BRL' },
    },
    ctx(),
  );
}
function command(
  w: { id: string; playerId: string },
  kind: Exclude<Kind, 'OPENING'> = 'BET',
  amount = '25.00',
  extra = {},
) {
  return {
    providerId: 'provider-a',
    externalTransactionId: crypto.randomUUID(),
    playerId: w.playerId,
    walletId: w.id,
    roundId: 'round-1',
    gameId: 'game-1',
    kind,
    money: { amount, currency: 'BRL' },
    ...extra,
  };
}
async function processCommand(
  body: ReturnType<typeof command>,
  key = `${body.providerId}:${body.externalTransactionId}`,
) {
  return service.process(body, key, ctx());
}
function envelope(
  body: ReturnType<typeof command>,
  messageId = crypto.randomUUID(),
) {
  return {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: {
      ...body,
      idempotencyKey: `${body.providerId}:${body.externalTransactionId}`,
    },
  };
}
async function send(body: unknown, group = crypto.randomUUID()) {
  await queue.client.send(
    new SendMessageCommand({
      QueueUrl: await queue.url(QUEUES.requests),
      MessageBody: typeof body === 'string' ? body : JSON.stringify(body),
      MessageGroupId: group,
      MessageDeduplicationId: crypto.randomUUID(),
    }),
  );
}
async function receive(): Promise<QueueMessage> {
  let found: QueueMessage | undefined;
  await eventually(async () => {
    found = (await queue.receive())[0];
    return !!found;
  });
  return found!;
}
function queueWith(overrides: Partial<QueuePort>): QueuePort {
  return {
    receive: () => queue.receive(),
    acknowledge: (h) => queue.acknowledge(h),
    changeVisibility: (h, s) => queue.changeVisibility(h, s),
    deadLetter: (b, k) => queue.deadLetter(b, k),
    publish: (b, a, e) => queue.publish(b, a, e),
    ready: () => queue.ready(),
    ...overrides,
  };
}
async function count(table: string, where = 'true', args: unknown[] = []) {
  return Number(
    (
      await db.query(`SELECT count(*) AS n FROM ${table} WHERE ${where}`, args)
    )[0].n,
  );
}
async function assertAllBalances() {
  const rows =
    await db.query(`SELECT w.id FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id GROUP BY w.id
    HAVING w.balance <> coalesce(sum(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)
      OR w.balance < 0 OR w.version <> 1 + count(l.id) FILTER (WHERE l.transaction_id IN (SELECT id FROM wager_transactions WHERE kind <> 'OPENING'))`);
  expect(rows).toEqual([]);
}
beforeAll(async () => {
  const adminUrl =
    process.env.TEST_DATABASE_ADMIN_URL ??
    process.env.DATABASE_URL ??
    'postgres://wager:wager@localhost:5432/wager';
  admin = new Client({ connectionString: adminUrl });
  await admin.connect();
  await admin.query(`CREATE DATABASE "${databaseName}"`);
  const url = new URL(adminUrl);
  url.pathname = `/${databaseName}`;
  config = loadConfig({
    ...process.env,
    DATABASE_URL: url.toString(),
    SQS_QUEUE_PREFIX: namespace,
    REFERENCE_MAX_ATTEMPTS: '3',
    REFERENCE_BACKOFF_MS: '1',
    WORKERS_ENABLED: 'false',
  });
  db = await createDatabase(config).initialize();
  await db.runMigrations();
  await db.undoLastMigration();
  await db.runMigrations();
  queue = new SqsQueue(config);
  const dlq = await queue.client.send(
    new CreateQueueCommand({
      QueueName: namespace + QUEUES.deadLetters,
      Attributes: { FifoQueue: 'true' },
    }),
  );
  const attrs = await queue.client.send(
    new GetQueueAttributesCommand({
      QueueUrl: dlq.QueueUrl,
      AttributeNames: ['QueueArn'],
    }),
  );
  await queue.client.send(
    new CreateQueueCommand({
      QueueName: namespace + QUEUES.requests,
      Attributes: {
        FifoQueue: 'true',
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: attrs.Attributes!.QueueArn,
          maxReceiveCount: '5',
        }),
      },
    }),
  );
  await queue.client.send(
    new CreateQueueCommand({
      QueueName: namespace + QUEUES.events,
      Attributes: { FifoQueue: 'true' },
    }),
  );
  await queue.ready();
  service = new WageringService(db, config, telemetry);
  queries = new QueryService(db, telemetry);
}, 120_000);
afterEach(async () => {
  if (db?.isInitialized) await assertAllBalances();
});
afterAll(async () => {
  await Promise.all(
    children.map(async (p) => {
      if (p.exitCode === null) {
        p.kill('SIGKILL');
        await p.exited;
      }
    }),
  );
  if (queue) {
    for (const name of Object.values(QUEUES))
      await queue.client.send(
        new DeleteQueueCommand({ QueueUrl: await queue.url(name) }),
      );
    queue.close();
  }
  if (db?.isInitialized) await db.destroy();
  if (admin) {
    await admin.query(`DROP DATABASE IF EXISTS "${databaseName}" WITH (FORCE)`);
    await admin.end();
  }
}, 30_000);

describe('Real PostgreSQL: schema, precision and transaction atomicity', () => {
  test('reversible migrations, opening audit, uniqueness, immutability and constraints', async () => {
    expect(await db.showMigrations()).toBe(false);
    const w = await wallet('9007199254740993.01');
    expect(w.balance.amount).toBe('9007199254740993.01');
    expect(w.version).toBe(1);
    expect(await count('wallet_ledger', 'wallet_id=$1', [w.id])).toBe(1);
    const opening = (
      await db.query('SELECT * FROM wager_transactions WHERE wallet_id=$1', [
        w.id,
      ])
    )[0];
    expect(opening.kind).toBe('OPENING');
    await expect(
      service.openWallet(
        { playerId: w.playerId, initialBalance: w.balance },
        ctx(),
      ),
    ).rejects.toMatchObject({ httpStatus: 409 });
    await expect(
      db.query('UPDATE wallets SET balance=-1 WHERE id=$1', [w.id]),
    ).rejects.toThrow();
    await expect(
      db.query('UPDATE wallets SET balance=balance+1 WHERE id=$1', [w.id]),
    ).rejects.toThrow('wallet/ledger divergence');
    await expect(
      db.query('UPDATE wallets SET version=version+1 WHERE id=$1', [w.id]),
    ).rejects.toThrow('wallet/ledger divergence');
    await expect(
      db.query('DELETE FROM wallet_ledger WHERE wallet_id=$1', [w.id]),
    ).rejects.toThrow('append-only');
    await expect(
      db.query('UPDATE wallet_ledger SET amount=amount WHERE wallet_id=$1', [
        w.id,
      ]),
    ).rejects.toThrow('append-only');
    await expect(db.query('TRUNCATE wallet_ledger')).rejects.toThrow(
      'append-only',
    );
    await expect(
      db.query("UPDATE wager_transactions SET status='PENDING' WHERE id=$1", [
        opening.id,
      ]),
    ).rejects.toThrow('immutable');
    await expect(
      db.query(
        `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after)
      SELECT gen_random_uuid(),wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after FROM wallet_ledger WHERE wallet_id=$1`,
        [w.id],
      ),
    ).rejects.toThrow();
  });
  test('failure after financial writes rolls back wallet, transaction, ledger, inbox and outbox', async () => {
    const w = await wallet();
    const body = command(w);
    const before = await count('outbox_messages');
    // Real failing trigger injects a storage failure at the final write, after ledger/outbox work.
    await db.query(`CREATE FUNCTION reject_test_inbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected inbox failure'; END $$;
      CREATE TRIGGER reject_test_inbox BEFORE INSERT ON inbox_messages FOR EACH ROW EXECUTE FUNCTION reject_test_inbox()`);
    try {
      await expect(
        service.process(body, 'atomicity', {
          ...ctx(),
          messageId: 'atomicity-message',
        }),
      ).rejects.toThrow('injected');
    } finally {
      await db.query(
        'DROP TRIGGER reject_test_inbox ON inbox_messages; DROP FUNCTION reject_test_inbox()',
      );
    }
    expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
    expect(
      await count('wager_transactions', 'external_transaction_id=$1', [
        body.externalTransactionId,
      ]),
    ).toBe(0);
    expect(await count('wallet_ledger', 'wallet_id=$1', [w.id])).toBe(1);
    expect(await count('inbox_messages')).toBe(0);
    expect(await count('outbox_messages')).toBe(before);
  });
  test('replay returns original balance, divergent key/provider identity conflicts, LOSS does not move balance', async () => {
    const w = await wallet();
    const bet = command(w);
    const first = await processCommand(bet, 'stable-key');
    await processCommand(command(w, 'WIN', '10.00'));
    expect(await processCommand(bet, 'stable-key')).toEqual({
      ...first,
      idempotentReplay: true,
    });
    await expect(
      processCommand(
        { ...bet, money: { amount: '1.00', currency: 'BRL' } },
        'stable-key',
      ),
    ).rejects.toThrow('IDEMPOTENCY_CONFLICT');
    await expect(processCommand(bet, 'other-key')).rejects.toThrow(
      'IDEMPOTENCY_CONFLICT',
    );
    const before = await queries.wallet(w.id);
    const loss = await processCommand(command(w, 'LOSS', '0.00'));
    expect(loss.status).toBe('PROCESSED');
    expect(await queries.wallet(w.id)).toEqual(before);
    expect(
      await count('wallet_ledger', 'transaction_id=$1', [loss.transactionId]),
    ).toBe(0);
    expect(
      await count(
        'outbox_messages',
        "event_type='WalletBalanceChanged' AND payload->'data'->>'transactionId'=$1",
        [loss.transactionId],
      ),
    ).toBe(0);
    expect(
      (
        await processCommand(
          command(w, 'BET', '1.00', {
            money: { amount: '1.00', currency: 'USD' },
          }),
        )
      ).failureCode,
    ).toBe('CURRENCY_MISMATCH');
  });
});

describe('HTTP contract and stable ledger pagination', () => {
  test('unresponsive SQS connection times out into readiness 503 while liveness stays 200', async () => {
    const stalled = Bun.serve({
      port: 0,
      fetch: () => new Promise<Response>(() => {}),
    });
    const stalledQueue = new SqsQueue({
      ...config,
      SQS_ENDPOINT: `http://127.0.0.1:${stalled.port}`,
    });
    const app = await createHttpApp(service, stalledQueue, telemetry);
    await app.listen(0, '127.0.0.1');
    const base = await app.getUrl();
    try {
      const response = await fetch(`${base}/health/ready`, {
        signal: AbortSignal.timeout(15000),
      });
      expect(response.status).toBe(503);
      expect((await response.json()).code).toBe('DEPENDENCIES_UNAVAILABLE');
      expect((await fetch(`${base}/health/live`)).status).toBe(200);
    } finally {
      stalledQueue.close();
      stalled.stop(true);
      await app.close();
    }
  }, 20000);
  test('validation, conflicts, business rejection, pending, lookup, readiness and correlation', async () => {
    let unavailable = false;
    const app = await createHttpApp(
      service,
      queueWith({
        ready: async () => {
          if (unavailable) throw new Error('DEPENDENCY_UNAVAILABLE');
          await queue.ready();
        },
      }),
      telemetry,
    );
    await app.listen(0, '127.0.0.1');
    const base = `http://127.0.0.1:${(app.getHttpServer().address() as { port: number }).port}`;
    const request = (path: string, body?: unknown, key?: string) =>
      fetch(base + path, {
        method: body === undefined ? 'GET' : 'POST',
        headers: {
          'content-type': 'application/json',
          'x-correlation-id': 'http-contract',
          ...(key ? { 'idempotency-key': key } : {}),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      });
    try {
      expect((await request('/health/live')).status).toBe(200);
      expect((await request('/health/ready')).status).toBe(200);
      const input = {
        playerId: crypto.randomUUID(),
        initialBalance: { amount: '100.00', currency: 'BRL' },
      };
      const created = await request('/wallets', input);
      expect(created.status).toBe(201);
      expect(created.headers.get('x-correlation-id')).toBe('http-contract');
      const w = await created.json();
      expect(w.version).toBe(1);
      expect((await request('/wallets', input)).status).toBe(409);
      const bet = command(w, 'BET', '80.00');
      expect((await request('/wagering/transactions', bet)).status).toBe(400);
      expect(
        (
          await request(
            '/wagering/transactions',
            { ...bet, money: { amount: 80, currency: 'BRL' } },
            'numeric',
          )
        ).status,
      ).toBe(400);
      expect(
        (
          await request(
            '/wagering/transactions',
            { ...bet, kind: 'OPENING' },
            'opening',
          )
        ).status,
      ).toBe(400);
      const first = await request('/wagering/transactions', bet, 'http-key');
      expect(first.status).toBe(200);
      const result = await first.json();
      expect(
        (
          await request(
            '/wagering/transactions',
            { ...bet, money: { amount: '79.00', currency: 'BRL' } },
            'http-key',
          )
        ).status,
      ).toBe(409);
      expect(
        (
          await request(
            '/wagering/transactions',
            command(w, 'BET', '80.00'),
            'no-funds',
          )
        ).status,
      ).toBe(422);
      expect(
        (await request(`/wagering/transactions/${result.transactionId}`))
          .status,
      ).toBe(200);
      const external = await request(
        `/providers/${bet.providerId}/wagering/transactions/${bet.externalTransactionId}`,
      );
      expect((await external.json()).id).toBe(result.transactionId);
      const pending = await request(
        '/wagering/transactions',
        command(w, 'REFUND', '80.00', {
          referenceExternalTransactionId: 'http-missing',
        }),
        'pending-http',
      );
      expect(pending.status).toBe(202);
      expect((await request('/wallets/not-a-uuid')).status).toBe(400);
      expect((await request(`/wallets/${crypto.randomUUID()}`)).status).toBe(
        404,
      );
      expect((await request('/metrics')).headers.get('content-type')).toContain(
        'text/plain',
      );
      unavailable = true;
      expect((await request('/health/ready')).status).toBe(503);
      expect((await request('/health/live')).status).toBe(200);
    } finally {
      await app.close();
    }
  });
  test('opaque cursor is wallet-bound and freezes the upper ledger boundary while new entries arrive', async () => {
    const w = await wallet();
    await processCommand(command(w));
    await processCommand(command(w, 'WIN', '1.00'));
    const first = await queries.ledger(w.id, { limit: 1 });
    expect(first.items).toHaveLength(1);
    expect(first.nextCursor).not.toBeNull();
    await processCommand(command(w, 'WIN', '2.00'));
    const second = await queries.ledger(w.id, {
      limit: 1,
      cursor: first.nextCursor,
    });
    const third = await queries.ledger(w.id, {
      limit: 1,
      cursor: second.nextCursor,
    });
    expect(third.items).toHaveLength(1);
    expect(third.nextCursor).toBeNull();
    expect(
      new Set(
        [...first.items, ...second.items, ...third.items].map(
          (e: { id: unknown }) => e.id,
        ),
      ).size,
    ).toBe(3);
    expect((await queries.ledger(w.id, {})).items).toHaveLength(4);
    const other = await wallet('0.00');
    await expect(
      queries.ledger(other.id, { cursor: first.nextCursor }),
    ).rejects.toThrow('INVALID_CURSOR');
    await expect(queries.ledger(w.id, { cursor: 'invalid' })).rejects.toThrow(
      'INVALID_CURSOR',
    );
    await expect(queries.ledger(w.id, { limit: 101 })).rejects.toThrow();
  });
});

describe('Real concurrency across wallets and processes', () => {
  test('50 simultaneous identical bets debit once, without FIFO', async () => {
    const w = await wallet(),
      body = command(w, 'BET', '25.00');
    const results = await Promise.all(
      Array.from({ length: 50 }, () => processCommand(body)),
    );
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
    expect(
      await count('wallet_ledger', "wallet_id=$1 AND direction='DEBIT'", [
        w.id,
      ]),
    ).toBe(1);
  });
  test('two simultaneous 80.00 bets from 100.00 give one debit, one business rejection and 20.00', async () => {
    const w = await wallet();
    const a = command(w, 'BET', '80.00'),
      b = command(w, 'BET', '80.00');
    const results = await Promise.all([processCommand(a), processCommand(b)]);
    expect(results.map((r) => r.status).sort()).toEqual([
      'PROCESSED',
      'REJECTED',
    ]);
    expect(results.find((r) => r.status === 'REJECTED')?.failureCode).toBe(
      'INSUFFICIENT_FUNDS',
    );
    expect((await queries.wallet(w.id)).balance.amount).toBe('20.00');
    expect(
      await count('wallet_ledger', "wallet_id=$1 AND direction='DEBIT'", [
        w.id,
      ]),
    ).toBe(1);
    await Promise.all([processCommand(a), processCommand(b)]);
    expect((await queries.wallet(w.id)).balance.amount).toBe('20.00');
  });
  test('a locked hot wallet does not block other wallets', async () => {
    const a = await wallet(),
      b = await wallet();
    const runner = db.createQueryRunner();
    await runner.startTransaction();
    try {
      await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [
        a.id,
      ]);
      expect((await processCommand(command(b))).status).toBe('PROCESSED');
      const serviceWithShortTimeout = new WageringService(
        db,
        { ...config, DB_LOCK_TIMEOUT_MS: 30 },
        telemetry,
      );
      await expect(
        serviceWithShortTimeout.process(command(a), 'locked', ctx()),
      ).rejects.toThrow('lock timeout');
    } finally {
      await runner.rollbackTransaction();
      await runner.release();
    }
    const wallets = await Promise.all(
      Array.from({ length: 12 }, () => wallet()),
    );
    await Promise.all(wallets.map((w) => processCommand(command(w))));
    expect(
      (await Promise.all(wallets.map((w) => queries.wallet(w.id)))).every(
        (w) => w.balance.amount === '75.00',
      ),
    ).toBe(true);
  });
  test('three independent Bun/Nest processes handle 50 concurrent HTTP requests and survive restart', async () => {
    const servers = [child('http'), child('http'), child('http')];
    const ready = await Promise.all(servers.map((s) => s.ready()));
    const w = await wallet(),
      body = command(w);
    const key = `multiprocess:${body.externalTransactionId}`;
    const results = await Promise.all(
      Array.from({ length: 50 }, async (_, i) => {
        const response = await fetch(
          `http://127.0.0.1:${ready[i % 3].port}/wagering/transactions`,
          {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'idempotency-key': key,
            },
            body: JSON.stringify(body),
          },
        );
        expect(response.status).toBe(200);
        return response.json() as Promise<TransactionResult>;
      }),
    );
    expect(results.filter((r) => !r.idempotentReplay)).toHaveLength(1);
    expect(new Set(results.map((r) => r.transactionId)).size).toBe(1);
    await Promise.all(
      servers.map(async (s) => {
        s.instance.kill('SIGTERM');
        expect(await s.instance.exited).toBe(0);
      }),
    );
    const restarted = child('http');
    const { port } = await restarted.ready();
    const reconcile = await fetch(
      `http://127.0.0.1:${port}/wallets/${w.id}/reconciliation`,
      { method: 'POST' },
    );
    expect((await reconcile.json()).consistent).toBe(true);
    const replay = await fetch(
      `http://127.0.0.1:${port}/wagering/transactions`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'idempotency-key': key },
        body: JSON.stringify(body),
      },
    );
    expect((await replay.json()).idempotentReplay).toBe(true);
    restarted.instance.kill('SIGTERM');
    await restarted.instance.exited;
  });
});

describe('References, reversals and eventual terminal states', () => {
  test('REFUND arrives before BET, is retried, then applied once', async () => {
    const w = await wallet(),
      bet = command(w),
      refund = command(w, 'REFUND', '25.00', {
        referenceExternalTransactionId: bet.externalTransactionId,
      });
    const result = await processCommand(refund);
    expect(result.status).toBe('PENDING_REFERENCE');
    expect(await count('wallet_ledger', 'wallet_id=$1', [w.id])).toBe(1);
    await processCommand(bet);
    await service.retryReference(result.transactionId);
    expect((await queries.transaction(result.transactionId)).status).toBe(
      'PROCESSED',
    );
    expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
    await service.retryReference(result.transactionId);
    expect((await queries.wallet(w.id)).version).toBe(3);
    const duplicate = await processCommand(
      command(w, 'REFUND', '25.00', {
        referenceExternalTransactionId: bet.externalTransactionId,
      }),
    );
    expect(duplicate.failureCode).toBe('REFERENCE_ALREADY_REVERSED');
  });
  test('expiry rejects missing reference with an outbox event', async () => {
    const w = await wallet();
    const result = await processCommand(
      command(w, 'ROLLBACK', '25.00', {
        referenceExternalTransactionId: 'missing',
      }),
    );
    for (let i = 0; i < config.REFERENCE_MAX_ATTEMPTS; i++) {
      await db.query(
        'UPDATE wager_transactions SET next_reference_at=now() WHERE id=$1',
        [result.transactionId],
      );
      await service.retryReference(result.transactionId);
    }
    expect((await queries.transaction(result.transactionId)).failureCode).toBe(
      'REFERENCE_NOT_FOUND',
    );
    expect(
      await count(
        'outbox_messages',
        "event_type='WagerTransactionRejected' AND payload->'data'->>'transactionId'=$1",
        [result.transactionId],
      ),
    ).toBe(1);
  });
  test('rollback credit and debit, full amounts, same round, rejected reference and negative reversal', async () => {
    const w = await wallet();
    const bet = command(w);
    await processCommand(bet);
    expect(
      (
        await processCommand(
          command(w, 'ROLLBACK', '25.00', {
            referenceExternalTransactionId: bet.externalTransactionId,
          }),
        )
      ).status,
    ).toBe('PROCESSED');
    expect((await queries.wallet(w.id)).balance.amount).toBe('100.00');
    const win = command(w, 'WIN', '50.00');
    await processCommand(win);
    await processCommand(command(w, 'BET', '140.00'));
    const negative = await processCommand(
      command(w, 'ROLLBACK', '50.00', {
        referenceExternalTransactionId: win.externalTransactionId,
      }),
    );
    expect(negative.failureCode).toBe('REVERSAL_INSUFFICIENT_FUNDS');
    const wrongAmount = await processCommand(
      command(w, 'REFUND', '24.00', {
        referenceExternalTransactionId: bet.externalTransactionId,
      }),
    );
    expect(wrongAmount.failureCode).toBe('REFERENCE_AMOUNT_MISMATCH');
    const wrongRound = await processCommand(
      command(w, 'WIN', '1.00', {
        referenceExternalTransactionId: bet.externalTransactionId,
        roundId: 'other',
      }),
    );
    expect(wrongRound.failureCode).toBe('REFERENCE_MISMATCH');
    const badBet = command(w, 'BET', '1000.00');
    await processCommand(badBet);
    expect(
      (
        await processCommand(
          command(w, 'REFUND', '1000.00', {
            referenceExternalTransactionId: badBet.externalTransactionId,
          }),
        )
      ).failureCode,
    ).toBe('REFERENCE_NOT_PROCESSED');
    const another = await wallet('0.00'),
      credit = command(another, 'WIN', '50.00');
    await processCommand(credit);
    expect(
      (
        await processCommand(
          command(another, 'ROLLBACK', '50.00', {
            referenceExternalTransactionId: credit.externalTransactionId,
          }),
        )
      ).balance.amount,
    ).toBe('0.00');
  });
});

describe('Real SQS: inbox, retries, DLQ and process death', () => {
  test('redelivering the same envelope records one inbox and one debit', async () => {
    const w = await wallet();
    const event = envelope(command(w));
    const consumer = new WagerConsumer(service, queue, config, telemetry);
    await send(event);
    await consumer.handle(await receive());
    await send(event);
    await consumer.handle(await receive());
    expect(
      await count('inbox_messages', 'message_id=$1', [event.messageId]),
    ).toBe(1);
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
  });
  test('worker is SIGKILLed after SQL commit and before ack; redelivery after visibility expiry is safe', async () => {
    const w = await wallet();
    const event = envelope(command(w));
    await send(event);
    const worker = child('crash-consumer');
    await worker.ready();
    await eventually(() => worker.output().includes('"marker":"COMMITTED"'));
    worker.instance.kill('SIGKILL');
    await worker.instance.exited;
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
    const redelivered = await receive();
    expect(redelivered.receiveCount).toBeGreaterThanOrEqual(2);
    await new WagerConsumer(service, queue, config, telemetry).handle(
      redelivered,
    );
    expect(
      await count('inbox_messages', 'message_id=$1', [event.messageId]),
    ).toBe(1);
    expect(
      await count('wallet_ledger', "wallet_id=$1 AND direction='DEBIT'", [
        w.id,
      ]),
    ).toBe(1);
  });
  test('transient SQL failure uses visibility backoff then succeeds; permanent input goes to DLQ', async () => {
    const w = await wallet();
    const event = envelope(command(w));
    await send(event);
    const message = await receive();
    const runner = db.createQueryRunner();
    await runner.startTransaction();
    try {
      await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [
        w.id,
      ]);
      const short = new WageringService(
        db,
        { ...config, DB_LOCK_TIMEOUT_MS: 30 },
        telemetry,
      );
      await new WagerConsumer(short, queue, config, telemetry).handle(message);
      expect(
        await count('inbox_messages', 'message_id=$1', [event.messageId]),
      ).toBe(0);
    } finally {
      await runner.rollbackTransaction();
      await runner.release();
    }
    const retry = await receive();
    expect(retry.receiveCount).toBeGreaterThanOrEqual(2);
    await new WagerConsumer(service, queue, config, telemetry).handle(retry);
    await send('not-json');
    await new WagerConsumer(service, queue, config, telemetry).handle(
      await receive(),
    );
    const dlq = await queue.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await queue.url(QUEUES.deadLetters),
        WaitTimeSeconds: 1,
      }),
    );
    expect(dlq.Messages?.[0]?.Body).toBe('not-json');
    await queue.client.send(
      new DeleteMessageCommand({
        QueueUrl: await queue.url(QUEUES.deadLetters),
        ReceiptHandle: dlq.Messages![0]!.ReceiptHandle!,
      }),
    );
  });
  test('bounded retries transfer original message to DLQ and record FAILED when DB is available', async () => {
    const w = await wallet();
    const event = envelope(command(w));
    await send(event);
    let message = await receive();
    const runner = db.createQueryRunner();
    await runner.startTransaction();
    const short = new WageringService(
      db,
      { ...config, DB_LOCK_TIMEOUT_MS: 20 },
      telemetry,
    );
    const consumer = new WagerConsumer(
      short,
      queue,
      { ...config, SQS_MAX_ATTEMPTS: 2 },
      telemetry,
    );
    try {
      await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [
        w.id,
      ]);
      await consumer.handle(message);
      await queue.changeVisibility(message.receiptHandle, 0);
      message = await receive();
      // A real storage trigger fails the normal processing path; the technical-failure path avoids that ledger insert.
      await db.query(`CREATE FUNCTION reject_test_ledger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected storage failure'; END $$;
        CREATE TRIGGER reject_test_ledger BEFORE INSERT ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION reject_test_ledger()`);
      await runner.rollbackTransaction();
      await consumer.handle(message);
    } finally {
      if (runner.isTransactionActive) await runner.rollbackTransaction();
      await runner.release();
      await db.query(
        'DROP TRIGGER IF EXISTS reject_test_ledger ON wallet_ledger; DROP FUNCTION IF EXISTS reject_test_ledger()',
      );
    }
    const row = (
      await db.query(
        'SELECT status FROM wager_transactions WHERE external_transaction_id=$1',
        [event.data.externalTransactionId],
      )
    )[0];
    expect(row.status).toBe('FAILED');
    const dlq = await queue.client.send(
      new ReceiveMessageCommand({
        QueueUrl: await queue.url(QUEUES.deadLetters),
        WaitTimeSeconds: 1,
      }),
    );
    expect(JSON.parse(dlq.Messages![0]!.Body!).messageId).toBe(event.messageId);
    await queue.client.send(
      new DeleteMessageCommand({
        QueueUrl: await queue.url(QUEUES.deadLetters),
        ReceiptHandle: dlq.Messages![0]!.ReceiptHandle!,
      }),
    );
  });
  test('returning visibility during drain safely redelivers an in-flight message', async () => {
    const w = await wallet();
    await send(envelope(command(w)));
    const message = await receive();
    let committed = false,
      finish!: () => void;
    const paused = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const consumer = new WagerConsumer(
      service,
      queue,
      config,
      telemetry,
      async () => {
        committed = true;
        await paused;
      },
    );
    const running = consumer.handle(message);
    await eventually(() => committed);
    await consumer.releaseInFlight();
    const redelivery = await receive();
    finish();
    await running;
    await new WagerConsumer(service, queue, config, telemetry).handle(
      redelivery,
    );
    expect((await queries.wallet(w.id)).balance.amount).toBe('75.00');
  });
});

describe('Outbox: post-commit visibility, concurrent publishers and recovery', () => {
  test('two publisher processes publish committed backlog with stable event identities', async () => {
    const expected = await count('outbox_messages', 'published_at IS NULL');
    expect(expected).toBeGreaterThan(0);
    const publishers = [child('publisher'), child('publisher')];
    await Promise.all(publishers.map((p) => p.ready()));
    expect(await publishers[0]!.instance.exited).toBe(0);
    expect(await publishers[1]!.instance.exited).toBe(0);
    expect(await count('outbox_messages', 'published_at IS NULL')).toBe(0);
    const received = new Map<string, string>();
    await eventually(async () => {
      const result = await queue.client.send(
        new ReceiveMessageCommand({
          QueueUrl: await queue.url(QUEUES.events),
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 1,
        }),
      );
      for (const m of result.Messages ?? []) {
        const e = JSON.parse(m.Body!);
        received.set(e.eventId, m.Body!);
        await queue.client.send(
          new DeleteMessageCommand({
            QueueUrl: await queue.url(QUEUES.events),
            ReceiptHandle: m.ReceiptHandle!,
          }),
        );
      }
      return received.size === expected;
    }, 30_000);
    expect(received.size).toBe(expected);
  });
  test('publisher failure stores backoff, a fresh publisher recovers; uncommitted rows are invisible', async () => {
    const w = await wallet('0.00');
    await processCommand(command(w, 'WIN', '1.00'));
    const runner = db.createQueryRunner();
    await runner.startTransaction();
    const hiddenId = crypto.randomUUID();
    try {
      await runner.query(
        `INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES ($1,$2,'test',$3,now())`,
        [hiddenId, w.id, { eventId: hiddenId }],
      );
      const publisher = new OutboxPublisher(
        db,
        queueWith({
          publish: async () => {
            throw new Error('SQS unavailable');
          },
        }),
        telemetry,
      );
      await publisher.tick();
      expect(await count('outbox_messages', 'attempts=1')).toBe(1);
      expect(await count('outbox_messages', 'id=$1', [hiddenId])).toBe(0);
    } finally {
      await runner.rollbackTransaction();
      await runner.release();
    }
    await db.query(
      'UPDATE outbox_messages SET next_attempt_at=now() WHERE published_at IS NULL',
    );
    const recovered = child('publisher');
    await recovered.ready();
    expect(await recovered.instance.exited).toBe(0);
    expect(await count('outbox_messages', 'published_at IS NULL')).toBe(0);
  });
});

describe('Operational evidence: corruption detection and real process shutdown', () => {
  test('reconciliation reports privileged corruption, logs and counts it without repairing the balance', async () => {
    const w = await wallet();
    const { spyOn } = await import('bun:test');
    const logged = spyOn(telemetry, 'log');
    const before = (await telemetry.divergences.get()).values[0]!.value;
    const corruptProjection = (amount: string) =>
      db.transaction(async (manager) => {
        // Fault injection only: a privileged test session bypasses the production constraints.
        // No application path or schema change permits this operation.
        await manager.query("SET LOCAL session_replication_role='replica'");
        await manager.query('UPDATE wallets SET balance=$2 WHERE id=$1', [
          w.id,
          amount,
        ]);
      });
    try {
      await corruptProjection('99.00');
      const result = await queries.reconcile(w.id, 'corrupt-projection');
      expect(result).toMatchObject({
        storedBalance: { amount: '99.00', currency: 'BRL' },
        calculatedBalance: { amount: '100.00', currency: 'BRL' },
        difference: { amount: '-1.00', currency: 'BRL' },
        consistent: false,
        checkedEntries: 1,
      });
      expect((await telemetry.divergences.get()).values[0]!.value).toBe(
        before + 1,
      );
      expect(logged).toHaveBeenCalledWith('reconciliation_divergence', {
        correlationId: 'corrupt-projection',
        walletId: w.id,
        code: 'LEDGER_MISMATCH',
      });
      expect((await queries.wallet(w.id)).balance.amount).toBe('99.00');
      console.log(
        JSON.stringify({ evidence: 'reconciliation-divergence', ...result }),
      );
    } finally {
      logged.mockRestore();
      await corruptProjection('100.00');
    }
  });

  for (const returnVisibility of [false, true]) {
    test(`SIGTERM on src/main.ts ${returnVisibility ? 'returns visibility after deadline' : 'finishes an in-flight message'} without duplicate debit`, async () => {
      const w = await wallet();
      const event = envelope(command(w));
      const runner = db.createQueryRunner();
      await runner.startTransaction();
      const [{ pid }] = await runner.query('SELECT pg_backend_pid() AS pid');
      await runner.query('SELECT id FROM wallets WHERE id=$1 FOR UPDATE', [
        w.id,
      ]);
      const reservation = Bun.serve({ port: 0, fetch: () => new Response() });
      const port = reservation.port!;
      await reservation.stop(true);
      let worker: ReturnType<typeof child> | undefined;
      try {
        await send(event);
        worker = child('main', {
          PORT: port,
          WORKERS_ENABLED: 'true',
          DB_LOCK_TIMEOUT_MS: 10000,
          SHUTDOWN_TIMEOUT_MS: returnVisibility ? 100 : 20000,
        });
        await worker.ready();
        // Observe an actual PostgreSQL lock wait, proving the consumer is in flight.
        await eventually(
          async () =>
            (
              await db.query(
                'SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid))',
                [pid],
              )
            ).length > 0,
        );
        worker.instance.kill('SIGTERM');
        await eventually(() =>
          worker!
            .output()
            .includes(
              returnVisibility
                ? '"event":"shutdown_visibility_returned"'
                : '"event":"shutdown_started"',
            ),
        );
        await runner.rollbackTransaction();
        await eventually(() => worker!.instance.exitCode !== null, 25000);
        expect(await worker.instance.exited).toBe(0);
        expect(worker.output()).toContain('"event":"shutdown_complete"');
        if (returnVisibility) {
          const redelivery = await receive();
          expect(redelivery.receiveCount).toBeGreaterThanOrEqual(2);
          await new WagerConsumer(service, queue, config, telemetry).handle(
            redelivery,
          );
        } else {
          expect(await queue.receive()).toEqual([]);
        }
        expect(
          await count('inbox_messages', 'message_id=$1', [event.messageId]),
        ).toBe(1);
        expect(
          await count('wallet_ledger', "wallet_id=$1 AND direction='DEBIT'", [
            w.id,
          ]),
        ).toBe(1);
        const reconciliation = await queries.reconcile(
          w.id,
          'sigterm-evidence',
        );
        expect(reconciliation.consistent).toBe(true);
        expect(reconciliation.storedBalance.amount).toBe('75.00');
        console.log(
          JSON.stringify({
            evidence: 'production-sigterm',
            visibilityReturned: returnVisibility,
            exitCode: worker.instance.exitCode,
            reconciliation,
            shutdownEvents: worker
              .output()
              .split('\n')
              .filter((line) => line.includes('shutdown_'))
              .map((line) => JSON.parse(line)),
          }),
        );
      } finally {
        if (runner.isTransactionActive) await runner.rollbackTransaction();
        await runner.release();
        if (worker && worker.instance.exitCode === null) {
          worker.instance.kill('SIGKILL');
          await worker.instance.exited;
        }
      }
    });
  }
});
