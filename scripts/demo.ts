import { SendMessageCommand } from '@aws-sdk/client-sqs';
import { loadConfig } from '../src/infrastructure/config';
import { SqsQueue, QUEUES } from '../src/infrastructure/sqs';

async function main() {
  const base = process.env.DEMO_BASE_URL ?? 'http://localhost:3000';
  const queue = new SqsQueue(loadConfig());
  async function request(path: string, body?: unknown, key?: string) {
    const response = await fetch(base + path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: {
        'content-type': 'application/json',
        ...(key ? { 'idempotency-key': key } : {}),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    if (!response.ok) throw new Error(`HTTP ${response.status} at ${path}`);
    return response.json();
  }
  try {
    await request('/health/ready');
    const wallet = await request('/wallets', {
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const bet = {
      providerId: 'demo-provider',
      externalTransactionId: crypto.randomUUID(),
      walletId: wallet.id,
      playerId: wallet.playerId,
      roundId: crypto.randomUUID(),
      gameId: 'demo-game',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    };
    const key = `${bet.providerId}:${bet.externalTransactionId}`;
    const results = await Promise.all(
      Array.from({ length: 50 }, () =>
        request('/wagering/transactions', bet, key),
      ),
    );
    if (
      new Set(results.map((r) => r.transactionId)).size !== 1 ||
      results.filter((r) => !r.idempotentReplay).length !== 1
    ) {
      throw new Error('Concurrent replay invariant failed');
    }
    const refund = {
      ...bet,
      externalTransactionId: crypto.randomUUID(),
      kind: 'REFUND',
      referenceExternalTransactionId: bet.externalTransactionId,
    };
    const messageId = crypto.randomUUID();
    await queue.client.send(
      new SendMessageCommand({
        QueueUrl: await queue.url(QUEUES.requests),
        MessageGroupId: wallet.id,
        MessageDeduplicationId: messageId,
        MessageBody: JSON.stringify({
          messageId,
          type: 'WagerTransactionRequested',
          occurredAt: new Date().toISOString(),
          data: {
            ...refund,
            idempotencyKey: `${refund.providerId}:${refund.externalTransactionId}`,
          },
        }),
      }),
    );
    let completed = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      const response = await fetch(
        `${base}/providers/${refund.providerId}/wagering/transactions/${refund.externalTransactionId}`,
      );
      if (response.ok && (await response.json()).status === 'PROCESSED') {
        completed = true;
        break;
      }
      await Bun.sleep(200);
    }
    if (!completed)
      throw new Error('SQS refund did not finish; ensure WORKERS_ENABLED=true');
    const reconciliation = await request(
      `/wallets/${wallet.id}/reconciliation`,
      {},
    );
    if (
      !reconciliation.consistent ||
      reconciliation.storedBalance.amount !== '100.00' ||
      reconciliation.checkedEntries !== 3
    ) {
      throw new Error('Final ledger reconciliation failed');
    }
    console.log(
      JSON.stringify(
        {
          walletId: wallet.id,
          parallelRequests: results.length,
          appliedBets: 1,
          idempotentReplays: 49,
          sqsRefund: 'PROCESSED',
          reconciliation,
        },
        null,
        2,
      ),
    );
  } finally {
    queue.close();
  }
}
void main().catch((error) => {
  console.error(error instanceof Error ? error.message : 'DEMO_FAILED');
  process.exitCode = 1;
});
