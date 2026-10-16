import { expect, spyOn, test } from 'bun:test';
import { Telemetry } from '../../src/infrastructure/telemetry';

test('structured logs retain trace identifiers and discard unapproved sensitive fields', () => {
  const telemetry = new Telemetry();
  const output = spyOn(console, 'log').mockImplementation(() => {});
  try {
    const context = {
      correlationId: 'correlation',
      messageId: 'message',
      transactionId: 'transaction',
      walletId: 'wallet',
      providerId: 'provider',
      code: 'INSUFFICIENT_FUNDS',
      authorization: 'Bearer secret',
      money: { amount: '123.45', currency: 'BRL' },
      payload: { playerId: 'sensitive-player' },
    };
    telemetry.log('transaction_rejected', context);
    const line = output.mock.calls[0]![0] as string;
    expect(JSON.parse(line)).toEqual({
      timestamp: expect.any(String),
      event: 'transaction_rejected',
      correlationId: 'correlation',
      messageId: 'message',
      transactionId: 'transaction',
      walletId: 'wallet',
      providerId: 'provider',
      code: 'INSUFFICIENT_FUNDS',
    });
    expect(line).not.toContain('secret');
    expect(line).not.toContain('123.45');
    expect(line).not.toContain('sensitive-player');
  } finally {
    output.mockRestore();
  }
});

test('all mandatory metrics expose values and labels through the Prometheus registry', async () => {
  const telemetry = new Telemetry();
  telemetry.transactions.inc({ status: 'PROCESSED' });
  telemetry.duplicates.inc({ source: 'inbox' });
  telemetry.retries.inc({ worker: 'consumer' });
  telemetry.dlq.inc();
  telemetry.lockConflicts.inc();
  telemetry.outboxLag.set(3);
  telemetry.latency.observe(0.01);
  const metrics = await telemetry.registry.metrics();
  for (const sample of [
    'wager_transactions_total{status="PROCESSED"} 1',
    'wager_duplicates_total{source="inbox"} 1',
    'wager_retries_total{worker="consumer"} 1',
    'wager_dlq_total 1',
    'wager_lock_conflicts_total 1',
    'wager_outbox_lag_seconds 3',
    'wager_processing_seconds_count 1',
  ])
    expect(metrics).toContain(sample);
});
