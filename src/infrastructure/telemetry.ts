import { Counter, Gauge, Histogram, Registry } from 'prom-client';

export class Telemetry {
  readonly registry = new Registry();
  readonly transactions = new Counter({
    name: 'wager_transactions_total',
    help: 'Committed status transitions',
    labelNames: ['status'],
    registers: [this.registry],
  });
  readonly duplicates = new Counter({
    name: 'wager_duplicates_total',
    help: 'Persistent deduplication hits',
    labelNames: ['source'],
    registers: [this.registry],
  });
  readonly retries = new Counter({
    name: 'wager_retries_total',
    help: 'Retry attempts',
    labelNames: ['worker'],
    registers: [this.registry],
  });
  readonly dlq = new Counter({
    name: 'wager_dlq_total',
    help: 'Messages transferred to DLQ',
    registers: [this.registry],
  });
  readonly lockConflicts = new Counter({
    name: 'wager_lock_conflicts_total',
    help: 'Database lock conflicts',
    registers: [this.registry],
  });
  readonly outboxLag = new Gauge({
    name: 'wager_outbox_lag_seconds',
    help: 'Age of oldest unpublished event',
    registers: [this.registry],
  });
  readonly latency = new Histogram({
    name: 'wager_processing_seconds',
    help: 'Use-case latency',
    buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1, 5],
    registers: [this.registry],
  });
  readonly divergences = new Counter({
    name: 'wager_reconciliation_divergences_total',
    help: 'Reconciliation mismatches',
    registers: [this.registry],
  });
  log(
    event: string,
    context: {
      correlationId?: string;
      messageId?: string;
      transactionId?: string;
      walletId?: string;
      providerId?: string;
      code?: string;
    } = {},
  ): void {
    const {
      correlationId,
      messageId,
      transactionId,
      walletId,
      providerId,
      code,
    } = context;
    console.log(
      JSON.stringify({
        timestamp: new Date().toISOString(),
        event,
        correlationId,
        messageId,
        transactionId,
        walletId,
        providerId,
        code,
      }),
    );
  }
}
