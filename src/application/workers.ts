import { createHash } from 'node:crypto';
import { ZodError } from 'zod';
import type { DataSource } from 'typeorm';
import { envelopeSchema } from './contracts';
import type { QueuePort, QueueMessage } from './ports';
import { ApplicationError, DomainError } from '../domain/errors';
import { OutboxMessage } from '../domain/messages';
import { payloadHash } from '../domain/idempotency';
import type { WageringService, ProcessingContext } from './wagering';
import type { Config } from '../infrastructure/config';
import type { Telemetry } from '../infrastructure/telemetry';
import { databaseCode } from '../infrastructure/database';

export class WagerConsumer {
  private readonly inFlight = new Set<string>();
  private readonly releasing = new Set<string>();
  constructor(
    private readonly service: WageringService,
    private readonly queue: QueuePort,
    private readonly config: Config,
    private readonly telemetry: Telemetry,
    private readonly afterCommit: () => Promise<void> = async () => {},
  ) {}
  async tick(): Promise<void> {
    for (const message of await this.queue.receive())
      await this.handle(message);
  }
  async handle(message: QueueMessage): Promise<void> {
    const handle = message.receiptHandle;
    this.inFlight.add(handle);
    let ctx: ProcessingContext = { correlationId: crypto.randomUUID() };
    // Keep visibility while waiting for locks or finishing a slow database transaction.
    const renewal = setInterval(() => {
      if (!this.releasing.has(handle))
        void this.queue
          .changeVisibility(handle, 30)
          .catch(() => this.telemetry.log('visibility_renewal_failed', ctx));
    }, 10_000);
    let parsed: ReturnType<typeof envelopeSchema.parse> | undefined;
    try {
      parsed = envelopeSchema.parse(JSON.parse(message.body));
      const { idempotencyKey, ...business } = parsed.data;
      ctx = {
        correlationId: parsed.correlationId ?? parsed.messageId,
        causationId: parsed.messageId,
        messageId: parsed.messageId,
        consumerName: 'wager-consumer',
        inboxHash: payloadHash(parsed.data),
      };
      await this.service.process(business, idempotencyKey, ctx);
      await this.afterCommit();
      if (!this.releasing.has(handle)) await this.queue.acknowledge(handle);
    } catch (error) {
      const permanent =
        error instanceof SyntaxError ||
        error instanceof ZodError ||
        error instanceof DomainError ||
        (error instanceof ApplicationError && error.httpStatus < 500);
      const exhausted = message.receiveCount >= this.config.SQS_MAX_ATTEMPTS;
      this.telemetry.log('consumer_failed', {
        ...ctx,
        walletId: parsed?.data.walletId,
        providerId: parsed?.data.providerId,
        code: permanent
          ? 'PERMANENT_MESSAGE'
          : (databaseCode(error) ?? 'TRANSIENT_INFRASTRUCTURE'),
      });
      if (permanent || exhausted) {
        // Persist a terminal technical failure if the valid command and DB are available.
        // If the DB itself is down, the original DLQ envelope remains the recovery record.
        if (exhausted && !permanent && parsed) {
          const { idempotencyKey, ...business } = parsed.data;
          await this.service
            .process(business, idempotencyKey, {
              ...ctx,
              permanentFailure: true,
            })
            .catch(() => {});
        }
        const dedup = createHash('sha256').update(message.body).digest('hex');
        await this.queue.deadLetter(message.body, dedup);
        this.telemetry.dlq.inc();
        if (!this.releasing.has(handle)) await this.queue.acknowledge(handle);
      } else {
        this.telemetry.retries.inc({ worker: 'consumer' });
        await this.queue.changeVisibility(
          handle,
          Math.min(300, 2 ** Math.min(message.receiveCount, 8)),
        );
      }
    } finally {
      clearInterval(renewal);
      this.inFlight.delete(handle);
      this.releasing.delete(handle);
    }
  }
  async releaseInFlight(): Promise<void> {
    await Promise.all(
      [...this.inFlight].map(async (handle) => {
        this.releasing.add(handle);
        await this.queue.changeVisibility(handle, 0);
      }),
    );
  }
}

export class OutboxPublisher {
  constructor(
    private readonly db: DataSource,
    private readonly queue: QueuePort,
    private readonly telemetry: Telemetry,
  ) {}
  async tick(): Promise<boolean> {
    const published = await this.db.transaction(async (manager) => {
      const rows =
        await manager.query(`SELECT * FROM outbox_messages WHERE published_at IS NULL AND next_attempt_at <= now()
        ORDER BY occurred_at,id FOR UPDATE SKIP LOCKED LIMIT 1`);
      const row = rows[0];
      if (!row) return false;
      const message = OutboxMessage.rehydrate({
        id: row.id,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        payload: row.payload,
        occurredAt: row.occurred_at,
        attempts: row.attempts,
        nextAttemptAt: row.next_attempt_at,
      });
      try {
        // This row was committed by the use case before it could become visible here.
        await this.queue.publish(
          JSON.stringify(message.payload),
          message.aggregateId,
          message.id,
        );
        message.markPublished(new Date());
        await manager.query(
          'UPDATE outbox_messages SET published_at=$2 WHERE id=$1',
          [message.id, message.publishedAt],
        );
      } catch {
        message.scheduleRetry(new Date());
        await manager.query(
          'UPDATE outbox_messages SET attempts=$2,next_attempt_at=$3 WHERE id=$1',
          [message.id, message.attempts, message.nextAttemptAt],
        );
        this.telemetry.retries.inc({ worker: 'outbox' });
        this.telemetry.log('outbox_publish_retry', { code: 'SQS_UNAVAILABLE' });
      }
      return true;
    });
    const [lag] = await this.db
      .query(`SELECT coalesce(extract(epoch FROM now()-min(occurred_at)),0)::float AS seconds
      FROM outbox_messages WHERE published_at IS NULL`);
    this.telemetry.outboxLag.set(Math.max(0, Number(lag.seconds)));
    return published;
  }
}

export class ReferenceWorker {
  private stopped = false;
  constructor(private readonly service: WageringService) {}
  stop(): void {
    this.stopped = true;
  }
  async tick(): Promise<void> {
    const rows: { id: string }[] = await this.service.db
      .query(`SELECT id FROM wager_transactions
      WHERE status='PENDING_REFERENCE' AND next_reference_at <= now() ORDER BY next_reference_at,id LIMIT 20`);
    // The use case locks wallet first, then rechecks the due transaction. No global lock.
    for (const row of rows) {
      if (this.stopped) break;
      await this.service.retryReference(row.id);
    }
  }
}

export class WorkerSupervisor {
  private stopped = false;
  private tasks: Promise<void>[] = [];
  constructor(
    private readonly consumer: WagerConsumer,
    private readonly publisher: OutboxPublisher,
    private readonly references: ReferenceWorker,
    private readonly config: Config,
    private readonly telemetry: Telemetry,
  ) {}
  start(): void {
    this.tasks = [
      () => this.consumer.tick(),
      () => this.publisher.tick(),
      () => this.references.tick(),
    ].map((tick) => this.loop(tick));
  }
  private async loop(tick: () => Promise<unknown>): Promise<void> {
    while (!this.stopped) {
      try {
        await tick();
      } catch {
        this.telemetry.log('worker_retry', {
          code: 'INFRASTRUCTURE_UNAVAILABLE',
        });
      }
      if (!this.stopped) await Bun.sleep(this.config.WORKER_INTERVAL_MS);
    }
  }
  async stop(): Promise<void> {
    this.stopped = true;
    this.references.stop();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const finished = Promise.allSettled(this.tasks);
    const completed = await Promise.race([
      finished.then(() => true),
      new Promise<false>((resolve) => {
        timer = setTimeout(
          () => resolve(false),
          this.config.SHUTDOWN_TIMEOUT_MS,
        );
      }),
    ]);
    clearTimeout(timer);
    if (!completed) {
      await this.consumer.releaseInFlight();
      this.telemetry.log('shutdown_visibility_returned');
      // All I/O has bounded timeouts. Finish SQL before destroying its connection pool.
      await finished;
    }
  }
}
