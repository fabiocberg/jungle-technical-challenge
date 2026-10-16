import { DomainError } from './errors';
import type { IntegrationEvent } from './events';

interface InboxState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}
export class InboxMessage {
  private constructor(private state: InboxState) {}
  static receive(
    props: Omit<InboxState, 'receivedAt' | 'processedAt'>,
  ): InboxMessage {
    return new InboxMessage({ ...props, receivedAt: new Date() });
  }
  static rehydrate(state: InboxState): InboxMessage {
    return new InboxMessage({ ...state });
  }
  get messageId(): string {
    return this.state.messageId;
  }
  get consumerName(): string {
    return this.state.consumerName;
  }
  get payloadHash(): string {
    return this.state.payloadHash;
  }
  get receivedAt(): Date {
    return new Date(this.state.receivedAt);
  }
  get processedAt(): Date | undefined {
    return this.state.processedAt && new Date(this.state.processedAt);
  }
  isProcessed(): boolean {
    return !!this.state.processedAt;
  }
  markProcessed(at: Date): void {
    if (this.isProcessed()) throw new DomainError('INBOX_ALREADY_PROCESSED');
    this.state = { ...this.state, processedAt: new Date(at) };
  }
}
interface OutboxState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}
export class OutboxMessage {
  private constructor(private state: OutboxState) {}
  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage({
      id: event.eventId,
      aggregateId: event.aggregateId,
      eventType: event.eventType,
      payload: event.toJSON(),
      occurredAt: event.occurredAt,
      attempts: 0,
    });
  }
  static rehydrate(state: OutboxState): OutboxMessage {
    return new OutboxMessage({ ...state });
  }
  get id(): string {
    return this.state.id;
  }
  get aggregateId(): string {
    return this.state.aggregateId;
  }
  get eventType(): string {
    return this.state.eventType;
  }
  get payload(): Readonly<Record<string, unknown>> {
    return structuredClone(this.state.payload);
  }
  get occurredAt(): Date {
    return new Date(this.state.occurredAt);
  }
  get attempts(): number {
    return this.state.attempts;
  }
  get nextAttemptAt(): Date | undefined {
    return this.state.nextAttemptAt && new Date(this.state.nextAttemptAt);
  }
  get publishedAt(): Date | undefined {
    return this.state.publishedAt && new Date(this.state.publishedAt);
  }
  isPending(): boolean {
    return !this.state.publishedAt;
  }
  isDue(now: Date): boolean {
    return (
      this.isPending() &&
      (!this.state.nextAttemptAt || this.state.nextAttemptAt <= now)
    );
  }
  markPublished(at: Date): void {
    this.state = { ...this.state, publishedAt: new Date(at) };
  }
  scheduleRetry(now: Date): void {
    const attempts = this.attempts + 1;
    this.state = {
      ...this.state,
      attempts,
      nextAttemptAt: new Date(
        now.getTime() + Math.min(60_000, 1000 * 2 ** Math.min(attempts - 1, 6)),
      ),
    };
  }
}
