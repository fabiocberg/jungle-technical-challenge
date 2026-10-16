import type { MoneyProps } from './money';
import type { LedgerDirection, WalletLedgerEntry } from './ledger';
import type { Wallet } from './wallet';
import type { WagerTransaction } from './transaction';

export interface EventContext {
  correlationId: string;
  causationId?: string;
}
export interface IntegrationEventProps<T> extends EventContext {
  eventId: string;
  aggregateId: string;
  occurredAt: Date;
  data: T;
}
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;
  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;
  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = new Date(props.occurredAt);
    this.data = Object.freeze(structuredClone(props.data));
  }
  toJSON() {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

interface TransactionEventData {
  transactionId: string;
  providerId: string;
  walletId: string;
  status: string;
  failureCode?: string;
}
function transactionProps(
  tx: WagerTransaction,
  ctx: EventContext,
): IntegrationEventProps<TransactionEventData> {
  return {
    ...ctx,
    eventId: crypto.randomUUID(),
    aggregateId: tx.walletId,
    occurredAt: new Date(),
    data: {
      transactionId: tx.id,
      providerId: tx.providerId,
      walletId: tx.walletId,
      status: tx.status,
      failureCode: tx.failureCode,
    },
  };
}
export class WagerTransactionProcessed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;
  static from(
    tx: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    return new WagerTransactionProcessed(transactionProps(tx, ctx));
  }
}
export class WagerTransactionRejected extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;
  static from(
    tx: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionRejected {
    return new WagerTransactionRejected(transactionProps(tx, ctx));
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;
  static from(
    tx: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(transactionProps(tx, ctx));
  }
}
export class WagerTransactionFailed extends IntegrationEvent<TransactionEventData> {
  readonly eventType = 'WagerTransactionFailed';
  readonly version = 1;
  static from(tx: WagerTransaction, ctx: EventContext): WagerTransactionFailed {
    return new WagerTransactionFailed(transactionProps(tx, ctx));
  }
}
interface BalanceData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}
export class WalletBalanceChanged extends IntegrationEvent<BalanceData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;
  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...ctx,
      eventId: crypto.randomUUID(),
      aggregateId: wallet.id,
      occurredAt: new Date(),
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}
