import { DomainError } from './errors';
import { WalletLedgerEntry, type LedgerDirection } from './ledger';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export class Wallet {
  private constructor(private state: WalletState) {}
  static open(props: {
    id: string;
    playerId: string;
    initialBalance: Money;
  }): Wallet {
    if (props.initialBalance.isNegative())
      throw new DomainError('NEGATIVE_BALANCE');
    const now = new Date();
    return new Wallet({
      id: props.id,
      playerId: props.playerId,
      currency: props.initialBalance.currency,
      balance: props.initialBalance,
      version: 1,
      createdAt: now,
      updatedAt: now,
    });
  }
  static rehydrate(state: WalletState): Wallet {
    return new Wallet({
      ...state,
      createdAt: new Date(state.createdAt),
      updatedAt: new Date(state.updatedAt),
    });
  }
  get id(): string {
    return this.state.id;
  }
  get playerId(): string {
    return this.state.playerId;
  }
  get currency(): string {
    return this.state.currency;
  }
  get balance(): Money {
    return this.state.balance;
  }
  get version(): number {
    return this.state.version;
  }
  get createdAt(): Date {
    return new Date(this.state.createdAt);
  }
  get updatedAt(): Date {
    return new Date(this.state.updatedAt);
  }
  debit(transactionId: string, money: Money): WalletLedgerEntry {
    return this.move(transactionId, money, 'DEBIT');
  }
  credit(transactionId: string, money: Money): WalletLedgerEntry {
    return this.move(transactionId, money, 'CREDIT');
  }
  private move(
    transactionId: string,
    money: Money,
    direction: LedgerDirection,
  ): WalletLedgerEntry {
    if (money.currency !== this.currency)
      throw new DomainError('CURRENCY_MISMATCH');
    if (!money.isPositive()) throw new DomainError('AMOUNT_MUST_BE_POSITIVE');
    const before = this.balance;
    const after =
      direction === 'CREDIT' ? before.add(money) : before.subtract(money);
    if (after.isNegative()) throw new DomainError('INSUFFICIENT_FUNDS');
    const entry = WalletLedgerEntry.create({
      id: crypto.randomUUID(),
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
      createdAt: new Date(),
    });
    this.state = {
      ...this.state,
      balance: after,
      version: this.version + 1,
      updatedAt: entry.createdAt,
    };
    return entry;
  }
}
