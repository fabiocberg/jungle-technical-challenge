import { DomainError } from './errors';

export interface MoneyProps {
  amount: string;
  currency: string;
}

/** Exact fixed-scale money: bigint minor units, never IEEE-754 arithmetic. */
export class Money {
  private constructor(
    private readonly minor: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (
      typeof props.amount !== 'string' ||
      !/^(0|[1-9]\d{0,25})\.\d{2}$/.test(props.amount)
    ) {
      throw new DomainError('INVALID_AMOUNT');
    }
    if (
      !/^[A-Z]{3}$/.test(props.currency) ||
      !['BRL', 'USD', 'EUR'].includes(props.currency)
    ) {
      throw new DomainError('INVALID_CURRENCY');
    }
    return new Money(BigInt(props.amount.replace('.', '')), props.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }
  /** Persistence is trusted; signed amounts also represent reconciliation differences. */
  static rehydrate(props: MoneyProps): Money {
    const negative = props.amount.startsWith('-');
    const money = Money.from({
      ...props,
      amount: negative ? props.amount.slice(1) : props.amount,
    });
    return negative ? money.negate() : money;
  }
  add(other: Money): Money {
    this.assertSameCurrency(other);
    const result = new Money(this.minor + other.minor, this.currency);
    if (result.minor > 10n ** 28n - 1n || result.minor < -(10n ** 28n - 1n)) {
      throw new DomainError('AMOUNT_OVERFLOW');
    }
    return result;
  }
  subtract(other: Money): Money {
    return this.add(other.negate());
  }
  negate(): Money {
    return new Money(-this.minor, this.currency);
  }
  isZero(): boolean {
    return this.minor === 0n;
  }
  isPositive(): boolean {
    return this.minor > 0n;
  }
  isNegative(): boolean {
    return this.minor < 0n;
  }
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor < other.minor;
  }
  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor === other.minor;
  }
  toString(): string {
    const absolute = this.minor < 0n ? -this.minor : this.minor;
    return `${this.minor < 0n ? '-' : ''}${absolute / 100n}.${(absolute % 100n).toString().padStart(2, '0')}`;
  }
  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency)
      throw new DomainError('CURRENCY_MISMATCH');
  }
}
