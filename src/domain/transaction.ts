import { DomainError, type FailureCode } from './errors';
import { Money } from './money';
import type { LedgerDirection } from './ledger';

export type Kind = 'OPENING' | 'BET' | 'WIN' | 'LOSS' | 'REFUND' | 'ROLLBACK';
export type Status =
  'PENDING' | 'PENDING_REFERENCE' | 'PROCESSED' | 'REJECTED' | 'FAILED';
export interface TransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: Money;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  createdAt: Date;
  status: Status;
  failureCode?: FailureCode;
  processedAt?: Date;
}

export class WagerTransaction {
  private constructor(private state: TransactionState) {}
  static create(
    props: Omit<TransactionState, 'status' | 'createdAt'>,
  ): WagerTransaction {
    if (
      ['REFUND', 'ROLLBACK'].includes(props.kind) &&
      !props.referenceExternalTransactionId
    )
      throw new DomainError('REFERENCE_REQUIRED');
    if (props.referenceExternalTransactionId === props.externalTransactionId)
      throw new DomainError('SELF_REFERENCE');
    if (props.kind === 'BET' && props.referenceExternalTransactionId)
      throw new DomainError('REFERENCE_NOT_ALLOWED');
    if (
      props.money.isNegative() ||
      (props.kind === 'LOSS'
        ? !props.money.isZero()
        : !props.money.isPositive())
    ) {
      throw new DomainError('INVALID_OPERATION_AMOUNT');
    }
    return new WagerTransaction({
      ...props,
      status: 'PENDING',
      createdAt: new Date(),
    });
  }
  static rehydrate(state: TransactionState): WagerTransaction {
    return new WagerTransaction({
      ...state,
      createdAt: new Date(state.createdAt),
      processedAt: state.processedAt && new Date(state.processedAt),
    });
  }
  get id(): string {
    return this.state.id;
  }
  get providerId(): string {
    return this.state.providerId;
  }
  get externalTransactionId(): string {
    return this.state.externalTransactionId;
  }
  get idempotencyKey(): string {
    return this.state.idempotencyKey;
  }
  get payloadHash(): string {
    return this.state.payloadHash;
  }
  get walletId(): string {
    return this.state.walletId;
  }
  get playerId(): string {
    return this.state.playerId;
  }
  get roundId(): string {
    return this.state.roundId;
  }
  get gameId(): string {
    return this.state.gameId;
  }
  get kind(): Kind {
    return this.state.kind;
  }
  get money(): Money {
    return this.state.money;
  }
  get referenceExternalTransactionId(): string | undefined {
    return this.state.referenceExternalTransactionId;
  }
  get referenceTransactionId(): string | undefined {
    return this.state.referenceTransactionId;
  }
  get status(): Status {
    return this.state.status;
  }
  get failureCode(): FailureCode | undefined {
    return this.state.failureCode;
  }
  get createdAt(): Date {
    return new Date(this.state.createdAt);
  }
  get processedAt(): Date | undefined {
    return this.state.processedAt && new Date(this.state.processedAt);
  }
  isTerminal(): boolean {
    return ['PROCESSED', 'REJECTED', 'FAILED'].includes(this.status);
  }
  affectsBalance(): boolean {
    return this.kind !== 'LOSS';
  }
  requiresReference(): boolean {
    return (
      ['REFUND', 'ROLLBACK'].includes(this.kind) ||
      !!this.referenceExternalTransactionId
    );
  }
  matchesPayload(hash: string): boolean {
    return this.payloadHash === hash;
  }
  markProcessed(reference: string | undefined, at: Date): void {
    this.assertMutable();
    this.state = {
      ...this.state,
      status: 'PROCESSED',
      referenceTransactionId: reference,
      processedAt: new Date(at),
    };
  }
  markPendingReference(): void {
    this.assertMutable();
    this.state = { ...this.state, status: 'PENDING_REFERENCE' };
  }
  reject(code: FailureCode): void {
    this.finish('REJECTED', code);
  }
  fail(code: FailureCode): void {
    this.finish('FAILED', code);
  }
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    if (!this.affectsBalance()) throw new DomainError('NO_LEDGER_FOR_LOSS');
    if (this.kind === 'BET') return 'DEBIT';
    if (this.kind === 'ROLLBACK') {
      if (!reference || !['BET', 'WIN', 'REFUND'].includes(reference.kind))
        throw new DomainError('REFERENCE_KIND_INVALID');
      return reference.kind === 'BET' ? 'CREDIT' : 'DEBIT';
    }
    return 'CREDIT';
  }
  validateReference(reference: WagerTransaction): FailureCode | undefined {
    if (
      reference.providerId !== this.providerId ||
      reference.playerId !== this.playerId ||
      reference.walletId !== this.walletId ||
      reference.roundId !== this.roundId ||
      reference.money.currency !== this.money.currency
    )
      return 'REFERENCE_MISMATCH';
    if (reference.status !== 'PROCESSED') return 'REFERENCE_NOT_PROCESSED';
    const allowed =
      this.kind === 'ROLLBACK' ? ['BET', 'WIN', 'REFUND'] : ['BET'];
    if (!allowed.includes(reference.kind)) return 'REFERENCE_KIND_INVALID';
    if (
      ['REFUND', 'ROLLBACK'].includes(this.kind) &&
      !this.money.equals(reference.money)
    )
      return 'REFERENCE_AMOUNT_MISMATCH';
    return undefined;
  }
  private finish(status: 'REJECTED' | 'FAILED', code: FailureCode): void {
    this.assertMutable();
    this.state = {
      ...this.state,
      status,
      failureCode: code,
      processedAt: new Date(),
    };
  }
  private assertMutable(): void {
    if (this.isTerminal()) throw new DomainError('INVALID_TRANSACTION_STATE');
  }
}
