import type { DataSource, EntityManager } from 'typeorm';
import {
  ApplicationError,
  DomainError,
  type FailureCode,
} from '../domain/errors';
import { Money, type MoneyProps } from '../domain/money';
import { Wallet } from '../domain/wallet';
import {
  WagerTransaction,
  type TransactionState,
  type Kind,
  type Status,
} from '../domain/transaction';
import { WalletLedgerEntry } from '../domain/ledger';
import { assertReplay, payloadHash } from '../domain/idempotency';
import { InboxMessage, OutboxMessage } from '../domain/messages';
import {
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WagerTransactionPendingReference,
  WagerTransactionFailed,
  WalletBalanceChanged,
  type IntegrationEvent,
  type EventContext,
} from '../domain/events';
import { WalletSchema, databaseCode } from '../infrastructure/database';
import type { Config } from '../infrastructure/config';
import type { Telemetry } from '../infrastructure/telemetry';
import {
  validateCommand,
  walletSchema,
  type TransactionCommand,
} from './contracts';

export interface TransactionResult {
  transactionId: string;
  status: Status;
  balance: MoneyProps;
  failureCode?: FailureCode;
  idempotentReplay: boolean;
}
export interface ProcessingContext extends EventContext {
  messageId?: string;
  consumerName?: string;
  inboxHash?: string;
  permanentFailure?: boolean;
}
export interface TransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  failure_code: FailureCode | null;
  status: Status;
  created_at: Date;
  processed_at: Date | null;
  response: TransactionResult;
  reference_attempts: number;
  correlation_id: string;
  causation_id: string | null;
}
export function hydrateTransaction(row: TransactionRow): WagerTransaction {
  const state: TransactionState = {
    id: row.id,
    providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash,
    walletId: row.wallet_id,
    playerId: row.player_id,
    roundId: row.round_id,
    gameId: row.game_id,
    kind: row.kind,
    money: Money.rehydrate({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId:
      row.reference_external_transaction_id ?? undefined,
    referenceTransactionId: row.reference_transaction_id ?? undefined,
    failureCode: row.failure_code ?? undefined,
    status: row.status,
    createdAt: row.created_at,
    processedAt: row.processed_at ?? undefined,
  };
  return WagerTransaction.rehydrate(state);
}

export class WageringService {
  constructor(
    readonly db: DataSource,
    readonly config: Config,
    readonly telemetry: Telemetry,
  ) {}

  async openWallet(input: unknown, ctx: EventContext) {
    const command = walletSchema.parse(input);
    const initial = Money.from(command.initialBalance);
    const wallet = Wallet.open({
      id: crypto.randomUUID(),
      playerId: command.playerId,
      initialBalance: initial,
    });
    try {
      return await this.db.transaction(async (manager) => {
        await manager.getRepository(WalletSchema).insert({
          id: wallet.id,
          playerId: wallet.playerId,
          currency: wallet.currency,
          balance: '0.00',
          version: 1,
          createdAt: wallet.createdAt,
          updatedAt: wallet.updatedAt,
        });
        if (initial.isPositive()) {
          const tx = WagerTransaction.create({
            id: crypto.randomUUID(),
            providerId: 'system',
            externalTransactionId: wallet.id,
            idempotencyKey: `opening:${wallet.id}`,
            payloadHash: payloadHash(command),
            walletId: wallet.id,
            playerId: wallet.playerId,
            roundId: 'opening',
            gameId: 'opening',
            kind: 'OPENING',
            money: initial,
          });
          await this.insertTransaction(manager, tx, ctx);
          tx.markProcessed(undefined, new Date());
          await this.storeTransition(manager, tx, wallet, ctx);
          const entry = WalletLedgerEntry.create({
            id: crypto.randomUUID(),
            walletId: wallet.id,
            transactionId: tx.id,
            direction: 'CREDIT',
            money: initial,
            balanceBefore: Money.zero(initial.currency),
            balanceAfter: initial,
            createdAt: new Date(),
          });
          await this.insertLedger(manager, entry);
          await this.enqueue(
            manager,
            WalletBalanceChanged.from(wallet, entry, ctx),
          );
        }
        return this.walletView(wallet);
      });
    } catch (error) {
      if (databaseCode(error) === '23505')
        throw new ApplicationError('WALLET_ALREADY_EXISTS', 409);
      throw error;
    }
  }

  async process(
    input: unknown,
    key: unknown,
    ctx: ProcessingContext,
  ): Promise<TransactionResult> {
    const command = validateCommand(input, key);
    const { idempotencyKey: _, ...business } = command;
    const hash = payloadHash(business);
    // Validate domain shape before opening SQL (e.g. no reference/self-reference/zero BET).
    const tx = WagerTransaction.create({
      ...command,
      money: Money.from(command.money),
      id: crypto.randomUUID(),
      payloadHash: hash,
    });
    const stopTimer = this.telemetry.latency.startTimer();
    let result: TransactionResult;
    try {
      result = await this.db.transaction(async (manager) => {
        await this.lockTimeout(manager);
        if (ctx.messageId) {
          const inbox: { payload_hash: string; transaction_id: string }[] =
            await manager.query(
              'SELECT payload_hash, transaction_id FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2',
              [ctx.consumerName ?? 'wager-consumer', ctx.messageId],
            );
          if (inbox[0]) {
            assertReplay(inbox[0].payload_hash, ctx.inboxHash ?? hash);
            const original = await this.findTransaction(manager, 'id=$1', [
              inbox[0].transaction_id,
            ]);
            this.telemetry.duplicates.inc({ source: 'inbox' });
            return { ...original.response, idempotentReplay: true };
          }
        }
        const wallet = await this.lockWallet(manager, command.walletId);
        const existing = await this.findExisting(manager, command);
        if (existing) return this.replay(manager, existing, hash, command, ctx);
        const inserted = await this.insertTransaction(manager, tx, ctx);
        if (!inserted) {
          const concurrent = await this.findExisting(manager, command);
          if (!concurrent) throw new Error('IDEMPOTENCY_INSERT_RACE');
          return this.replay(manager, concurrent, hash, command, ctx);
        }
        if (ctx.permanentFailure) tx.fail('PERMANENT_INFRASTRUCTURE_ERROR');
        else await this.apply(manager, tx, wallet, ctx);
        const response = await this.storeTransition(manager, tx, wallet, ctx);
        await this.recordInbox(manager, tx.id, hash, ctx);
        return response;
      });
    } catch (error) {
      if (['55P03', '40P01', '40001'].includes(databaseCode(error) ?? ''))
        this.telemetry.lockConflicts.inc();
      throw error;
    } finally {
      stopTimer();
    }
    if (result.idempotentReplay)
      this.telemetry.duplicates.inc({ source: 'transaction' });
    else this.telemetry.transactions.inc({ status: result.status });
    this.telemetry.log('transaction_committed', {
      ...this.logContext(ctx),
      transactionId: result.transactionId,
      walletId: command.walletId,
      providerId: command.providerId,
    });
    return result;
  }

  private async apply(
    manager: EntityManager,
    tx: WagerTransaction,
    wallet: Wallet,
    ctx: EventContext,
  ): Promise<void> {
    if (wallet.playerId !== tx.playerId) {
      tx.reject('PLAYER_MISMATCH');
      return;
    }
    if (wallet.currency !== tx.money.currency) {
      tx.reject('CURRENCY_MISMATCH');
      return;
    }
    let reference: WagerTransaction | undefined;
    if (tx.requiresReference()) {
      const rows: TransactionRow[] = await manager.query(
        'SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2',
        [tx.providerId, tx.referenceExternalTransactionId],
      );
      if (
        !rows[0] ||
        ['PENDING', 'PENDING_REFERENCE'].includes(rows[0].status)
      ) {
        tx.markPendingReference();
        return;
      }
      reference = hydrateTransaction(rows[0]);
      const error = tx.validateReference(reference);
      if (error) {
        tx.reject(error);
        return;
      }
      if (['REFUND', 'ROLLBACK'].includes(tx.kind)) {
        const reversed: { id: string }[] = await manager.query(
          "SELECT id FROM wager_transactions WHERE reference_transaction_id=$1 AND kind=$2 AND status='PROCESSED'",
          [reference.id, tx.kind],
        );
        if (reversed.length) {
          tx.reject('REFERENCE_ALREADY_REVERSED');
          return;
        }
      }
    }
    let entry: WalletLedgerEntry | undefined;
    if (tx.affectsBalance()) {
      const direction = tx.ledgerDirectionFor(reference);
      if (direction === 'DEBIT' && wallet.balance.isLessThan(tx.money)) {
        tx.reject(
          tx.kind === 'ROLLBACK'
            ? 'REVERSAL_INSUFFICIENT_FUNDS'
            : 'INSUFFICIENT_FUNDS',
        );
        return;
      }
      try {
        entry =
          direction === 'DEBIT'
            ? wallet.debit(tx.id, tx.money)
            : wallet.credit(tx.id, tx.money);
      } catch (error) {
        if (error instanceof DomainError && error.code === 'AMOUNT_OVERFLOW') {
          tx.reject('WALLET_CAPACITY_EXCEEDED');
          return;
        }
        throw error;
      }
    }
    tx.markProcessed(reference?.id, new Date());
    // The processed state and ledger are checked together by deferred DB constraints.
    const response: TransactionResult = {
      transactionId: tx.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      idempotentReplay: false,
    };
    await manager.query(
      'UPDATE wager_transactions SET status=$2, reference_transaction_id=$3, processed_at=$4,response=$5 WHERE id=$1',
      [
        tx.id,
        tx.status,
        tx.referenceTransactionId ?? null,
        tx.processedAt,
        response,
      ],
    );
    if (entry) {
      await this.insertLedger(manager, entry);
      await this.enqueue(
        manager,
        WalletBalanceChanged.from(wallet, entry, ctx),
      );
    }
  }

  async retryReference(id: string): Promise<void> {
    await this.db.transaction(async (manager) => {
      await this.lockTimeout(manager);
      const initial: TransactionRow[] = await manager.query(
        "SELECT * FROM wager_transactions WHERE id=$1 AND status='PENDING_REFERENCE'",
        [id],
      );
      if (!initial[0]) return;
      const wallet = await this.lockWallet(manager, initial[0].wallet_id);
      const rows: TransactionRow[] = await manager.query(
        "SELECT * FROM wager_transactions WHERE id=$1 AND status='PENDING_REFERENCE' AND next_reference_at <= now() FOR UPDATE",
        [id],
      );
      const row = rows[0];
      if (!row) return;
      const tx = hydrateTransaction(row);
      const ctx = {
        correlationId: row.correlation_id,
        causationId: row.causation_id ?? undefined,
      };
      await this.apply(manager, tx, wallet, ctx);
      const attempts = row.reference_attempts + 1;
      if (
        tx.status === 'PENDING_REFERENCE' &&
        attempts >= this.config.REFERENCE_MAX_ATTEMPTS
      )
        tx.reject('REFERENCE_NOT_FOUND');
      if (tx.status === 'PENDING_REFERENCE') {
        await manager.query(
          'UPDATE wager_transactions SET reference_attempts=$2,next_reference_at=$3 WHERE id=$1',
          [
            id,
            attempts,
            new Date(
              Date.now() +
                this.config.REFERENCE_BACKOFF_MS * 2 ** Math.min(attempts, 16),
            ),
          ],
        );
      } else {
        await this.storeTransition(manager, tx, wallet, ctx);
        this.telemetry.transactions.inc({ status: tx.status });
      }
      this.telemetry.retries.inc({ worker: 'reference' });
    });
  }

  private async storeTransition(
    manager: EntityManager,
    tx: WagerTransaction,
    wallet: Wallet,
    ctx: EventContext,
  ): Promise<TransactionResult> {
    const response: TransactionResult = {
      transactionId: tx.id,
      status: tx.status,
      balance: wallet.balance.toJSON(),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      idempotentReplay: false,
    };
    // apply writes the complete terminal state before inserting its ledger entry.
    if (tx.status !== 'PROCESSED' || tx.kind === 'OPENING') {
      await manager.query(
        'UPDATE wager_transactions SET status=$2, reference_transaction_id=$3, failure_code=$4, processed_at=$5,response=$6 WHERE id=$1',
        [
          tx.id,
          tx.status,
          tx.referenceTransactionId ?? null,
          tx.failureCode ?? null,
          tx.processedAt ?? null,
          response,
        ],
      );
    }
    const event =
      tx.status === 'PROCESSED'
        ? WagerTransactionProcessed.from(tx, ctx)
        : tx.status === 'REJECTED'
          ? WagerTransactionRejected.from(tx, ctx)
          : tx.status === 'FAILED'
            ? WagerTransactionFailed.from(tx, ctx)
            : WagerTransactionPendingReference.from(tx, ctx);
    await this.enqueue(manager, event);
    return response;
  }

  private async insertTransaction(
    manager: EntityManager,
    tx: WagerTransaction,
    ctx: EventContext,
  ): Promise<boolean> {
    const rows: { id: string }[] = await manager.query(
      `INSERT INTO wager_transactions
      (id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,
       status,reference_external_transaction_id,correlation_id,causation_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'PENDING',$13,$14,$15) ON CONFLICT DO NOTHING RETURNING id`,
      [
        tx.id,
        tx.providerId,
        tx.externalTransactionId,
        tx.idempotencyKey,
        tx.payloadHash,
        tx.walletId,
        tx.playerId,
        tx.roundId,
        tx.gameId,
        tx.kind,
        tx.money.toString(),
        tx.money.currency,
        tx.referenceExternalTransactionId ?? null,
        ctx.correlationId,
        ctx.causationId ?? null,
      ],
    );
    return !!rows[0];
  }
  private async insertLedger(
    manager: EntityManager,
    entry: WalletLedgerEntry,
  ): Promise<void> {
    await manager.query(
      `INSERT INTO wallet_ledger (id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        entry.id,
        entry.walletId,
        entry.transactionId,
        entry.direction,
        entry.money.toString(),
        entry.money.currency,
        entry.balanceBefore.toString(),
        entry.balanceAfter.toString(),
        entry.createdAt,
      ],
    );
  }
  private async enqueue(
    manager: EntityManager,
    event: IntegrationEvent<unknown>,
  ): Promise<void> {
    const message = OutboxMessage.enqueue(event);
    await manager.query(
      'INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES ($1,$2,$3,$4,$5)',
      [
        message.id,
        message.aggregateId,
        message.eventType,
        message.payload,
        message.occurredAt,
      ],
    );
  }
  private async recordInbox(
    manager: EntityManager,
    txId: string,
    hash: string,
    ctx: ProcessingContext,
  ): Promise<void> {
    if (!ctx.messageId) return;
    const inbox = InboxMessage.receive({
      messageId: ctx.messageId,
      consumerName: ctx.consumerName ?? 'wager-consumer',
      payloadHash: ctx.inboxHash ?? hash,
    });
    inbox.markProcessed(new Date());
    await manager.query(
      `INSERT INTO inbox_messages (consumer_name,message_id,payload_hash,transaction_id,processed_at)
      VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
      [
        inbox.consumerName,
        inbox.messageId,
        inbox.payloadHash,
        txId,
        inbox.processedAt,
      ],
    );
    const rows: { payload_hash: string }[] = await manager.query(
      'SELECT payload_hash FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2',
      [inbox.consumerName, inbox.messageId],
    );
    assertReplay(rows[0]!.payload_hash, inbox.payloadHash);
  }
  private async replay(
    manager: EntityManager,
    existing: TransactionRow,
    hash: string,
    command: TransactionCommand,
    ctx: ProcessingContext,
  ): Promise<TransactionResult> {
    assertReplay(existing.payload_hash, hash);
    if (
      existing.idempotency_key !== command.idempotencyKey ||
      existing.provider_id !== command.providerId ||
      existing.external_transaction_id !== command.externalTransactionId
    ) {
      throw new DomainError('IDEMPOTENCY_CONFLICT');
    }
    await this.recordInbox(manager, existing.id, hash, ctx);
    return { ...existing.response, idempotentReplay: true };
  }
  private async findExisting(
    manager: EntityManager,
    command: TransactionCommand,
  ): Promise<TransactionRow | undefined> {
    const rows: TransactionRow[] = await manager.query(
      'SELECT * FROM wager_transactions WHERE idempotency_key=$1 OR (provider_id=$2 AND external_transaction_id=$3)',
      [
        command.idempotencyKey,
        command.providerId,
        command.externalTransactionId,
      ],
    );
    if (rows.length > 1) throw new DomainError('IDEMPOTENCY_CONFLICT');
    return rows[0];
  }
  private async findTransaction(
    manager: EntityManager,
    condition: string,
    args: unknown[],
  ): Promise<TransactionRow> {
    const rows: TransactionRow[] = await manager.query(
      `SELECT * FROM wager_transactions WHERE ${condition}`,
      args,
    );
    if (!rows[0]) throw new ApplicationError('TRANSACTION_NOT_FOUND', 404);
    return rows[0];
  }
  private async lockTimeout(manager: EntityManager): Promise<void> {
    await manager.query("SELECT set_config('lock_timeout',$1,true)", [
      `${this.config.DB_LOCK_TIMEOUT_MS}ms`,
    ]);
  }
  private async lockWallet(
    manager: EntityManager,
    id: string,
  ): Promise<Wallet> {
    const row = await manager
      .getRepository(WalletSchema)
      .findOne({ where: { id }, lock: { mode: 'pessimistic_write' } });
    if (!row) throw new ApplicationError('WALLET_NOT_FOUND', 404);
    return Wallet.rehydrate({
      ...row,
      balance: Money.rehydrate({ amount: row.balance, currency: row.currency }),
    });
  }
  walletView(wallet: Wallet) {
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
    };
  }
  private logContext(ctx: ProcessingContext) {
    return { correlationId: ctx.correlationId, messageId: ctx.messageId };
  }
}
