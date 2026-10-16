import { z } from 'zod';
import type { DataSource } from 'typeorm';
import { ApplicationError } from '../domain/errors';
import { Money } from '../domain/money';
import { WalletSchema } from '../infrastructure/database';
import type { Telemetry } from '../infrastructure/telemetry';
import type { TransactionRow } from './wagering';

const cursorSchema = z.strictObject({
  walletId: z.uuid(),
  after: z.string().regex(/^\d+$/),
  high: z.string().regex(/^\d+$/),
});
export class QueryService {
  constructor(
    private readonly db: DataSource,
    private readonly telemetry: Telemetry,
  ) {}
  async wallet(id: string) {
    z.uuid().parse(id);
    const row = await this.db.getRepository(WalletSchema).findOneBy({ id });
    if (!row) throw new ApplicationError('WALLET_NOT_FOUND', 404);
    return {
      id: row.id,
      playerId: row.playerId,
      balance: { amount: row.balance, currency: row.currency },
      version: row.version,
    };
  }
  async transaction(id: string) {
    z.uuid().parse(id);
    return this.find('id=$1', [id]);
  }
  async externalTransaction(providerId: string, externalId: string) {
    z.string().min(1).max(128).parse(providerId);
    z.string().min(1).max(128).parse(externalId);
    return this.find('provider_id=$1 AND external_transaction_id=$2', [
      providerId,
      externalId,
    ]);
  }
  private async find(where: string, args: string[]) {
    const rows: TransactionRow[] = await this.db.query(
      `SELECT * FROM wager_transactions WHERE ${where}`,
      args,
    );
    const row = rows[0];
    if (!row) throw new ApplicationError('TRANSACTION_NOT_FOUND', 404);
    return {
      id: row.id,
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      playerId: row.player_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      money: { amount: row.amount, currency: row.currency },
      status: row.status,
      referenceExternalTransactionId: row.reference_external_transaction_id,
      referenceTransactionId: row.reference_transaction_id,
      failureCode: row.failure_code,
      createdAt: row.created_at.toISOString(),
      processedAt: row.processed_at?.toISOString() ?? null,
      result: row.response,
    };
  }
  async ledger(walletId: string, query: unknown) {
    await this.wallet(walletId);
    const { limit, cursor } = z
      .strictObject({
        limit: z.coerce.number().int().min(1).max(100).default(50),
        cursor: z.string().max(1024).optional(),
      })
      .parse(query);
    let after = '0',
      high: string;
    if (cursor) {
      try {
        const decoded = cursorSchema.parse(
          JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')),
        );
        if (
          decoded.walletId !== walletId ||
          BigInt(decoded.after) > BigInt(decoded.high) ||
          BigInt(decoded.high) > 9223372036854775807n
        )
          throw new Error();
        after = decoded.after;
        high = decoded.high;
      } catch {
        throw new ApplicationError('INVALID_CURSOR', 400);
      }
    } else {
      const [row] = await this.db.query(
        'SELECT coalesce(max(sequence),0)::text AS high FROM wallet_ledger WHERE wallet_id=$1',
        [walletId],
      );
      high = row.high;
    }
    const rows = await this.db.query(
      `SELECT * FROM wallet_ledger WHERE wallet_id=$1 AND sequence>$2::bigint AND sequence<=$3::bigint
      ORDER BY sequence LIMIT $4`,
      [walletId, after, high, limit + 1],
    );
    const page = rows.slice(0, limit);
    const nextCursor =
      rows.length > limit
        ? Buffer.from(
            JSON.stringify({ walletId, after: page.at(-1).sequence, high }),
          ).toString('base64url')
        : null;
    return {
      items: page.map((row: Record<string, unknown>) => ({
        id: row.id,
        transactionId: row.transaction_id,
        walletId,
        direction: row.direction,
        money: { amount: row.amount, currency: row.currency },
        balanceBefore: { amount: row.balance_before, currency: row.currency },
        balanceAfter: { amount: row.balance_after, currency: row.currency },
        createdAt: (row.created_at as Date).toISOString(),
      })),
      nextCursor,
    };
  }
  async reconcile(walletId: string, correlationId: string) {
    z.uuid().parse(walletId);
    // One MVCC snapshot prevents a concurrent commit from manufacturing a false divergence.
    const [row] = await this.db.query(
      `SELECT w.balance::text AS stored,w.currency,
      coalesce(sum(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0)::numeric(28,2)::text AS calculated,
      count(l.id)::int AS checked FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id
      WHERE w.id=$1 GROUP BY w.id`,
      [walletId],
    );
    if (!row) throw new ApplicationError('WALLET_NOT_FOUND', 404);
    const stored = Money.rehydrate({
      amount: row.stored,
      currency: row.currency,
    });
    const calculated = Money.rehydrate({
      amount: row.calculated,
      currency: row.currency,
    });
    const difference = stored.subtract(calculated);
    const consistent = difference.isZero();
    if (!consistent) {
      this.telemetry.divergences.inc();
      this.telemetry.log('reconciliation_divergence', {
        correlationId,
        walletId,
        code: 'LEDGER_MISMATCH',
      });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: row.checked,
    };
  }
}
