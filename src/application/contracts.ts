import { z } from 'zod';
import { Money } from '../domain/money';
import { DomainError } from '../domain/errors';

const id = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9:._-]*$/);
export const moneySchema = z.strictObject({
  amount: z.string(),
  currency: z.string(),
});
export const walletSchema = z.strictObject({
  playerId: z.uuid(),
  initialBalance: moneySchema,
});
export const transactionSchema = z.strictObject({
  providerId: id,
  externalTransactionId: id,
  playerId: z.uuid(),
  walletId: z.uuid(),
  roundId: id,
  gameId: id,
  kind: z.enum(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK']),
  money: moneySchema,
  referenceExternalTransactionId: id.optional(),
});
export type TransactionCommand = z.infer<typeof transactionSchema> & {
  idempotencyKey: string;
};
export const keySchema = z
  .string()
  .min(1)
  .max(256)
  .regex(/^[\x21-\x7E]+$/);
export const envelopeSchema = z.strictObject({
  messageId: id,
  type: z.literal('WagerTransactionRequested'),
  occurredAt: z.iso.datetime(),
  correlationId: id.optional(),
  data: transactionSchema.extend({ idempotencyKey: keySchema }),
});
export function validateCommand(
  input: unknown,
  key: unknown,
): TransactionCommand {
  const data = transactionSchema.parse(input);
  const idempotencyKey = keySchema.parse(key);
  Money.from(data.money);
  if (data.providerId === 'system') throw new DomainError('RESERVED_PROVIDER');
  if (data.kind === 'BET' && data.referenceExternalTransactionId)
    throw new DomainError('REFERENCE_NOT_ALLOWED');
  return { ...data, idempotencyKey };
}
