import { describe, expect, test } from 'bun:test';
import { Money } from '../../src/domain/money';
import { Wallet } from '../../src/domain/wallet';
import {
  WagerTransaction,
  type Kind,
  type TransactionState,
} from '../../src/domain/transaction';
import { WalletLedgerEntry } from '../../src/domain/ledger';
import {
  assertReplay,
  canonicalJson,
  payloadHash,
} from '../../src/domain/idempotency';
import { InboxMessage, OutboxMessage } from '../../src/domain/messages';
import {
  WalletBalanceChanged,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WagerTransactionPendingReference,
  WagerTransactionFailed,
} from '../../src/domain/events';
import { validateCommand } from '../../src/application/contracts';

const money = (amount: string, currency = 'BRL') =>
  Money.from({ amount, currency });
const base = {
  id: 'tx',
  walletId: 'wallet',
  playerId: 'player',
  providerId: 'provider',
  externalTransactionId: 'external',
  idempotencyKey: 'key',
  payloadHash: 'hash',
  roundId: 'round',
  gameId: 'game',
};
function transaction(
  kind: Kind = 'BET',
  overrides: Partial<TransactionState> = {},
) {
  return WagerTransaction.create({
    ...base,
    kind,
    money: money(kind === 'LOSS' ? '0.00' : '25.00'),
    ...(['REFUND', 'ROLLBACK'].includes(kind)
      ? { referenceExternalTransactionId: 'bet' }
      : {}),
    ...overrides,
  });
}
function reference(
  kind: Kind = 'BET',
  overrides: Partial<TransactionState> = {},
) {
  const tx = transaction(kind, { externalTransactionId: 'bet', ...overrides });
  tx.markProcessed(undefined, new Date());
  return tx;
}

describe('Money: exact fixed scale, no rounding and immutable arithmetic', () => {
  test('cent arithmetic and values beyond Number.MAX_SAFE_INTEGER remain exact', () => {
    const first = money('9007199254740993.01');
    expect(first.add(money('0.09')).toString()).toBe('9007199254740993.10');
    expect(first.toString()).toBe('9007199254740993.01');
    expect(money('0.10').add(money('0.20')).toJSON()).toEqual({
      amount: '0.30',
      currency: 'BRL',
    });
    expect(money('1.00').subtract(money('2.00')).toString()).toBe('-1.00');
    expect(money('1.00').negate().isNegative()).toBe(true);
    expect(Money.zero('BRL').isZero()).toBe(true);
    expect(money('1.00').isPositive()).toBe(true);
    expect(money('1.00').isLessThan(money('2.00'))).toBe(true);
    expect(money('1.00').equals(money('1.00'))).toBe(true);
    expect(Object.isFrozen(first)).toBe(true);
  });
  test.each([
    'NaN',
    'Infinity',
    '1e2',
    '',
    '1.001',
    '-1.00',
    '-0.00',
    '1',
    '1.0',
    '01.00',
    ' 1.00',
    '+1.00',
    '1,00',
    '0x10',
    '100000000000000000000000000.00',
  ])('rejects invalid amount %j', (amount) => {
    expect(() => money(amount)).toThrow();
  });
  test('runtime numeric input, unsupported currency, overflow and mixed currency are errors', () => {
    expect(() =>
      Money.from({ amount: 1 as unknown as string, currency: 'BRL' }),
    ).toThrow();
    expect(() => money('1.00', 'brl')).toThrow();
    expect(() =>
      money('99999999999999999999999999.99').add(money('0.01')),
    ).toThrow('AMOUNT_OVERFLOW');
    for (const method of ['add', 'subtract', 'equals', 'isLessThan'] as const)
      expect(() => money('1.00')[method](money('1.00', 'USD'))).toThrow(
        'CURRENCY_MISMATCH',
      );
    expect(
      Money.rehydrate({ amount: '-0.01', currency: 'BRL' }).toString(),
    ).toBe('-0.01');
  });
});
describe('Wallet and immutable balanced ledger', () => {
  test('moves balance and version together, rejected movement leaves state untouched', () => {
    const wallet = Wallet.open({
      id: 'wallet',
      playerId: 'player',
      initialBalance: money('100.00'),
    });
    expect(wallet.version).toBe(1);
    const debit = wallet.debit('tx', money('80.00'));
    expect(wallet.balance.toString()).toBe('20.00');
    expect(wallet.version).toBe(2);
    expect(debit.isBalanced()).toBe(true);
    expect(() => wallet.debit('other', money('80.00'))).toThrow(
      'INSUFFICIENT_FUNDS',
    );
    expect(() => wallet.credit('other', money('0.00'))).toThrow();
    expect(() => wallet.debit('other', money('1.00', 'USD'))).toThrow(
      'CURRENCY_MISMATCH',
    );
    expect(wallet.version).toBe(2);
    expect(wallet.balance.toString()).toBe('20.00');
    const credit = wallet.credit('credit', money('1.00'));
    expect(wallet.version).toBe(3);
    expect(credit.balanceAfter.toString()).toBe('21.00');
    expect(Object.isFrozen(debit)).toBe(true);
    const date = debit.createdAt;
    date.setFullYear(1990);
    expect(debit.createdAt.getFullYear()).not.toBe(1990);
  });
  test('factory rejects inconsistent ledger and negative wallet, rehydration preserves version', () => {
    expect(() =>
      Wallet.open({
        id: 'w',
        playerId: 'p',
        initialBalance: money('1.00').negate(),
      }),
    ).toThrow();
    expect(() =>
      WalletLedgerEntry.create({
        id: 'l',
        walletId: 'w',
        transactionId: 't',
        direction: 'CREDIT',
        money: money('1.00'),
        balanceBefore: money('1.00'),
        balanceAfter: money('3.00'),
        createdAt: new Date(),
      }),
    ).toThrow('UNBALANCED_LEDGER_ENTRY');
    const state = {
      id: 'w',
      playerId: 'p',
      currency: 'BRL',
      balance: money('2.00'),
      version: 8,
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    expect(Wallet.rehydrate(state).version).toBe(8);
  });
});
describe('Transactions: operation rules and lifecycle', () => {
  test.each(['BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK'] as const)(
    '%s has explicit movement semantics',
    (kind) => {
      const tx = transaction(kind);
      expect(tx.isTerminal()).toBe(false);
      expect(tx.affectsBalance()).toBe(kind !== 'LOSS');
      if (kind === 'LOSS')
        expect(() => tx.ledgerDirectionFor()).toThrow('NO_LEDGER_FOR_LOSS');
      else
        expect(tx.ledgerDirectionFor(reference())).toBe(
          kind === 'BET' ? 'DEBIT' : 'CREDIT',
        );
    },
  );
  test('rollback inverts credits; WIN can refer to a BET without equal amounts', () => {
    expect(transaction('ROLLBACK').ledgerDirectionFor(reference('WIN'))).toBe(
      'DEBIT',
    );
    const refund = reference('REFUND', { externalTransactionId: 'refund' });
    expect(transaction('ROLLBACK').ledgerDirectionFor(refund)).toBe('DEBIT');
    expect(
      transaction('WIN', { money: money('100.00') }).validateReference(
        reference(),
      ),
    ).toBeUndefined();
    expect(() => transaction('ROLLBACK').ledgerDirectionFor()).toThrow();
  });
  test('requires full amount and same identity on processed valid reference', () => {
    const tx = transaction('REFUND');
    expect(tx.validateReference(reference())).toBeUndefined();
    for (const props of [
      { providerId: 'other' },
      { walletId: 'other' },
      { playerId: 'other' },
      { roundId: 'other' },
      { money: money('25.00', 'USD') },
    ]) {
      expect(tx.validateReference(reference('BET', props))).toBe(
        'REFERENCE_MISMATCH',
      );
    }
    expect(tx.validateReference(transaction())).toBe('REFERENCE_NOT_PROCESSED');
    expect(tx.validateReference(reference('WIN'))).toBe(
      'REFERENCE_KIND_INVALID',
    );
    expect(
      tx.validateReference(reference('BET', { money: money('20.00') })),
    ).toBe('REFERENCE_AMOUNT_MISMATCH');
    expect(() =>
      transaction('REFUND', { referenceExternalTransactionId: undefined }),
    ).toThrow('REFERENCE_REQUIRED');
    expect(() =>
      transaction('BET', { referenceExternalTransactionId: 'external' }),
    ).toThrow('SELF_REFERENCE');
    expect(() => transaction('BET', { money: money('0.00') })).toThrow();
    expect(() => transaction('LOSS', { money: money('1.00') })).toThrow();
  });
  test.each(['PROCESSED', 'REJECTED', 'FAILED'] as const)(
    '%s is terminal for every transition',
    (status) => {
      const tx = transaction();
      if (status === 'PROCESSED') tx.markProcessed(undefined, new Date());
      else if (status === 'REJECTED') tx.reject('INSUFFICIENT_FUNDS');
      else tx.fail('PERMANENT_INFRASTRUCTURE_ERROR');
      expect(tx.isTerminal()).toBe(true);
      for (const transition of [
        () => tx.markProcessed(undefined, new Date()),
        () => tx.markPendingReference(),
        () => tx.reject('INSUFFICIENT_FUNDS'),
        () => tx.fail('PERMANENT_INFRASTRUCTURE_ERROR'),
      ])
        expect(transition).toThrow('INVALID_TRANSACTION_STATE');
    },
  );
  test('pending can be resumed, rehydrate does not repeat transition validation', () => {
    const tx = transaction('REFUND');
    tx.markPendingReference();
    expect(tx.status).toBe('PENDING_REFERENCE');
    tx.markProcessed('ref', new Date());
    expect(tx.referenceTransactionId).toBe('ref');
    expect(tx.matchesPayload('hash')).toBe(true);
    expect(
      WagerTransaction.rehydrate({
        ...base,
        kind: 'REFUND',
        money: money('25.00'),
        status: 'PROCESSED',
        createdAt: new Date(),
      }).status,
    ).toBe('PROCESSED');
  });
});
describe('Persistent-message domain and canonical idempotency', () => {
  test('canonical hash sorts recursively, preserves values and detects changed business payload', () => {
    expect(canonicalJson({ z: 1, a: { y: '2', x: '1' } })).toBe(
      '{"a":{"x":"1","y":"2"},"z":1}',
    );
    const hash = payloadHash({ b: '2', a: '1' });
    expect(hash).toBe(payloadHash({ a: '1', b: '2' }));
    expect(() =>
      assertReplay(hash, payloadHash({ a: 'different', b: '2' })),
    ).toThrow('IDEMPOTENCY_CONFLICT');
    expect(() => assertReplay(hash, hash)).not.toThrow();
  });
  test('header is required, body key and OPENING are not public commands', () => {
    const command = {
      ...base,
      playerId: crypto.randomUUID(),
      walletId: crypto.randomUUID(),
      kind: 'BET',
      money: money('1.00').toJSON(),
    };
    const { id: _, idempotencyKey: __, payloadHash: ___, ...body } = command;
    expect(() => validateCommand(body, undefined)).toThrow();
    expect(() =>
      validateCommand({ ...body, idempotencyKey: 'body' }, 'header'),
    ).toThrow();
    expect(() =>
      validateCommand({ ...body, kind: 'OPENING' }, 'key'),
    ).toThrow();
    expect(validateCommand(body, 'header').idempotencyKey).toBe('header');
  });
  test('inbox cannot be processed twice; outbox retries exponentially and serializes concrete events', () => {
    const inbox = InboxMessage.receive({
      messageId: 'm',
      consumerName: 'c',
      payloadHash: 'h',
    });
    expect(inbox.isProcessed()).toBe(false);
    inbox.markProcessed(new Date());
    expect(() => inbox.markProcessed(new Date())).toThrow();
    const tx = transaction('LOSS');
    tx.markProcessed(undefined, new Date());
    const event = WagerTransactionProcessed.from(tx, {
      correlationId: 'c',
      causationId: 'm',
    });
    const outbox = OutboxMessage.enqueue(event);
    const now = new Date('2026-01-01T00:00:00Z');
    expect(outbox.isDue(now)).toBe(true);
    outbox.scheduleRetry(now);
    expect(outbox.attempts).toBe(1);
    expect(outbox.nextAttemptAt?.getTime()).toBe(now.getTime() + 1000);
    outbox.scheduleRetry(now);
    expect(outbox.nextAttemptAt?.getTime()).toBe(now.getTime() + 2000);
    expect(outbox.isDue(now)).toBe(false);
    outbox.markPublished(now);
    expect(outbox.isPending()).toBe(false);
    expect(JSON.parse(JSON.stringify(event)).version).toBe(1);
    const wallet = Wallet.open({
      id: 'w',
      playerId: 'p',
      initialBalance: money('0.00'),
    });
    const balanceEvent = WalletBalanceChanged.from(
      wallet,
      wallet.credit('tx', money('1.00')),
      { correlationId: 'c' },
    );
    expect(balanceEvent.data.money).toEqual({
      amount: '1.00',
      currency: 'BRL',
    });
    for (const factory of [
      WagerTransactionRejected,
      WagerTransactionPendingReference,
      WagerTransactionFailed,
    ])
      expect(factory.from(transaction(), { correlationId: 'c' }).version).toBe(
        1,
      );
  });
});
