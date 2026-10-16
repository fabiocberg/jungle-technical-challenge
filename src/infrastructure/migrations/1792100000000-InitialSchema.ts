import type { MigrationInterface, QueryRunner } from 'typeorm';

export class InitialSchema1792100000000 implements MigrationInterface {
  name = 'InitialSchema1792100000000';
  async up(runner: QueryRunner): Promise<void> {
    await runner.query(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY, player_id uuid NOT NULL, currency text NOT NULL CHECK (currency IN ('BRL','USD','EUR')),
        balance numeric(28,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
        version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
        created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (player_id, currency), UNIQUE (id, currency)
      );
      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY, provider_id text NOT NULL CHECK (length(provider_id) BETWEEN 1 AND 128),
        external_transaction_id text NOT NULL, idempotency_key text NOT NULL UNIQUE,
        payload_hash char(64) NOT NULL, wallet_id uuid NOT NULL REFERENCES wallets(id), player_id uuid NOT NULL,
        round_id text NOT NULL, game_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(28,2) NOT NULL CHECK (amount >= 0), currency text NOT NULL,
        status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        reference_external_transaction_id text, reference_transaction_id uuid REFERENCES wager_transactions(id),
        failure_code text, processed_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
        response jsonb, reference_attempts integer NOT NULL DEFAULT 0,
        next_reference_at timestamptz NOT NULL DEFAULT now(), correlation_id text NOT NULL, causation_id text,
        UNIQUE (provider_id, external_transaction_id), UNIQUE (id, wallet_id),
        CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),
        CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CHECK (reference_external_transaction_id IS DISTINCT FROM external_transaction_id),
        CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
        CHECK ((status IN ('PROCESSED','REJECTED','FAILED')) = (processed_at IS NOT NULL))
      );
      CREATE UNIQUE INDEX one_reversal_per_kind ON wager_transactions(reference_transaction_id, kind)
        WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
      CREATE INDEX pending_references_due ON wager_transactions(next_reference_at, id) WHERE status = 'PENDING_REFERENCE';
      CREATE TABLE wallet_ledger (
        id uuid PRIMARY KEY, sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
        wallet_id uuid NOT NULL, transaction_id uuid NOT NULL,
        direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount numeric(28,2) NOT NULL CHECK (amount > 0), currency text NOT NULL,
        balance_before numeric(28,2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(28,2) NOT NULL CHECK (balance_after >= 0), created_at timestamptz NOT NULL DEFAULT now(),
        UNIQUE (transaction_id, wallet_id),
        FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions(id, wallet_id),
        FOREIGN KEY (wallet_id, currency) REFERENCES wallets(id, currency),
        CHECK (balance_after = balance_before + CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
      );
      CREATE INDEX ledger_wallet_cursor ON wallet_ledger(wallet_id, sequence);
      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL, message_id text NOT NULL, payload_hash char(64) NOT NULL,
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
        received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz NOT NULL,
        PRIMARY KEY (consumer_name, message_id)
      );
      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL, attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
      );
      CREATE INDEX outbox_due ON outbox_messages(next_attempt_at, id) WHERE published_at IS NULL;
      CREATE INDEX outbox_transaction ON outbox_messages((payload->'data'->>'transactionId'));

      CREATE FUNCTION immutable_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'ledger is append-only' USING ERRCODE = '23514'; END $$;
      CREATE TRIGGER immutable_ledger BEFORE UPDATE OR DELETE ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION immutable_ledger();
      CREATE TRIGGER immutable_ledger_truncate BEFORE TRUNCATE ON wallet_ledger FOR EACH STATEMENT EXECUTE FUNCTION immutable_ledger();

      CREATE FUNCTION apply_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE w wallets; t wager_transactions;
      BEGIN
        SELECT * INTO w FROM wallets WHERE id = NEW.wallet_id FOR UPDATE;
        SELECT * INTO t FROM wager_transactions WHERE id = NEW.transaction_id;
        IF w.balance <> NEW.balance_before OR t.status <> 'PROCESSED' OR t.kind = 'LOSS'
           OR t.amount <> NEW.amount OR t.currency <> NEW.currency OR t.player_id <> w.player_id THEN
          RAISE EXCEPTION 'ledger does not match wallet/transaction' USING ERRCODE = '23514';
        END IF;
        IF (t.kind = 'BET' AND NEW.direction <> 'DEBIT') OR
           (t.kind IN ('OPENING','WIN','REFUND') AND NEW.direction <> 'CREDIT') THEN
          RAISE EXCEPTION 'invalid ledger direction' USING ERRCODE = '23514';
        END IF;
        IF t.kind = 'OPENING' AND EXISTS (SELECT 1 FROM wallet_ledger WHERE wallet_id = w.id) THEN
          RAISE EXCEPTION 'opening must be first' USING ERRCODE = '23514';
        END IF;
        UPDATE wallets SET balance = NEW.balance_after,
          version = version + CASE WHEN t.kind = 'OPENING' THEN 0 ELSE 1 END,
          updated_at = NEW.created_at WHERE id = NEW.wallet_id;
        RETURN NEW;
      END $$;
      CREATE TRIGGER apply_ledger BEFORE INSERT ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION apply_ledger();

      CREATE FUNCTION check_wallet_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE expected numeric; expected_version bigint; actual wallets;
      BEGIN
        SELECT * INTO actual FROM wallets WHERE id = NEW.id;
        SELECT coalesce(sum(CASE l.direction WHEN 'CREDIT' THEN l.amount ELSE -l.amount END),0),
          1 + count(*) FILTER (WHERE t.kind <> 'OPENING') INTO expected, expected_version
          FROM wallet_ledger l JOIN wager_transactions t ON t.id = l.transaction_id WHERE l.wallet_id = NEW.id;
        IF actual.balance <> expected OR actual.version <> expected_version THEN
          RAISE EXCEPTION 'wallet/ledger divergence' USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER wallet_consistency AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION check_wallet_consistency();

      CREATE FUNCTION protect_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'transactions are audit records' USING ERRCODE = '23514'; END IF;
        IF OLD.status IN ('PROCESSED','REJECTED','FAILED') OR
           (to_jsonb(NEW) - ARRAY['status','reference_transaction_id','failure_code','processed_at','response','reference_attempts','next_reference_at'])
             IS DISTINCT FROM
           (to_jsonb(OLD) - ARRAY['status','reference_transaction_id','failure_code','processed_at','response','reference_attempts','next_reference_at']) THEN
          RAISE EXCEPTION 'immutable transaction/state' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER protect_transaction BEFORE UPDATE OR DELETE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION protect_transaction();

      CREATE FUNCTION check_transaction_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
      DECLARE t wager_transactions; ref wager_transactions; entry wallet_ledger;
      BEGIN
        SELECT * INTO t FROM wager_transactions WHERE id = NEW.id;
        SELECT * INTO entry FROM wallet_ledger WHERE transaction_id = t.id;
        IF (t.status = 'PROCESSED' AND t.kind <> 'LOSS') <> (entry.id IS NOT NULL) THEN
          RAISE EXCEPTION 'transaction/ledger mismatch' USING ERRCODE = '23514';
        END IF;
        IF t.status = 'PROCESSED' AND t.reference_external_transaction_id IS NOT NULL THEN
          SELECT * INTO ref FROM wager_transactions WHERE id = t.reference_transaction_id;
          IF ref.id IS NULL OR ref.status <> 'PROCESSED' OR ref.provider_id <> t.provider_id
            OR ref.external_transaction_id <> t.reference_external_transaction_id OR ref.wallet_id <> t.wallet_id
            OR ref.player_id <> t.player_id OR ref.round_id <> t.round_id OR ref.currency <> t.currency
            OR (t.kind = 'ROLLBACK' AND ref.kind NOT IN ('BET','WIN','REFUND'))
            OR (t.kind <> 'ROLLBACK' AND ref.kind <> 'BET')
            OR (t.kind IN ('REFUND','ROLLBACK') AND ref.amount <> t.amount) THEN
            RAISE EXCEPTION 'invalid processed reference' USING ERRCODE = '23514';
          END IF;
          IF t.kind = 'ROLLBACK' AND entry.direction <> (CASE WHEN ref.kind = 'BET' THEN 'CREDIT' ELSE 'DEBIT' END) THEN
            RAISE EXCEPTION 'rollback direction mismatch' USING ERRCODE = '23514';
          END IF;
        END IF;
        IF t.status <> 'PENDING' AND NOT EXISTS (
          SELECT 1 FROM outbox_messages WHERE payload->'data'->>'transactionId' = t.id::text
            AND event_type = CASE t.status WHEN 'PROCESSED' THEN 'WagerTransactionProcessed'
              WHEN 'REJECTED' THEN 'WagerTransactionRejected' WHEN 'FAILED' THEN 'WagerTransactionFailed'
              ELSE 'WagerTransactionPendingReference' END) THEN
          RAISE EXCEPTION 'missing transaction outbox event' USING ERRCODE = '23514';
        END IF;
        IF entry.id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM outbox_messages
          WHERE event_type = 'WalletBalanceChanged' AND payload->'data'->>'transactionId' = t.id::text) THEN
          RAISE EXCEPTION 'missing balance outbox event' USING ERRCODE = '23514';
        END IF;
        RETURN NULL;
      END $$;
      CREATE CONSTRAINT TRIGGER transaction_consistency AFTER INSERT OR UPDATE ON wager_transactions
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_consistency();
    `);
  }
  async down(runner: QueryRunner): Promise<void> {
    await runner.query(`DROP TABLE inbox_messages, outbox_messages, wallet_ledger, wager_transactions, wallets CASCADE;
      DROP FUNCTION immutable_ledger(), apply_ledger(), check_wallet_consistency(), protect_transaction(), check_transaction_consistency();`);
  }
}
