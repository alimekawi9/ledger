-- Ledger schema. Applied idempotently at startup (see app/db.py).
--
-- Money is stored as BIGINT minor units (cents). Never floats: 0.1 + 0.2 != 0.3.
-- There is deliberately NO mutable balance anywhere. Each entry records the
-- account's running balance *after* that entry (entries.balance_after),
-- written once by the database and never updated, so the current balance is
-- the newest entry's balance_after: O(1) to read, and still just history.

CREATE TABLE IF NOT EXISTS accounts (
    id          uuid PRIMARY KEY,
    name        text        NOT NULL,
    currency    char(3)     NOT NULL,
    -- 'external' accounts represent money outside the ledger (e.g. the bank's
    -- settlement account at the central bank). Deposits are transfers FROM the
    -- external account, so even funding is double-entry. External accounts are
    -- allowed to go negative; customer accounts are not.
    kind        text        NOT NULL DEFAULT 'customer'
                            CHECK (kind IN ('customer', 'external')),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS accounts_one_external_per_currency
    ON accounts (currency) WHERE kind = 'external';

CREATE TABLE IF NOT EXISTS transfers (
    id               uuid PRIMARY KEY,
    -- The idempotency key IS the deduplication mechanism: the UNIQUE index
    -- makes a second INSERT with the same key block until the first
    -- transaction finishes, then conflict. See app/service.py.
    idempotency_key  text        NOT NULL UNIQUE,
    -- sha256 of the request payload, so reusing a key for a *different*
    -- request is detected instead of silently replaying the wrong result.
    request_hash     text        NOT NULL,
    kind             text        NOT NULL CHECK (kind IN ('transfer', 'deposit')),
    from_account_id  uuid        NOT NULL REFERENCES accounts (id),
    to_account_id    uuid        NOT NULL REFERENCES accounts (id),
    amount           bigint      NOT NULL CHECK (amount > 0),
    currency         char(3)     NOT NULL,
    -- 'pending' exists only inside the transaction that creates the row; it
    -- is never visible after commit (the invariant tests assert this).
    status           text        NOT NULL
                                 CHECK (status IN ('pending', 'completed', 'rejected')),
    failure_reason   text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    CHECK (from_account_id <> to_account_id)
);

CREATE TABLE IF NOT EXISTS entries (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transfer_id  uuid        NOT NULL REFERENCES transfers (id),
    account_id   uuid        NOT NULL REFERENCES accounts (id),
    -- Signed: negative = debit (money leaves the account),
    --         positive = credit (money enters the account).
    amount       bigint      NOT NULL CHECK (amount <> 0),
    -- Running balance of account_id including this entry. Computed by the
    -- ledger_set_balance_after trigger below; any value the client supplies
    -- is overwritten. NULL for external accounts (see the trigger).
    balance_after bigint,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- (account_id, id) serves "newest entry for this account" (the current
-- balance, via a backward index scan) and paginated history.
CREATE INDEX IF NOT EXISTS entries_account_id_idx
    ON entries (account_id, id) INCLUDE (amount);
CREATE INDEX IF NOT EXISTS entries_transfer_id_idx ON entries (transfer_id);

-- Invariant 1: every transfer's entries sum to zero.
-- A DEFERRED constraint trigger runs at COMMIT, not per statement, so the
-- debit can be inserted before the credit inside one transaction, but a
-- transaction that inserted only one leg can never commit. This is enforced
-- by the database, independent of any application bug.
CREATE OR REPLACE FUNCTION ledger_check_transfer_balanced() RETURNS trigger AS $$
DECLARE
    total bigint;
BEGIN
    SELECT COALESCE(SUM(amount), 0) INTO total
      FROM entries WHERE transfer_id = NEW.transfer_id;
    IF total <> 0 THEN
        RAISE EXCEPTION 'transfer % is unbalanced: entries sum to %',
            NEW.transfer_id, total
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Invariant 2: running balances are computed by the database, serially per
-- account, and a customer balance can never go negative.
--
-- Why this is safe: the trigger locks the account row before reading the
-- previous balance_after. Every writer of a customer account's entries holds
-- that lock until COMMIT, so writers of one account form a queue, and each
-- one's "newest previous entry" is really the latest committed one. (The
-- service takes the same locks earlier, in sorted order, to avoid deadlocks;
-- here they are re-acquired as no-ops. Anyone writing SQL by hand gets the
-- same guarantee.)
--
-- The id is re-drawn AFTER the lock is held. Postgres fills the identity
-- column before BEFORE triggers run, i.e. before we waited for the lock, so
-- a writer that queued behind another would otherwise keep a *lower* id
-- than the entry it builds on, and "newest entry = highest id" would skip
-- it. Re-drawing makes id order match lock order for every account.
-- (Found by mutation testing: remove the service's own locks and the old
-- trigger forked the chain. tests/test_db_invariants.py covers it now.)
--
-- That argument needs each statement to see what was committed before it
-- ran, i.e. READ COMMITTED. Under REPEATABLE READ the snapshot predates the
-- lock wait and the chain would silently fork, so other levels are refused.
--
-- External accounts are not tracked: they may go negative (nothing to
-- protect) and every deposit touches one, so locking them would serialize
-- all deposits of a currency. Their balance is SUM(amount), read rarely.
CREATE OR REPLACE FUNCTION ledger_set_balance_after() RETURNS trigger AS $$
DECLARE
    acct_kind text;
    prev_acct uuid;
    prev bigint;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'ledger writes require READ COMMITTED (got %)',
            current_setting('transaction_isolation')
            USING ERRCODE = 'invalid_transaction_state';
    END IF;

    -- Kind first, unlocked (accounts never change kind). Locking before this
    -- check would lock external accounts too: every deposit would queue on
    -- one row, and two raw-SQL writers could deadlock on it.
    SELECT kind INTO acct_kind FROM accounts WHERE id = NEW.account_id;
    IF acct_kind IS DISTINCT FROM 'customer' THEN
        NEW.balance_after := NULL;
        RETURN NEW;
    END IF;
    PERFORM 1 FROM accounts WHERE id = NEW.account_id FOR NO KEY UPDATE;

    NEW.id := nextval(pg_get_serial_sequence('entries', 'id'));

    -- Newest entry of this account, phrased so only entries_account_id_idx
    -- can answer it (see _customer_balance in service.py for why).
    SELECT account_id, balance_after INTO prev_acct, prev FROM entries
     WHERE (account_id, id) <= (NEW.account_id, 9223372036854775807)
     ORDER BY account_id DESC, id DESC LIMIT 1;
    IF prev_acct IS DISTINCT FROM NEW.account_id THEN
        prev := 0;
    END IF;
    NEW.balance_after := prev + NEW.amount;
    IF NEW.balance_after < 0 THEN
        RAISE EXCEPTION 'account % would be overdrawn (balance %)',
            NEW.account_id, NEW.balance_after
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Invariant 3: entries are append-only. Corrections are new entries
-- (a reversing transfer), never edits. History is the source of truth.
CREATE OR REPLACE FUNCTION ledger_reject_entry_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'entries are append-only (% rejected)', TG_OP
        USING ERRCODE = 'insufficient_privilege';
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'entries_balanced') THEN
        CREATE CONSTRAINT TRIGGER entries_balanced
            AFTER INSERT ON entries
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION ledger_check_transfer_balanced();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'entries_balance_after') THEN
        CREATE TRIGGER entries_balance_after
            BEFORE INSERT ON entries
            FOR EACH ROW EXECUTE FUNCTION ledger_set_balance_after();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'entries_append_only') THEN
        CREATE TRIGGER entries_append_only
            BEFORE UPDATE OR DELETE ON entries
            FOR EACH ROW EXECUTE FUNCTION ledger_reject_entry_mutation();
    END IF;
END;
$$;

-- Migration for databases created before balance_after existed: add the
-- column and backfill it from history. The one sanctioned exception to
-- append-only, done once, with the guard trigger disabled only inside this
-- transaction's scope.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'entries' AND column_name = 'balance_after') THEN
        ALTER TABLE entries ADD COLUMN balance_after bigint;
        ALTER TABLE entries DISABLE TRIGGER entries_append_only;
        UPDATE entries e SET balance_after = r.running
          FROM (SELECT e2.id,
                       SUM(e2.amount) OVER (PARTITION BY e2.account_id ORDER BY e2.id) AS running
                  FROM entries e2 JOIN accounts a ON a.id = e2.account_id
                 WHERE a.kind = 'customer') r
         WHERE e.id = r.id;
        ALTER TABLE entries ENABLE TRIGGER entries_append_only;
    END IF;
END;
$$;
