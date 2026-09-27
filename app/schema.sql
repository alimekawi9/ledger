-- Ledger schema. Applied idempotently at startup (see app/db.py).
--
-- Money is stored as BIGINT minor units (cents). Never floats: 0.1 + 0.2 != 0.3.
-- There is deliberately NO balance column anywhere. A balance is always
-- SUM(entries.amount) for an account, so it cannot drift from the history.

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
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Covering index: balance = SUM(amount) WHERE account_id = ? can be answered
-- from the index alone; (account_id, id) also serves paginated history.
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

-- Invariant 2: entries are append-only. Corrections are new entries
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
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'entries_append_only') THEN
        CREATE TRIGGER entries_append_only
            BEFORE UPDATE OR DELETE ON entries
            FOR EACH ROW EXECUTE FUNCTION ledger_reject_entry_mutation();
    END IF;
END;
$$;
