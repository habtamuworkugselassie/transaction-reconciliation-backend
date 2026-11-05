-- ============================================================================
-- V1: core schema for transaction imports and reconciliation.
--
-- Design notes (see ARCHITECTURE.md "Database as the correctness model"):
--  * imports is both the upload record and the job row. Job ownership is a lease
--    (lease_owner + lease_expires_at); claiming uses SELECT ... FOR UPDATE SKIP
--    LOCKED so N workers never process the same import concurrently.
--  * last_line_committed is the processing checkpoint. It is updated in the SAME
--    transaction as the batch of transactions/rejections it covers, so a crash
--    between statements can never leave a half-recorded batch.
--  * transactions(provider_id, transaction_id) UNIQUE is the duplicate-detection
--    mechanism. It holds within a file, across files, across workers and across
--    restarts because it is enforced by the database, not by process memory.
-- ============================================================================

CREATE TABLE imports (
    id                  UUID         PRIMARY KEY,
    idempotency_key     VARCHAR(128) NOT NULL,
    provider_id         VARCHAR(64)  NOT NULL,
    status              VARCHAR(16)  NOT NULL,
    original_filename   VARCHAR(255),
    storage_key         VARCHAR(512),
    file_size_bytes     BIGINT,
    content_sha256      CHAR(64),

    -- progress counters; authoritative, updated per committed batch
    processed_count     BIGINT       NOT NULL DEFAULT 0,
    accepted_count      BIGINT       NOT NULL DEFAULT 0,
    rejected_count      BIGINT       NOT NULL DEFAULT 0,
    duplicate_count     BIGINT       NOT NULL DEFAULT 0,
    last_line_committed BIGINT       NOT NULL DEFAULT 0,

    -- job control
    cancel_requested    BOOLEAN      NOT NULL DEFAULT FALSE,
    attempt_count       INT          NOT NULL DEFAULT 0,
    lease_owner         VARCHAR(128),
    lease_expires_at    TIMESTAMPTZ,

    failure_reason      VARCHAR(64),
    failure_message     VARCHAR(512),

    created_at          TIMESTAMPTZ  NOT NULL,
    updated_at          TIMESTAMPTZ  NOT NULL,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,

    CONSTRAINT imports_status_check CHECK (status IN
        ('uploading', 'pending', 'processing', 'completed', 'failed', 'cancelling', 'cancelled')),
    CONSTRAINT imports_counts_check CHECK (
        processed_count >= 0 AND accepted_count >= 0 AND rejected_count >= 0 AND duplicate_count >= 0
        AND processed_count = accepted_count + rejected_count + duplicate_count),
    -- Idempotency: one import per client key. Insert races resolve on this constraint.
    CONSTRAINT imports_idempotency_key_unique UNIQUE (idempotency_key)
);

-- Workers scan only claimable rows; the partial index keeps that scan tiny
-- regardless of how many completed imports accumulate.
CREATE INDEX imports_claimable_idx
    ON imports (created_at)
    WHERE status IN ('pending', 'processing', 'cancelling');

-- Stale-upload reaper scans only rows still uploading.
CREATE INDEX imports_uploading_idx
    ON imports (created_at)
    WHERE status = 'uploading';

CREATE TABLE transactions (
    id                  BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id           UUID          NOT NULL REFERENCES imports (id),
    line_number         BIGINT        NOT NULL,
    provider_id         VARCHAR(64)   NOT NULL,
    transaction_id      VARCHAR(128)  NOT NULL,
    account_id          VARCHAR(128)  NOT NULL,
    merchant_id         VARCHAR(128)  NOT NULL,
    amount              NUMERIC(19,4) NOT NULL,
    currency            CHAR(3)       NOT NULL,
    occurred_at         TIMESTAMPTZ   NOT NULL,
    description         VARCHAR(500),
    fingerprint         CHAR(64)      NOT NULL,
    fingerprint_version SMALLINT      NOT NULL,
    risk_score          SMALLINT      NOT NULL,
    risk_level          VARCHAR(6)    NOT NULL,
    created_at          TIMESTAMPTZ   NOT NULL,

    CONSTRAINT transactions_amount_positive CHECK (amount > 0),
    CONSTRAINT transactions_risk_score_range CHECK (risk_score BETWEEN 0 AND 100),
    CONSTRAINT transactions_risk_level_check CHECK (risk_level IN ('low', 'medium', 'high')),
    -- Duplicate detection. "Same transactionId already accepted for the same provider."
    CONSTRAINT transactions_provider_transaction_unique UNIQUE (provider_id, transaction_id)
);

-- Summary queries (GROUP BY currency / risk_level / merchant / account) and
-- import deletion are all scoped by import_id. A single index serves them; the
-- aggregation itself is a hash aggregate over the matching rows.
CREATE INDEX transactions_import_id_idx ON transactions (import_id);

CREATE TABLE rejected_records (
    id          BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id   UUID         NOT NULL REFERENCES imports (id),
    line_number BIGINT       NOT NULL,
    reason      VARCHAR(64)  NOT NULL,
    message     VARCHAR(512) NOT NULL,
    -- Limited, sanitised excerpt of the offending input (never the full line).
    raw_value   JSONB,
    created_at  TIMESTAMPTZ  NOT NULL,

    -- Re-delivery of a batch must not double-record a rejection.
    CONSTRAINT rejected_records_import_line_unique UNIQUE (import_id, line_number)
);

-- Cursor pagination: ORDER BY id WHERE import_id = ? AND id > cursor.
CREATE INDEX rejected_records_import_cursor_idx ON rejected_records (import_id, id);

-- Materialised summary, written once when an import reaches a terminal state.
-- Until then the summary is derived live from transactions.
CREATE TABLE import_summaries (
    import_id   UUID        PRIMARY KEY REFERENCES imports (id),
    summary     JSONB       NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

-- Audit of every processing attempt: which worker, when, how it ended.
CREATE TABLE import_attempts (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    import_id       UUID         NOT NULL REFERENCES imports (id),
    attempt         INT          NOT NULL,
    worker_id       VARCHAR(128) NOT NULL,
    started_at      TIMESTAMPTZ  NOT NULL,
    ended_at        TIMESTAMPTZ,
    outcome         VARCHAR(16),
    failure_message VARCHAR(512),
    CONSTRAINT import_attempts_outcome_check CHECK (outcome IS NULL OR outcome IN
        ('completed', 'failed', 'cancelled', 'released', 'abandoned')),
    CONSTRAINT import_attempts_unique UNIQUE (import_id, attempt)
);
