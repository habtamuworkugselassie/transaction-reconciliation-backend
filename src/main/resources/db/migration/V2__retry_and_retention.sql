-- ============================================================================
-- Manual retry of failed imports and retention of uploaded files.
--
--  * retry_count: how many times a client re-queued this import with
--    POST /v1/imports/{id}/retry. Bounded by app.processing.max-manual-retries, so a
--    client cannot create unlimited work.
--  * attempt_base: attempt_count at the moment of the last retry. The automatic
--    attempt budget (app.processing.max-attempts) applies to attempt_count -
--    attempt_base, so a retried import gets a fresh budget while attempt numbers
--    keep increasing and import_attempts history is never rewritten.
--  * file_purged_at: set by the retention job when it deletes the uploaded file.
--    storage_key is kept (it is the record of what was uploaded); a purged import
--    can no longer be retried.
-- ============================================================================

ALTER TABLE imports
    ADD COLUMN retry_count    INT         NOT NULL DEFAULT 0,
    ADD COLUMN attempt_base   INT         NOT NULL DEFAULT 0,
    ADD COLUMN file_purged_at TIMESTAMPTZ;

ALTER TABLE imports
    ADD CONSTRAINT imports_retry_check CHECK (retry_count >= 0 AND attempt_base >= 0 AND attempt_base <= attempt_count);

-- Retention scans only terminal imports whose file is still on disk; once purged,
-- a row leaves this index for good.
CREATE INDEX imports_retention_idx
    ON imports (completed_at)
    WHERE status IN ('completed', 'failed', 'cancelled')
      AND storage_key IS NOT NULL
      AND file_purged_at IS NULL;
