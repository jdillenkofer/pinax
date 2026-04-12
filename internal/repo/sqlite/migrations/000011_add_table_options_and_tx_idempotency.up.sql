ALTER TABLE tables ADD COLUMN table_class TEXT NOT NULL DEFAULT 'STANDARD';
ALTER TABLE tables ADD COLUMN deletion_protection_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tables ADD COLUMN stream_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tables ADD COLUMN stream_view_type TEXT;
ALTER TABLE tables ADD COLUMN stream_arn TEXT;
ALTER TABLE tables ADD COLUMN stream_label TEXT;
ALTER TABLE tables ADD COLUMN sse_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tables ADD COLUMN sse_type TEXT;
ALTER TABLE tables ADD COLUMN sse_status TEXT NOT NULL DEFAULT 'DISABLED';
ALTER TABLE tables ADD COLUMN sse_kms_key_id TEXT;
ALTER TABLE tables ADD COLUMN tags_json TEXT NOT NULL DEFAULT '[]';

CREATE TABLE IF NOT EXISTS transact_write_idempotency (
    token TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    response_json BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tx_write_idempotency_expires_at ON transact_write_idempotency(expires_at);
