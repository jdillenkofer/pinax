DROP INDEX IF EXISTS idx_tx_write_idempotency_expires_at;
DROP TABLE IF EXISTS transact_write_idempotency;

ALTER TABLE tables DROP COLUMN tags_json;
ALTER TABLE tables DROP COLUMN sse_kms_key_id;
ALTER TABLE tables DROP COLUMN sse_status;
ALTER TABLE tables DROP COLUMN sse_type;
ALTER TABLE tables DROP COLUMN sse_enabled;
ALTER TABLE tables DROP COLUMN stream_label;
ALTER TABLE tables DROP COLUMN stream_arn;
ALTER TABLE tables DROP COLUMN stream_view_type;
ALTER TABLE tables DROP COLUMN stream_enabled;
ALTER TABLE tables DROP COLUMN deletion_protection_enabled;
ALTER TABLE tables DROP COLUMN table_class;
