DROP INDEX IF EXISTS idx_item_history_table_time_id;
DROP TABLE IF EXISTS item_history;

ALTER TABLE tables DROP COLUMN pitr_enabled_at;
ALTER TABLE tables DROP COLUMN pitr_recovery_period_days;
ALTER TABLE tables DROP COLUMN pitr_enabled;
