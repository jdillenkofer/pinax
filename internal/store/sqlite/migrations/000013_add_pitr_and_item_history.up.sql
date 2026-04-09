ALTER TABLE tables ADD COLUMN pitr_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tables ADD COLUMN pitr_recovery_period_days INTEGER NOT NULL DEFAULT 35;
ALTER TABLE tables ADD COLUMN pitr_enabled_at INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS item_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT NOT NULL,
    change_type TEXT NOT NULL,
    item_json BLOB,
    changed_at INTEGER NOT NULL,
    FOREIGN KEY (table_name) REFERENCES tables(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_item_history_table_time_id ON item_history(table_name, changed_at ASC, id ASC);
