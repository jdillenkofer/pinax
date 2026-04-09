CREATE TABLE IF NOT EXISTS backups (
    backup_arn TEXT PRIMARY KEY,
    backup_name TEXT NOT NULL UNIQUE,
    table_name TEXT NOT NULL,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    backup_status TEXT NOT NULL,
    backup_type TEXT NOT NULL,
    backup_creation_date_time INTEGER NOT NULL,
    backup_size_bytes INTEGER NOT NULL,
    source_table_details_json TEXT NOT NULL,
    source_table_feature_details_json TEXT NOT NULL,
    snapshot_table_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backups_table_name_created ON backups(table_name, backup_creation_date_time DESC, backup_arn DESC);

CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    item_json BLOB NOT NULL,
    PRIMARY KEY (backup_arn, ordinal),
    FOREIGN KEY (backup_arn) REFERENCES backups(backup_arn) ON DELETE CASCADE
);
