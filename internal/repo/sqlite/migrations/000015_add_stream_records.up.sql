CREATE TABLE IF NOT EXISTS stream_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    stream_arn TEXT NOT NULL,
    shard_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    keys_json BLOB NOT NULL,
    old_image_json BLOB,
    new_image_json BLOB,
    changed_at INTEGER NOT NULL,
    FOREIGN KEY (table_name) REFERENCES tables(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_stream_records_arn_id ON stream_records(stream_arn, id ASC);
