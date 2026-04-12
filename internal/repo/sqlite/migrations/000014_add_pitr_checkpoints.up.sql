CREATE TABLE IF NOT EXISTS pitr_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    changed_at INTEGER NOT NULL,
    history_sequence INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (table_name) REFERENCES tables(name) ON DELETE CASCADE,
    UNIQUE(table_name, history_sequence)
);

CREATE TABLE IF NOT EXISTS pitr_checkpoint_items (
    checkpoint_id INTEGER NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT NOT NULL,
    item_json BLOB NOT NULL,
    PRIMARY KEY (checkpoint_id, pk, sk),
    FOREIGN KEY (checkpoint_id) REFERENCES pitr_checkpoints(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pitr_checkpoints_table_time_seq
    ON pitr_checkpoints(table_name, changed_at DESC, history_sequence DESC);
