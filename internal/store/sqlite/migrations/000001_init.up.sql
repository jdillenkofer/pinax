CREATE TABLE IF NOT EXISTS tables (
    name TEXT PRIMARY KEY,
    hash_key TEXT NOT NULL,
    hash_type TEXT NOT NULL,
    range_key TEXT,
    range_type TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    table_name TEXT NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT NOT NULL,
    item_json BLOB NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (table_name, pk, sk),
    FOREIGN KEY (table_name) REFERENCES tables(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_items_table_pk ON items(table_name, pk);
