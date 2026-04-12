PRAGMA foreign_keys = OFF;

CREATE TABLE tables_old (
    name TEXT PRIMARY KEY,
    hash_key TEXT NOT NULL,
    hash_type TEXT NOT NULL,
    range_key TEXT,
    range_type TEXT,
    created_at INTEGER NOT NULL
);

INSERT INTO tables_old(name, hash_key, hash_type, range_key, range_type, created_at)
SELECT name, hash_key, hash_type, range_key, range_type, created_at
FROM tables;

DROP TABLE tables;
ALTER TABLE tables_old RENAME TO tables;

PRAGMA foreign_keys = ON;
