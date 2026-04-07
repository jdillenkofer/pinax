CREATE TABLE IF NOT EXISTS item_gsis (
    table_name TEXT NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT NOT NULL,
    index_name TEXT NOT NULL,
    gsi_pk TEXT NOT NULL,
    gsi_sk TEXT NOT NULL,
    PRIMARY KEY (table_name, index_name, gsi_pk, gsi_sk, pk, sk),
    FOREIGN KEY (table_name, pk, sk) REFERENCES items(table_name, pk, sk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_item_gsis_lookup ON item_gsis(table_name, index_name, gsi_pk, gsi_sk);
