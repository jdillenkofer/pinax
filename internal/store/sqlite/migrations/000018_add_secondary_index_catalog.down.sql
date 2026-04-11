ALTER TABLE tables ADD COLUMN gsi_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tables ADD COLUMN lsi_json TEXT NOT NULL DEFAULT '[]';

UPDATE tables
SET gsi_json = COALESCE((
    SELECT json_group_array(json_object(
        'IndexName', si.index_name,
        'HashKey', si.hash_key,
        'HashType', si.hash_type,
        'RangeKey', COALESCE(si.range_key, ''),
        'RangeType', COALESCE(si.range_type, ''),
        'Status', COALESCE(si.index_status, 'ACTIVE'),
        'StatusAt', COALESCE(si.index_status_at, 0),
        'ReadCapacity', COALESCE(si.read_capacity_units, 0),
        'WriteCapacity', COALESCE(si.write_capacity_units, 0),
        'ProjectionType', COALESCE(si.projection_type, 'ALL'),
        'NonKeyAttrs', COALESCE((
            SELECT json_group_array(nka.attr_name)
            FROM secondary_index_non_key_attrs nka
            WHERE nka.table_key = si.table_key AND nka.index_name = si.index_name
            ORDER BY nka.ordinal ASC
        ), '[]')
    ))
    FROM secondary_indexes si
    WHERE si.table_key = tables.name AND si.index_type = 'GSI'
), '[]');

UPDATE tables
SET lsi_json = COALESCE((
    SELECT json_group_array(json_object(
        'IndexName', si.index_name,
        'RangeKey', COALESCE(si.range_key, ''),
        'RangeType', COALESCE(si.range_type, ''),
        'ProjectionType', COALESCE(si.projection_type, 'ALL'),
        'NonKeyAttrs', COALESCE((
            SELECT json_group_array(nka.attr_name)
            FROM secondary_index_non_key_attrs nka
            WHERE nka.table_key = si.table_key AND nka.index_name = si.index_name
            ORDER BY nka.ordinal ASC
        ), '[]')
    ))
    FROM secondary_indexes si
    WHERE si.table_key = tables.name AND si.index_type = 'LSI'
), '[]');

CREATE TABLE IF NOT EXISTS item_gsis (
    table_key TEXT NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT NOT NULL,
    index_name TEXT NOT NULL,
    gsi_pk TEXT NOT NULL,
    gsi_sk TEXT NOT NULL,
    PRIMARY KEY (table_key, index_name, gsi_pk, gsi_sk, pk, sk),
    FOREIGN KEY (table_key, pk, sk) REFERENCES items(table_key, pk, sk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_item_gsis_lookup ON item_gsis(table_key, index_name, gsi_pk, gsi_sk);

INSERT OR IGNORE INTO item_gsis(table_key, pk, sk, index_name, gsi_pk, gsi_sk)
SELECT e.table_key, e.base_pk, e.base_sk, e.index_name, e.index_pk, e.index_sk
FROM secondary_index_entries e
JOIN secondary_indexes si
  ON si.table_key = e.table_key
 AND si.index_name = e.index_name
WHERE si.index_type = 'GSI';

DROP INDEX IF EXISTS idx_secondary_index_entries_base;
DROP INDEX IF EXISTS idx_secondary_index_entries_lookup;
DROP TABLE IF EXISTS secondary_index_entries;

DROP INDEX IF EXISTS idx_secondary_index_non_key_attrs_ordinal;
DROP TABLE IF EXISTS secondary_index_non_key_attrs;

DROP TABLE IF EXISTS secondary_indexes;
