CREATE TABLE IF NOT EXISTS secondary_indexes (
    table_key TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_type TEXT NOT NULL,
    hash_key TEXT,
    hash_type TEXT,
    range_key TEXT,
    range_type TEXT,
    index_status TEXT NOT NULL DEFAULT 'ACTIVE',
    index_status_at INTEGER NOT NULL DEFAULT 0,
    read_capacity_units INTEGER NOT NULL DEFAULT 0,
    write_capacity_units INTEGER NOT NULL DEFAULT 0,
    projection_type TEXT NOT NULL DEFAULT 'ALL',
    PRIMARY KEY (table_key, index_name),
    FOREIGN KEY (table_key) REFERENCES tables(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS secondary_index_non_key_attrs (
    table_key TEXT NOT NULL,
    index_name TEXT NOT NULL,
    attr_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (table_key, index_name, attr_name),
    FOREIGN KEY (table_key, index_name) REFERENCES secondary_indexes(table_key, index_name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_secondary_index_non_key_attrs_ordinal
    ON secondary_index_non_key_attrs(table_key, index_name, ordinal);

CREATE TABLE IF NOT EXISTS secondary_index_entries (
    table_key TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_pk TEXT NOT NULL,
    index_sk TEXT NOT NULL,
    base_pk TEXT NOT NULL,
    base_sk TEXT NOT NULL,
    PRIMARY KEY (table_key, index_name, index_pk, index_sk, base_pk, base_sk),
    FOREIGN KEY (table_key, base_pk, base_sk) REFERENCES items(table_key, pk, sk) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_secondary_index_entries_lookup
    ON secondary_index_entries(table_key, index_name, index_pk, index_sk);

CREATE INDEX IF NOT EXISTS idx_secondary_index_entries_base
    ON secondary_index_entries(table_key, index_name, base_pk, base_sk);

INSERT OR REPLACE INTO secondary_indexes (
    table_key,
    index_name,
    index_type,
    hash_key,
    hash_type,
    range_key,
    range_type,
    index_status,
    index_status_at,
    read_capacity_units,
    write_capacity_units,
    projection_type
)
SELECT
    t.name,
    json_extract(g.value, '$.IndexName'),
    'GSI',
    json_extract(g.value, '$.HashKey'),
    json_extract(g.value, '$.HashType'),
    NULLIF(json_extract(g.value, '$.RangeKey'), ''),
    NULLIF(json_extract(g.value, '$.RangeType'), ''),
    COALESCE(NULLIF(json_extract(g.value, '$.Status'), ''), 'ACTIVE'),
    COALESCE(json_extract(g.value, '$.StatusAt'), 0),
    COALESCE(json_extract(g.value, '$.ReadCapacity'), 0),
    COALESCE(json_extract(g.value, '$.WriteCapacity'), 0),
    COALESCE(NULLIF(json_extract(g.value, '$.ProjectionType'), ''), 'ALL')
FROM tables t
JOIN json_each(t.gsi_json) g
WHERE NULLIF(TRIM(json_extract(g.value, '$.IndexName')), '') IS NOT NULL
  AND NULLIF(TRIM(json_extract(g.value, '$.HashKey')), '') IS NOT NULL;

INSERT OR REPLACE INTO secondary_indexes (
    table_key,
    index_name,
    index_type,
    hash_key,
    hash_type,
    range_key,
    range_type,
    index_status,
    index_status_at,
    read_capacity_units,
    write_capacity_units,
    projection_type
)
SELECT
    t.name,
    json_extract(l.value, '$.IndexName'),
    'LSI',
    t.hash_key,
    t.hash_type,
    NULLIF(json_extract(l.value, '$.RangeKey'), ''),
    NULLIF(json_extract(l.value, '$.RangeType'), ''),
    'ACTIVE',
    0,
    0,
    0,
    COALESCE(NULLIF(json_extract(l.value, '$.ProjectionType'), ''), 'ALL')
FROM tables t
JOIN json_each(t.lsi_json) l
WHERE NULLIF(TRIM(json_extract(l.value, '$.IndexName')), '') IS NOT NULL
  AND NULLIF(TRIM(json_extract(l.value, '$.RangeKey')), '') IS NOT NULL;

INSERT OR IGNORE INTO secondary_index_non_key_attrs(table_key, index_name, attr_name, ordinal)
SELECT
    t.name,
    json_extract(g.value, '$.IndexName'),
    attr.value,
    attr.key
FROM tables t
JOIN json_each(t.gsi_json) g
JOIN json_each(COALESCE(json_extract(g.value, '$.NonKeyAttrs'), '[]')) attr
WHERE COALESCE(NULLIF(json_extract(g.value, '$.ProjectionType'), ''), 'ALL') = 'INCLUDE'
  AND NULLIF(TRIM(attr.value), '') IS NOT NULL;

INSERT OR IGNORE INTO secondary_index_non_key_attrs(table_key, index_name, attr_name, ordinal)
SELECT
    t.name,
    json_extract(l.value, '$.IndexName'),
    attr.value,
    attr.key
FROM tables t
JOIN json_each(t.lsi_json) l
JOIN json_each(COALESCE(json_extract(l.value, '$.NonKeyAttrs'), '[]')) attr
WHERE COALESCE(NULLIF(json_extract(l.value, '$.ProjectionType'), ''), 'ALL') = 'INCLUDE'
  AND NULLIF(TRIM(attr.value), '') IS NOT NULL;

INSERT OR IGNORE INTO secondary_index_entries(table_key, index_name, index_pk, index_sk, base_pk, base_sk)
SELECT
    i.table_key,
    si.index_name,
    CASE
        WHEN json_type(i.item_json, '$."' || si.hash_key || '".S') IS NOT NULL THEN 'S|' || json_extract(i.item_json, '$."' || si.hash_key || '".S')
        WHEN json_type(i.item_json, '$."' || si.hash_key || '".N') IS NOT NULL THEN 'N|' || json_extract(i.item_json, '$."' || si.hash_key || '".N')
        WHEN json_type(i.item_json, '$."' || si.hash_key || '".B') IS NOT NULL THEN 'B|' || json_extract(i.item_json, '$."' || si.hash_key || '".B')
    END AS index_pk,
    CASE
        WHEN COALESCE(si.range_key, '') = '' THEN '__PINAX_NO_SORT_KEY__'
        WHEN json_type(i.item_json, '$."' || si.range_key || '".S') IS NOT NULL THEN 'S|' || json_extract(i.item_json, '$."' || si.range_key || '".S')
        WHEN json_type(i.item_json, '$."' || si.range_key || '".N') IS NOT NULL THEN 'N|' || json_extract(i.item_json, '$."' || si.range_key || '".N')
        WHEN json_type(i.item_json, '$."' || si.range_key || '".B') IS NOT NULL THEN 'B|' || json_extract(i.item_json, '$."' || si.range_key || '".B')
    END AS index_sk,
    i.pk,
    i.sk
FROM items i
JOIN secondary_indexes si
  ON si.table_key = i.table_key
 AND si.index_type = 'GSI'
WHERE (
    json_type(i.item_json, '$."' || si.hash_key || '".S') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.hash_key || '".N') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.hash_key || '".B') IS NOT NULL
)
AND (
    COALESCE(si.range_key, '') = ''
    OR json_type(i.item_json, '$."' || si.range_key || '".S') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.range_key || '".N') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.range_key || '".B') IS NOT NULL
);

INSERT OR IGNORE INTO secondary_index_entries(table_key, index_name, index_pk, index_sk, base_pk, base_sk)
SELECT
    i.table_key,
    si.index_name,
    i.pk,
    CASE
        WHEN json_type(i.item_json, '$."' || si.range_key || '".S') IS NOT NULL THEN 'S|' || json_extract(i.item_json, '$."' || si.range_key || '".S')
        WHEN json_type(i.item_json, '$."' || si.range_key || '".N') IS NOT NULL THEN 'N|' || json_extract(i.item_json, '$."' || si.range_key || '".N')
        WHEN json_type(i.item_json, '$."' || si.range_key || '".B') IS NOT NULL THEN 'B|' || json_extract(i.item_json, '$."' || si.range_key || '".B')
    END AS index_sk,
    i.pk,
    i.sk
FROM items i
JOIN secondary_indexes si
  ON si.table_key = i.table_key
 AND si.index_type = 'LSI'
WHERE (
    json_type(i.item_json, '$."' || si.range_key || '".S') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.range_key || '".N') IS NOT NULL
    OR json_type(i.item_json, '$."' || si.range_key || '".B') IS NOT NULL
);

DROP INDEX IF EXISTS idx_item_gsis_lookup;
DROP TABLE IF EXISTS item_gsis;

ALTER TABLE tables DROP COLUMN gsi_json;
ALTER TABLE tables DROP COLUMN lsi_json;
