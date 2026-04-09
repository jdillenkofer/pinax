CREATE INDEX IF NOT EXISTS idx_items_table_ttl ON items(table_name, ttl) WHERE ttl IS NOT NULL;
