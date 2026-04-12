ALTER TABLE stream_records RENAME COLUMN table_key TO table_name;
ALTER TABLE pitr_checkpoints RENAME COLUMN table_key TO table_name;
ALTER TABLE item_history RENAME COLUMN table_key TO table_name;
ALTER TABLE backups RENAME COLUMN table_key TO table_name;
ALTER TABLE item_gsis RENAME COLUMN table_key TO table_name;
ALTER TABLE items RENAME COLUMN table_key TO table_name;
