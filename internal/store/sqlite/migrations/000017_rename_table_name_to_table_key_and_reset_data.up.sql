ALTER TABLE items RENAME COLUMN table_name TO table_key;
ALTER TABLE item_gsis RENAME COLUMN table_name TO table_key;
ALTER TABLE backups RENAME COLUMN table_name TO table_key;
ALTER TABLE item_history RENAME COLUMN table_name TO table_key;
ALTER TABLE pitr_checkpoints RENAME COLUMN table_name TO table_key;
ALTER TABLE stream_records RENAME COLUMN table_name TO table_key;
