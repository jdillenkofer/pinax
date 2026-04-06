-- Add TTL columns to tables
ALTER TABLE tables ADD COLUMN ttl_enabled INTEGER DEFAULT 0;
ALTER TABLE tables ADD COLUMN ttl_attribute TEXT;
