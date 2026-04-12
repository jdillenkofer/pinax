-- Remove TTL columns from tables
ALTER TABLE tables DROP COLUMN ttl_enabled;
ALTER TABLE tables DROP COLUMN ttl_attribute;
