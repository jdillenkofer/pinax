CREATE TABLE IF NOT EXISTS resource_policies (
    resource_arn TEXT PRIMARY KEY,
    policy_json TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
