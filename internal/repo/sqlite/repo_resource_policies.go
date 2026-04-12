package sqlite

import (
	"context"
	"database/sql"
	"errors"
)

func (r sqlTxRepo) PutResourcePolicy(ctx context.Context, resourceARN string, policy string, revisionID string, updatedAt int64) error {
	_, err := r.tx.ExecContext(ctx, `
		INSERT INTO resource_policies(resource_arn, policy_json, revision_id, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(resource_arn) DO UPDATE SET
			policy_json = excluded.policy_json,
			revision_id = excluded.revision_id,
			updated_at = excluded.updated_at
	`, resourceARN, policy, revisionID, updatedAt)
	return err
}

func (r sqlTxRepo) GetResourcePolicy(ctx context.Context, resourceARN string) (string, string, error) {
	var policy string
	var revisionID string
	err := r.tx.QueryRowContext(ctx, `SELECT policy_json, revision_id FROM resource_policies WHERE resource_arn = ?`, resourceARN).Scan(&policy, &revisionID)
	if err != nil {
		return "", "", err
	}
	return policy, revisionID, nil
}

func (r sqlTxRepo) DeleteResourcePolicy(ctx context.Context, resourceARN string) (string, bool, error) {
	_, revisionID, err := r.GetResourcePolicy(ctx, resourceARN)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	_, err = r.tx.ExecContext(ctx, `DELETE FROM resource_policies WHERE resource_arn = ?`, resourceARN)
	if err != nil {
		return "", false, err
	}
	return revisionID, true, nil
}
