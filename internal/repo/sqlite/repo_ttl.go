package sqlite

import (
	"context"
)

func (r *ttlRepo) GetExpiredItems(ctx context.Context, tableName string, ttlAttr string, before int64, limit int) ([]struct {
	PK string
	SK string
}, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk FROM items
		WHERE table_key = ? AND ttl > 0 AND ttl < ?
		ORDER BY ttl ASC, pk ASC, sk ASC
		LIMIT ?
	`, tableName, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expired []struct {
		PK string
		SK string
	}
	for rows.Next() {
		var pk, sk string
		if err := rows.Scan(&pk, &sk); err != nil {
			return nil, err
		}
		expired = append(expired, struct {
			PK string
			SK string
		}{PK: pk, SK: sk})
	}
	return expired, rows.Err()
}

func (r *ttlRepo) DeleteExpiredItem(ctx context.Context, tableName, pk, sk string) error {
	_, err := r.tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_key = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	if err != nil {
		return err
	}

	return nil
}

func (r *ttlRepo) DeleteExpiredItems(ctx context.Context, tableName string, before int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	expired, err := r.GetExpiredItems(ctx, tableName, "", before, limit)
	if err != nil {
		return 0, err
	}
	for _, key := range expired {
		if err := r.DeleteExpiredItem(ctx, tableName, key.PK, key.SK); err != nil {
			return 0, err
		}
	}
	return int64(len(expired)), nil
}
