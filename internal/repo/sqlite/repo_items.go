package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jdillenkofer/pinax/internal/model"
	"time"
)

func (r itemRepo) CountItems(ctx context.Context, tableName string) (int64, error) {
	var n int64
	err := r.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE table_key = ?`, tableName).Scan(&n)
	return n, err
}

func (r itemRepo) GetItem(ctx context.Context, tableName, pk, sk string) (map[string]any, error) {
	var raw []byte
	err := r.tx.QueryRowContext(ctx, `
		SELECT item_json FROM items
		WHERE table_key = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk).Scan(&raw)
	if err != nil {
		return nil, err
	}
	return decodeItem(raw)
}

func (r itemRepo) PutItem(ctx context.Context, t model.Table, pk, sk string, item map[string]any) error {
	ttl, hasTTL := model.ExtractTTL(t, item)
	var ttlVal any
	if hasTTL {
		ttlVal = ttl
	}

	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
		INSERT INTO items(table_key, pk, sk, item_json, updated_at, ttl)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(table_key, pk, sk)
		DO UPDATE SET item_json = excluded.item_json, updated_at = excluded.updated_at, ttl = excluded.ttl
	`, t.Name, pk, sk, raw, time.Now().Unix(), ttlVal)
	if err != nil {
		return err
	}

	_, err = r.tx.ExecContext(ctx, `DELETE FROM secondary_index_entries WHERE table_key = ? AND base_pk = ? AND base_sk = ?`, t.Name, pk, sk)
	if err != nil {
		return err
	}

	for _, gsi := range t.GSIs {
		gpk, gsk, ok := model.ExtractGSIKeys(gsi, item)
		if !ok {
			continue
		}
		_, err = r.tx.ExecContext(ctx, `
			INSERT INTO secondary_index_entries(table_key, index_name, index_pk, index_sk, base_pk, base_sk)
			VALUES (?, ?, ?, ?, ?, ?)
		`, t.Name, gsi.IndexName, gpk, gsk, pk, sk)
		if err != nil {
			return err
		}
	}
	for _, lsi := range t.LSIs {
		lsk, ok := model.ExtractLSISortKey(lsi, item)
		if !ok {
			continue
		}
		_, err = r.tx.ExecContext(ctx, `
			INSERT INTO secondary_index_entries(table_key, index_name, index_pk, index_sk, base_pk, base_sk)
			VALUES (?, ?, ?, ?, ?, ?)
		`, t.Name, lsi.IndexName, pk, lsk, pk, sk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r itemRepo) DeleteItem(ctx context.Context, tableName, pk, sk string) error {
	_, err := r.tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_key = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	if err != nil {
		return err
	}
	return nil
}

func (r itemRepo) QueryByPK(ctx context.Context, tableName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	order := "ASC"
	comp := ">"
	if !scanForward {
		order = "DESC"
		comp = "<"
	}
	q := fmt.Sprintf(`
		SELECT item_json FROM items
		WHERE table_key = ? AND pk = ? AND sk %s ?
		ORDER BY sk %s
	`, comp, order)
	args := []any{tableName, pk, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := r.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r itemRepo) QueryByGSI(ctx context.Context, tableName, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	order := "ASC"
	comp := ">"
	if !scanForward {
		order = "DESC"
		comp = "<"
	}
	q := fmt.Sprintf(`
		SELECT i.item_json FROM secondary_index_entries e
		JOIN items i ON e.table_key = i.table_key AND e.base_pk = i.pk AND e.base_sk = i.sk
		WHERE e.table_key = ? AND e.index_name = ? AND e.index_pk = ? AND e.index_sk %s ?
		ORDER BY e.index_sk %s
	`, comp, order)
	args := []any{tableName, indexName, pk, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := r.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r itemRepo) QueryByPKSK(ctx context.Context, tableName, pk, sk string) ([]map[string]any, error) {
	item, err := r.GetItem(ctx, tableName, pk, sk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return []map[string]any{item}, nil
}

func (r itemRepo) Scan(ctx context.Context, tableName, startPK, startSK string, limit int) ([]map[string]any, error) {
	q := `
		SELECT item_json FROM items
		WHERE table_key = ? AND (pk > ? OR (pk = ? AND sk > ?))
		ORDER BY pk ASC, sk ASC
	`
	args := []any{tableName, startPK, startPK, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := r.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r itemRepo) GetTransactWriteIdempotency(ctx context.Context, token string, now int64) (model.TransactWriteIdempotencyRecord, error) {
	var rec model.TransactWriteIdempotencyRecord
	var responseJSON []byte
	err := r.tx.QueryRowContext(ctx, `
		SELECT token, request_hash, response_json, created_at, expires_at
		FROM transact_write_idempotency
		WHERE token = ? AND expires_at > ?
	`, token, now).Scan(&rec.Token, &rec.RequestHash, &responseJSON, &rec.CreatedAt, &rec.ExpiresAt)
	if err != nil {
		return model.TransactWriteIdempotencyRecord{}, err
	}
	if err := json.Unmarshal(responseJSON, &rec.Response); err != nil {
		return model.TransactWriteIdempotencyRecord{}, err
	}
	return rec, nil
}

func (r itemRepo) PutTransactWriteIdempotency(ctx context.Context, record model.TransactWriteIdempotencyRecord) error {
	responseJSON, err := json.Marshal(record.Response)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
		INSERT INTO transact_write_idempotency(token, request_hash, response_json, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(token) DO UPDATE SET request_hash = excluded.request_hash, response_json = excluded.response_json, created_at = excluded.created_at, expires_at = excluded.expires_at
	`, record.Token, record.RequestHash, responseJSON, record.CreatedAt, record.ExpiresAt)
	return err
}

func (r itemRepo) DeleteExpiredTransactWriteIdempotency(ctx context.Context, now int64) error {
	_, err := r.tx.ExecContext(ctx, `DELETE FROM transact_write_idempotency WHERE expires_at <= ?`, now)
	return err
}
