package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/jdillenkofer/pinax/internal/model"
	"sort"
	"strings"
	"time"
)

func (r pitrRepo) ListItemChangesUpTo(ctx context.Context, tableName string, upTo int64) ([]model.ItemChange, error) {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at ASC, id ASC
	`, tableName, upTo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r pitrRepo) ResolveItemChangeCursorAtOrBefore(ctx context.Context, tableName string, upTo int64) (model.ItemChangeCursor, error) {
	var cursor model.ItemChangeCursor
	err := r.tx.QueryRowContext(ctx, `
		SELECT changed_at, id
		FROM item_history
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at DESC, id DESC
		LIMIT 1
	`, tableName, upTo).Scan(&cursor.ChangedAt, &cursor.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.ItemChangeCursor{}, nil
		}
		return model.ItemChangeCursor{}, err
	}
	cursor.Found = true
	return cursor, nil
}

func (r pitrRepo) ListItemChangesUpToCursor(ctx context.Context, tableName string, cursor model.ItemChangeCursor) ([]model.ItemChange, error) {
	if !cursor.Found {
		return []model.ItemChange{}, nil
	}
	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ?
		  AND (changed_at < ? OR (changed_at = ? AND id <= ?))
		ORDER BY changed_at ASC, id ASC
	`, tableName, cursor.ChangedAt, cursor.ChangedAt, cursor.Sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r pitrRepo) ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableName string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error) {
	if !upTo.Found {
		return []model.ItemChange{}, nil
	}
	if !after.Found {
		return r.ListItemChangesUpToCursor(ctx, tableName, upTo)
	}
	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ?
		  AND (changed_at > ? OR (changed_at = ? AND id > ?))
		  AND (changed_at < ? OR (changed_at = ? AND id <= ?))
		ORDER BY changed_at ASC, id ASC
	`, tableName, after.ChangedAt, after.ChangedAt, after.Sequence, upTo.ChangedAt, upTo.ChangedAt, upTo.Sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r pitrRepo) GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableName string, upTo int64) (model.PITRCheckpoint, error) {
	if upTo <= 0 {
		return model.PITRCheckpoint{}, nil
	}
	var cursor model.ItemChangeCursor
	err := r.tx.QueryRowContext(ctx, `
		SELECT changed_at, history_sequence
		FROM pitr_checkpoints
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at DESC, history_sequence DESC
		LIMIT 1
	`, tableName, upTo).Scan(&cursor.ChangedAt, &cursor.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PITRCheckpoint{}, nil
		}
		return model.PITRCheckpoint{}, err
	}
	cursor.Found = true
	return r.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tableName, cursor)
}

func (r pitrRepo) GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableName string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	return r.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tableName, cursor)
}

func (r pitrRepo) CreatePITRCheckpointFromCurrentState(ctx context.Context, tableName string, changedAt int64) error {
	cursor, err := r.ResolveItemChangeCursorAtOrBefore(ctx, tableName, changedAt)
	if err != nil {
		return err
	}
	sequence := int64(0)
	if cursor.Found {
		sequence = cursor.Sequence
	} else if changedAt > 0 {
		sequence = -changedAt
	} else {
		sequence = -1
	}

	var existing int
	err = r.tx.QueryRowContext(ctx, `
		SELECT 1
		FROM pitr_checkpoints
		WHERE table_key = ? AND history_sequence = ?
		LIMIT 1
	`, tableName, sequence).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk, item_json
		FROM items
		WHERE table_key = ?
		ORDER BY pk ASC, sk ASC
	`, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	items := make([]model.PITRCheckpointItem, 0)
	for rows.Next() {
		var item model.PITRCheckpointItem
		var raw []byte
		if err := rows.Scan(&item.PK, &item.SK, &raw); err != nil {
			return err
		}
		item.Item = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &item.Item); err != nil {
				return err
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return r.insertPITRCheckpoint(ctx, tableName, changedAt, sequence, items)
}

func (r pitrRepo) getLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableName string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	if !cursor.Found {
		return model.PITRCheckpoint{}, nil
	}
	var checkpoint model.PITRCheckpoint
	var checkpointID int64
	err := r.tx.QueryRowContext(ctx, `
		SELECT id, changed_at, history_sequence
		FROM pitr_checkpoints
		WHERE table_key = ?
		  AND (changed_at < ? OR (changed_at = ? AND history_sequence <= ?))
		ORDER BY changed_at DESC, history_sequence DESC
		LIMIT 1
	`, tableName, cursor.ChangedAt, cursor.ChangedAt, cursor.Sequence).Scan(&checkpointID, &checkpoint.ChangedAt, &checkpoint.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PITRCheckpoint{}, nil
		}
		return model.PITRCheckpoint{}, err
	}
	checkpoint.Found = true
	items, err := r.listPITRCheckpointItems(ctx, checkpointID)
	if err != nil {
		return model.PITRCheckpoint{}, err
	}
	checkpoint.Items = items
	return checkpoint, nil
}

func (r pitrRepo) listPITRCheckpointItems(ctx context.Context, checkpointID int64) ([]model.PITRCheckpointItem, error) {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT pk, sk, item_json
		FROM pitr_checkpoint_items
		WHERE checkpoint_id = ?
	`, checkpointID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]model.PITRCheckpointItem, 0)
	for rows.Next() {
		var item model.PITRCheckpointItem
		var raw []byte
		if err := rows.Scan(&item.PK, &item.SK, &raw); err != nil {
			return nil, err
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &item.Item); err != nil {
				return nil, err
			}
		} else {
			item.Item = map[string]any{}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (r pitrRepo) DeleteItemChangesBefore(ctx context.Context, tableName string, before int64) (int64, error) {
	res, err := r.tx.ExecContext(ctx, `
		DELETE FROM item_history
		WHERE table_key = ? AND changed_at < ?
	`, tableName, before)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (r pitrRepo) CompactItemChangesBefore(ctx context.Context, tableName string, before int64) (int64, error) {
	if before <= 0 {
		return 0, nil
	}
	var hasRows int
	err := r.tx.QueryRowContext(ctx, `
		SELECT 1
		FROM item_history
		WHERE table_key = ? AND changed_at < ?
		LIMIT 1
	`, tableName, before).Scan(&hasRows)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	cursor, err := r.ResolveItemChangeCursorAtOrBefore(ctx, tableName, before)
	if err != nil {
		return 0, err
	}
	if cursor.Found {
		if err := r.createPITRCheckpointForCursor(ctx, tableName, cursor); err != nil {
			return 0, err
		}
	}

	return r.DeleteItemChangesBefore(ctx, tableName, before)
}

func (r pitrRepo) createPITRCheckpointForCursor(ctx context.Context, tableName string, cursor model.ItemChangeCursor) error {
	if !cursor.Found {
		return nil
	}

	var existing int
	err := r.tx.QueryRowContext(ctx, `
		SELECT 1
		FROM pitr_checkpoints
		WHERE table_key = ? AND history_sequence = ?
		LIMIT 1
	`, tableName, cursor.Sequence).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	previousCheckpoint, err := r.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tableName, cursor)
	if err != nil {
		return err
	}

	after := model.ItemChangeCursor{}
	if previousCheckpoint.Found {
		after = model.ItemChangeCursor{Found: true, ChangedAt: previousCheckpoint.ChangedAt, Sequence: previousCheckpoint.Sequence}
	}
	changes, err := r.ListItemChangesAfterCursorUpToCursor(ctx, tableName, after, cursor)
	if err != nil {
		return err
	}

	state := make(map[string]map[string]any)
	if previousCheckpoint.Found {
		for _, item := range previousCheckpoint.Items {
			state[item.PK+"\x00"+item.SK] = item.Item
		}
	}
	for _, change := range changes {
		key := change.PK + "\x00" + change.SK
		if strings.EqualFold(change.ChangeType, "DELETE") {
			delete(state, key)
			continue
		}
		if change.Item != nil {
			state[key] = change.Item
		}
	}

	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items := make([]model.PITRCheckpointItem, 0, len(keys))
	for _, key := range keys {
		sep := strings.IndexByte(key, '\x00')
		if sep < 0 {
			continue
		}
		items = append(items, model.PITRCheckpointItem{
			PK:   key[:sep],
			SK:   key[sep+1:],
			Item: state[key],
		})
	}

	return r.insertPITRCheckpoint(ctx, tableName, cursor.ChangedAt, cursor.Sequence, items)
}

func (r pitrRepo) insertPITRCheckpoint(ctx context.Context, tableName string, changedAt int64, sequence int64, items []model.PITRCheckpointItem) error {
	res, err := r.tx.ExecContext(ctx, `
		INSERT INTO pitr_checkpoints(table_key, changed_at, history_sequence, created_at)
		VALUES (?, ?, ?, ?)
	`, tableName, changedAt, sequence, time.Now().UnixMilli())
	if err != nil {
		return err
	}

	checkpointID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, item := range items {
		raw, err := json.Marshal(item.Item)
		if err != nil {
			return err
		}
		if _, err := r.tx.ExecContext(ctx, `
			INSERT INTO pitr_checkpoint_items(checkpoint_id, pk, sk, item_json)
			VALUES (?, ?, ?, ?)
		`, checkpointID, item.PK, item.SK, raw); err != nil {
			return err
		}
	}

	return nil
}

func (r pitrRepo) AppendItemChange(ctx context.Context, tableName, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	return r.appendItemHistory(ctx, tableName, pk, sk, changeType, item, changedAt)
}

func (r pitrRepo) appendItemHistory(ctx context.Context, tableName, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	var raw any
	if item != nil {
		payload, err := json.Marshal(item)
		if err != nil {
			return err
		}
		raw = string(payload)
	}
	_, err := r.tx.ExecContext(ctx, `
		INSERT INTO item_history(table_key, pk, sk, change_type, item_json, changed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, tableName, pk, sk, changeType, raw, changedAt)
	return err
}
