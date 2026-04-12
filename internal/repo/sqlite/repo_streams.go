package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/jdillenkofer/pinax/internal/model"
	"strings"
)

func (r sqlTxRepo) AppendStreamRecord(ctx context.Context, record model.StreamRecord) error {
	keysJSON, err := json.Marshal(record.Keys)
	if err != nil {
		return err
	}
	var oldImage any
	if record.OldImage != nil {
		oldJSON, err := json.Marshal(record.OldImage)
		if err != nil {
			return err
		}
		oldImage = oldJSON
	}
	var newImage any
	if record.NewImage != nil {
		newJSON, err := json.Marshal(record.NewImage)
		if err != nil {
			return err
		}
		newImage = newJSON
	}
	_, err = r.tx.ExecContext(ctx, `
		INSERT INTO stream_records(table_key, stream_arn, shard_id, event_name, keys_json, old_image_json, new_image_json, changed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, streamTableNameFromARN(record.StreamARN), record.StreamARN, firstNonEmpty(record.ShardID, "shardId-000000000000"), record.EventName, keysJSON, oldImage, newImage, record.ChangedAt)
	return err
}

func (r sqlTxRepo) ListStreamRecordsAfterSequence(ctx context.Context, streamARN string, sequence int64, limit int) ([]model.StreamRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.tx.QueryContext(ctx, `
		SELECT id, shard_id, event_name, keys_json, old_image_json, new_image_json, changed_at
		FROM stream_records
		WHERE stream_arn = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?
	`, streamARN, sequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.StreamRecord, 0)
	for rows.Next() {
		var rec model.StreamRecord
		var keysJSON []byte
		var oldJSON []byte
		var newJSON []byte
		if err := rows.Scan(&rec.Sequence, &rec.ShardID, &rec.EventName, &keysJSON, &oldJSON, &newJSON, &rec.ChangedAt); err != nil {
			return nil, err
		}
		rec.StreamARN = streamARN
		if err := json.Unmarshal(keysJSON, &rec.Keys); err != nil {
			return nil, err
		}
		if len(oldJSON) > 0 {
			if err := json.Unmarshal(oldJSON, &rec.OldImage); err != nil {
				return nil, err
			}
		}
		if len(newJSON) > 0 {
			if err := json.Unmarshal(newJSON, &rec.NewImage); err != nil {
				return nil, err
			}
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r sqlTxRepo) GetStreamSequenceBounds(ctx context.Context, streamARN string) (int64, int64, bool, error) {
	var first int64
	err := r.tx.QueryRowContext(ctx, `SELECT id FROM stream_records WHERE stream_arn = ? ORDER BY id ASC LIMIT 1`, streamARN).Scan(&first)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	var last int64
	err = r.tx.QueryRowContext(ctx, `SELECT id FROM stream_records WHERE stream_arn = ? ORDER BY id DESC LIMIT 1`, streamARN).Scan(&last)
	if err != nil {
		return 0, 0, false, err
	}
	return first, last, true, nil
}

func (r sqlTxRepo) GetStreamRecordChangedAt(ctx context.Context, streamARN string, sequence int64) (int64, bool, error) {
	var changedAt int64
	err := r.tx.QueryRowContext(ctx, `SELECT changed_at FROM stream_records WHERE stream_arn = ? AND id = ?`, streamARN, sequence).Scan(&changedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return changedAt, true, nil
}

func (r sqlTxRepo) DeleteStreamRecordsBefore(ctx context.Context, streamARN string, before int64) (int64, error) {
	res, err := r.tx.ExecContext(ctx, `DELETE FROM stream_records WHERE stream_arn = ? AND changed_at < ?`, streamARN, before)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func streamTableNameFromARN(streamARN string) string {
	accountID := "000000000000"
	parts := strings.SplitN(strings.TrimSpace(streamARN), ":", 6)
	if len(parts) >= 5 && strings.TrimSpace(parts[4]) != "" {
		accountID = strings.TrimSpace(parts[4])
	}
	marker := "/table/"
	start := strings.Index(streamARN, marker)
	if start < 0 {
		marker = ":table/"
		start = strings.Index(streamARN, marker)
	}
	if start < 0 {
		return ""
	}
	remainder := streamARN[start+len(marker):]
	if slash := strings.Index(remainder, "/"); slash >= 0 {
		remainder = remainder[:slash]
	}
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return ""
	}
	return accountID + "#" + remainder
}
