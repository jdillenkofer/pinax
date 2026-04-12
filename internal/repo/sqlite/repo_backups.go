package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/jdillenkofer/pinax/internal/model"
)

func (r backupRepo) CreateBackup(ctx context.Context, backup model.Backup) error {
	sourceTableDetailsJSON, err := json.Marshal(backup.SourceTableDetails)
	if err != nil {
		return err
	}
	sourceTableFeatureDetailsJSON, err := json.Marshal(backup.SourceTableFeatureDetails)
	if err != nil {
		return err
	}
	snapshotTableJSON, err := json.Marshal(backup.SnapshotTable)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `
		INSERT INTO backups(
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, backup.BackupARN, backup.BackupName, backup.TableName, backup.TableARN, backup.TableID, backup.BackupStatus, backup.BackupType, backup.BackupCreationDateTime, backup.BackupSizeBytes, string(sourceTableDetailsJSON), string(sourceTableFeatureDetailsJSON), string(snapshotTableJSON))
	if err != nil {
		return err
	}
	for i, item := range backup.SnapshotItems {
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if _, err := r.tx.ExecContext(ctx, `
			INSERT INTO backup_items(backup_arn, ordinal, item_json)
			VALUES (?, ?, ?)
		`, backup.BackupARN, i, raw); err != nil {
			return err
		}
	}
	return nil
}

func (r backupRepo) GetBackup(ctx context.Context, backupARN string) (model.Backup, error) {
	row := r.tx.QueryRowContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
		WHERE backup_arn = ?
	`, backupARN)
	backup, err := scanBackupMetadata(row)
	if err != nil {
		return model.Backup{}, err
	}
	items, err := r.loadBackupItems(ctx, backup.BackupARN)
	if err != nil {
		return model.Backup{}, err
	}
	backup.SnapshotItems = items
	return backup, nil
}

func (r backupRepo) GetBackupByName(ctx context.Context, backupName string) (model.Backup, error) {
	row := r.tx.QueryRowContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
		WHERE backup_name = ?
	`, backupName)
	backup, err := scanBackupMetadata(row)
	if err != nil {
		return model.Backup{}, err
	}
	items, err := r.loadBackupItems(ctx, backup.BackupARN)
	if err != nil {
		return model.Backup{}, err
	}
	backup.SnapshotItems = items
	return backup, nil
}

func (r backupRepo) ListBackups(ctx context.Context) ([]model.Backup, error) {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.Backup, 0)
	for rows.Next() {
		backup, err := scanBackupMetadata(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, backup)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r backupRepo) DeleteBackup(ctx context.Context, backupARN string) error {
	res, err := r.tx.ExecContext(ctx, `DELETE FROM backups WHERE backup_arn = ?`, backupARN)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

type backupScanner interface {
	Scan(dest ...any) error
}

func scanBackupMetadata(scanner backupScanner) (model.Backup, error) {
	var backup model.Backup
	var sourceTableDetailsJSON string
	var sourceTableFeatureDetailsJSON string
	var snapshotTableJSON string
	if err := scanner.Scan(
		&backup.BackupARN,
		&backup.BackupName,
		&backup.TableName,
		&backup.TableARN,
		&backup.TableID,
		&backup.BackupStatus,
		&backup.BackupType,
		&backup.BackupCreationDateTime,
		&backup.BackupSizeBytes,
		&sourceTableDetailsJSON,
		&sourceTableFeatureDetailsJSON,
		&snapshotTableJSON,
	); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(sourceTableDetailsJSON), &backup.SourceTableDetails); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(sourceTableFeatureDetailsJSON), &backup.SourceTableFeatureDetails); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(snapshotTableJSON), &backup.SnapshotTable); err != nil {
		return model.Backup{}, err
	}
	return backup, nil
}

func (r backupRepo) loadBackupItems(ctx context.Context, backupARN string) ([]map[string]any, error) {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT item_json
		FROM backup_items
		WHERE backup_arn = ?
		ORDER BY ordinal ASC
	`, backupARN)
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
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
