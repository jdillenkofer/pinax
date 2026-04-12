package backup

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

var (
	ErrBackupExists      = errors.New("backup already exists")
	ErrTargetTableExists = errors.New("target table already exists")
	ErrTargetTableInUse  = errors.New("target table in use")
)

type TableLoader func(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error)

type BackupBuilder func(table model.Table, itemCount int64, items []map[string]any) (model.Backup, error)

type RestoreTableBuilder func(backup model.Backup) (model.Table, error)

type Service struct {
	store store.Store
}

func NewService(store store.Store) *Service {
	return &Service{store: store}
}

func (s *Service) CreateBackup(ctx context.Context, tableName, backupName string, loadTable TableLoader, buildBackup BackupBuilder) (model.Backup, error) {
	if loadTable == nil {
		return model.Backup{}, errors.New("loadTable callback is required")
	}
	if buildBackup == nil {
		return model.Backup{}, errors.New("buildBackup callback is required")
	}

	var backup model.Backup
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		t, err := loadTable(ctx, tx, tableName)
		if err != nil {
			return err
		}
		if t.Status != model.TableStatusActive {
			return ErrTargetTableInUse
		}

		if _, err := s.store.GetBackupByName(ctx, tx, backupName); err == nil {
			return ErrBackupExists
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		count, err := s.store.CountItems(ctx, tx, t.Name)
		if err != nil {
			return err
		}
		items, err := s.store.Scan(ctx, tx, t.Name, "", "", 0)
		if err != nil {
			return err
		}
		backup, err = buildBackup(t, count, items)
		if err != nil {
			return err
		}
		if err := s.store.CreateBackup(ctx, tx, backup); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return ErrBackupExists
			}
			return err
		}
		return nil
	})
	return backup, err
}

func (s *Service) DescribeBackup(ctx context.Context, backupARN string) (model.Backup, error) {
	var backup model.Backup
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		backup, err = s.store.GetBackup(ctx, tx, backupARN)
		return err
	})
	return backup, err
}

func (s *Service) DeleteBackup(ctx context.Context, backupARN string) (model.Backup, error) {
	var deleted model.Backup
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		backup, err := s.store.GetBackup(ctx, tx, backupARN)
		if err != nil {
			return err
		}
		deleted = backup
		deleted.BackupStatus = model.BackupStatusDeleted
		if err := s.store.DeleteBackup(ctx, tx, backupARN); err != nil {
			return err
		}
		return nil
	})
	return deleted, err
}

func (s *Service) ListBackups(ctx context.Context) ([]model.Backup, error) {
	var backups []model.Backup
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		backups, err = s.store.ListBackups(ctx, tx)
		return err
	})
	return backups, err
}

func (s *Service) RestoreTableFromBackup(ctx context.Context, backupARN, targetScopedTableKey string, buildTable RestoreTableBuilder) (model.Table, int, error) {
	if buildTable == nil {
		return model.Table{}, 0, errors.New("buildTable callback is required")
	}

	var tableToCreate model.Table
	itemCount := 0
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		backup, err := s.store.GetBackup(ctx, tx, backupARN)
		if err != nil {
			return err
		}
		if existing, err := s.store.GetTable(ctx, tx, targetScopedTableKey); err == nil {
			if existing.Status == model.TableStatusCreating || existing.Status == model.TableStatusDeleting {
				return ErrTargetTableInUse
			}
			return ErrTargetTableExists
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		tableToCreate, err = buildTable(backup)
		if err != nil {
			return err
		}
		if err := s.store.CreateTable(ctx, tx, tableToCreate); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return ErrTargetTableExists
			}
			return err
		}
		for _, item := range backup.SnapshotItems {
			pk, sk, err := model.ExtractItemKeys(tableToCreate, item)
			if err != nil {
				return err
			}
			if err := s.store.PutItem(ctx, tx, tableToCreate.Name, pk, sk, item); err != nil {
				return err
			}
		}
		itemCount = len(backup.SnapshotItems)
		return nil
	})

	return tableToCreate, itemCount, err
}

func (s *Service) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
