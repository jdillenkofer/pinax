package backup

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

var (
	ErrBackupExists      = errors.New("backup already exists")
	ErrTargetTableExists = errors.New("target table already exists")
	ErrTargetTableInUse  = errors.New("target table in use")
)

type TableLoader func(ctx context.Context, txs uow.TxStore, tableName string) (model.Table, error)

type BackupBuilder func(table model.Table, itemCount int64, items []map[string]any) (model.Backup, error)

type RestoreTableBuilder func(backup model.Backup) (model.Table, error)

type Service struct {
	uow uow.UnitOfWork
}

func NewService(unitOfWork uow.UnitOfWork) *Service {
	return &Service{uow: unitOfWork}
}

func (s *Service) CreateBackup(ctx context.Context, tableName, backupName string, loadTable TableLoader, buildBackup BackupBuilder) (model.Backup, error) {
	if loadTable == nil {
		return model.Backup{}, errors.New("loadTable callback is required")
	}
	if buildBackup == nil {
		return model.Backup{}, errors.New("buildBackup callback is required")
	}

	var backup model.Backup
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		t, err := loadTable(ctx, txs, tableName)
		if err != nil {
			return err
		}
		if t.Status != model.TableStatusActive {
			return ErrTargetTableInUse
		}

		if _, err := txs.GetBackupByName(ctx, backupName); err == nil {
			return ErrBackupExists
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		count, err := txs.CountItems(ctx, t.Name)
		if err != nil {
			return err
		}
		items, err := txs.Scan(ctx, t.Name, "", "", 0)
		if err != nil {
			return err
		}
		backup, err = buildBackup(t, count, items)
		if err != nil {
			return err
		}
		if err := txs.CreateBackup(ctx, backup); err != nil {
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
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		var err error
		backup, err = txs.GetBackup(ctx, backupARN)
		return err
	})
	return backup, err
}

func (s *Service) DeleteBackup(ctx context.Context, backupARN string) (model.Backup, error) {
	var deleted model.Backup
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		backup, err := txs.GetBackup(ctx, backupARN)
		if err != nil {
			return err
		}
		deleted = backup
		deleted.BackupStatus = model.BackupStatusDeleted
		if err := txs.DeleteBackup(ctx, backupARN); err != nil {
			return err
		}
		return nil
	})
	return deleted, err
}

func (s *Service) ListBackups(ctx context.Context) ([]model.Backup, error) {
	var backups []model.Backup
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		var err error
		backups, err = txs.ListBackups(ctx)
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
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		backup, err := txs.GetBackup(ctx, backupARN)
		if err != nil {
			return err
		}
		if existing, err := txs.GetTable(ctx, targetScopedTableKey); err == nil {
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
		if err := txs.CreateTable(ctx, tableToCreate); err != nil {
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
			if err := txs.PutItem(ctx, tableToCreate.Name, pk, sk, item); err != nil {
				return err
			}
		}
		itemCount = len(backup.SnapshotItems)
		return nil
	})

	return tableToCreate, itemCount, err
}
