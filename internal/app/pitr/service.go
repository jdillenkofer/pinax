package pitr

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

var (
	ErrPointInTimeRecoveryUnavailable = errors.New("point in time recovery unavailable")
	ErrInvalidRestoreTime             = errors.New("invalid restore time")
	ErrTargetTableExists              = errors.New("target table already exists")
	ErrTargetTableInUse               = errors.New("target table in use")
)

type TableLoader func(ctx context.Context, txs uow.TxStore, tableName string) (model.Table, error)

type RestoreTableBuilder func(source model.Table) (model.Table, error)

type UpdateContinuousBackupsInput struct {
	TableName             string
	Enable                bool
	RecoveryPeriodInDays  int64
	NowMillis             int64
	DefaultRecoveryWindow int64
}

type RestoreTableToPointInTimeInput struct {
	SourceName                string
	TargetScopedTableKey      string
	UseLatestRestorableTime   bool
	RestoreDateTimeMillis     int64
	PITRLatestRestorableLagMs int64
	NowMillis                 int64
}

type Service struct {
	uow uow.UnitOfWork
}

func NewService(unitOfWork uow.UnitOfWork) *Service {
	return &Service{uow: unitOfWork}
}

func (s *Service) UpdateContinuousBackups(ctx context.Context, input UpdateContinuousBackupsInput, tableLoader TableLoader) (model.Table, int64, error) {
	if tableLoader == nil {
		return model.Table{}, 0, errors.New("loadTable callback is required")
	}
	nowMs := input.NowMillis
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	recoveryDefault := input.DefaultRecoveryWindow
	if recoveryDefault <= 0 {
		recoveryDefault = DefaultRecoveryPeriodInDays
	}

	var t model.Table
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		var err error
		t, err = tableLoader(ctx, txs, input.TableName)
		if err != nil {
			return err
		}

		recoveryDays := t.PITR.RecoveryPeriodInDays
		if recoveryDays <= 0 {
			recoveryDays = recoveryDefault
		}
		if input.RecoveryPeriodInDays != 0 {
			recoveryDays = input.RecoveryPeriodInDays
		}

		enabledAt := t.PITR.EnabledAt
		if input.Enable {
			if !t.PITR.Enabled || enabledAt == 0 {
				enabledAt = nowMs
				items, err := txs.Scan(ctx, t.Name, "", "", 0)
				if err != nil {
					return err
				}
				for _, item := range items {
					pk, sk, err := model.ExtractItemKeys(t, item)
					if err != nil {
						return err
					}
					if err := txs.AppendItemChange(ctx, t.Name, pk, sk, "PUT", item, enabledAt); err != nil {
						return err
					}
				}
				if err := txs.CreatePITRCheckpointFromCurrentState(ctx, t.Name, enabledAt); err != nil {
					return err
				}
			}
		} else {
			enabledAt = 0
		}

		next := model.PointInTimeRecovery{Enabled: input.Enable, RecoveryPeriodInDays: recoveryDays, EnabledAt: enabledAt}
		if err := txs.UpdatePointInTimeRecovery(ctx, t.Name, next); err != nil {
			return err
		}
		t.PITR = next

		if next.Enabled {
			cutoff := nowMs - (recoveryDays * 24 * 60 * 60 * 1000)
			if cutoff > 0 {
				if _, err := txs.CompactItemChangesBefore(ctx, t.Name, cutoff); err != nil {
					return err
				}
			}
		}
		return nil
	})

	return t, nowMs, err
}

func (s *Service) DescribeContinuousBackups(ctx context.Context, tableName string, nowMs int64, tableLoader TableLoader) (model.Table, int64, error) {
	if tableLoader == nil {
		return model.Table{}, 0, errors.New("loadTable callback is required")
	}
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}

	var t model.Table
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		var err error
		t, err = tableLoader(ctx, txs, tableName)
		return err
	})

	return t, nowMs, err
}

func (s *Service) RestoreTableToPointInTime(ctx context.Context, input RestoreTableToPointInTimeInput, tableLoader TableLoader, restoreTableBuilder RestoreTableBuilder) (model.Table, int64, error) {
	if tableLoader == nil {
		return model.Table{}, 0, errors.New("loadTable callback is required")
	}
	if restoreTableBuilder == nil {
		return model.Table{}, 0, errors.New("buildTable callback is required")
	}

	nowMs := input.NowMillis
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}

	var tableToCreate model.Table
	count := int64(0)
	err := s.uow.Do(ctx, func(txs uow.TxStore) error {
		source, err := tableLoader(ctx, txs, input.SourceName)
		if err != nil {
			return err
		}
		if !source.PITR.Enabled {
			return ErrPointInTimeRecoveryUnavailable
		}

		earliest, latest := RestoreWindow(source, nowMs, input.PITRLatestRestorableLagMs)
		restoreAt := latest
		if !input.UseLatestRestorableTime {
			restoreAt = input.RestoreDateTimeMillis
		}
		if restoreAt < earliest || restoreAt > latest {
			return ErrInvalidRestoreTime
		}

		if existing, err := txs.GetTable(ctx, input.TargetScopedTableKey); err == nil {
			if existing.Status == model.TableStatusCreating || existing.Status == model.TableStatusDeleting {
				return ErrTargetTableInUse
			}
			return ErrTargetTableExists
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		tableToCreate, err = restoreTableBuilder(source)
		if err != nil {
			return err
		}
		if err := txs.CreateTable(ctx, tableToCreate); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return ErrTargetTableExists
			}
			return err
		}

		cursor, err := txs.ResolveItemChangeCursorAtOrBefore(ctx, source.Name, restoreAt)
		if err != nil {
			return err
		}

		checkpoint, err := txs.GetLatestPITRCheckpointAtOrBeforeCursor(ctx, source.Name, cursor)
		if err != nil {
			return err
		}
		if !checkpoint.Found {
			checkpoint, err = txs.GetLatestPITRCheckpointAtOrBefore(ctx, source.Name, restoreAt)
		}
		if err != nil {
			return err
		}

		changes, err := txs.ListItemChangesAfterCursorUpToCursor(ctx, source.Name, model.ItemChangeCursor{Found: checkpoint.Found, ChangedAt: checkpoint.ChangedAt, Sequence: checkpoint.Sequence}, cursor)
		if err != nil {
			return err
		}

		state := map[string]map[string]any{}
		if checkpoint.Found {
			for _, item := range checkpoint.Items {
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

		for _, item := range state {
			pk, sk, err := model.ExtractItemKeys(tableToCreate, item)
			if err != nil {
				return err
			}
			if err := txs.PutItem(ctx, tableToCreate.Name, pk, sk, item); err != nil {
				return err
			}
			count++
		}

		return nil
	})

	return tableToCreate, count, err
}
