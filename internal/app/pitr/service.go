package pitr

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

var (
	ErrPointInTimeRecoveryUnavailable = errors.New("point in time recovery unavailable")
	ErrInvalidRestoreTime             = errors.New("invalid restore time")
	ErrTargetTableExists              = errors.New("target table already exists")
	ErrTargetTableInUse               = errors.New("target table in use")
)

type TableLoader func(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error)

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
	store store.Store
}

func NewService(store store.Store) *Service {
	return &Service{store: store}
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
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		t, err = tableLoader(ctx, tx, input.TableName)
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
				items, err := s.store.Scan(ctx, tx, t.Name, "", "", 0)
				if err != nil {
					return err
				}
				for _, item := range items {
					pk, sk, err := model.ExtractItemKeys(t, item)
					if err != nil {
						return err
					}
					if err := s.store.AppendItemChange(ctx, tx, t.Name, pk, sk, "PUT", item, enabledAt); err != nil {
						return err
					}
				}
				if err := s.store.CreatePITRCheckpointFromCurrentState(ctx, tx, t.Name, enabledAt); err != nil {
					return err
				}
			}
		} else {
			enabledAt = 0
		}

		next := model.PointInTimeRecovery{Enabled: input.Enable, RecoveryPeriodInDays: recoveryDays, EnabledAt: enabledAt}
		if err := s.store.UpdatePointInTimeRecovery(ctx, tx, t.Name, next); err != nil {
			return err
		}
		t.PITR = next

		if next.Enabled {
			cutoff := nowMs - (recoveryDays * 24 * 60 * 60 * 1000)
			if cutoff > 0 {
				if _, err := s.store.CompactItemChangesBefore(ctx, tx, t.Name, cutoff); err != nil {
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
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		t, err = tableLoader(ctx, tx, tableName)
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
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		source, err := tableLoader(ctx, tx, input.SourceName)
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

		if existing, err := s.store.GetTable(ctx, tx, input.TargetScopedTableKey); err == nil {
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
		if err := s.store.CreateTable(ctx, tx, tableToCreate); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return ErrTargetTableExists
			}
			return err
		}

		cursor, err := s.store.ResolveItemChangeCursorAtOrBefore(ctx, tx, source.Name, restoreAt)
		if err != nil {
			return err
		}

		checkpoint, err := s.store.GetLatestPITRCheckpointAtOrBeforeCursor(ctx, tx, source.Name, cursor)
		if err != nil {
			return err
		}
		if !checkpoint.Found {
			checkpoint, err = s.store.GetLatestPITRCheckpointAtOrBefore(ctx, tx, source.Name, restoreAt)
		}
		if err != nil {
			return err
		}

		changes, err := s.store.ListItemChangesAfterCursorUpToCursor(ctx, tx, source.Name, model.ItemChangeCursor{Found: checkpoint.Found, ChangedAt: checkpoint.ChangedAt, Sequence: checkpoint.Sequence}, cursor)
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
			if err := s.store.PutItem(ctx, tx, tableToCreate.Name, pk, sk, item); err != nil {
				return err
			}
			count++
		}

		return nil
	})

	return tableToCreate, count, err
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
