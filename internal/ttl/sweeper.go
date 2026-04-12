package ttl

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
)

type Sweeper struct {
	uow              uow.UnitOfWork
	db               *sql.DB
	mutationExecutor *mutation.Executor
	interval         time.Duration
	stopCh           chan struct{}
}

const (
	tablePageSize  = 100
	sweepBatchSize = 1000
)

func NewSweeper(db *sql.DB, unitOfWork uow.UnitOfWork, interval time.Duration, mutationExecutor *mutation.Executor) *Sweeper {
	if mutationExecutor == nil {
		panic("ttl sweeper requires a mutation executor")
	}
	return &Sweeper{
		uow:              unitOfWork,
		db:               db,
		mutationExecutor: mutationExecutor,
		interval:         interval,
		stopCh:           make(chan struct{}),
	}
}

func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.RunOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.RunOnce(ctx)
		}
	}
}

func (s *Sweeper) Stop() {
	close(s.stopCh)
}

func (s *Sweeper) RunOnce(ctx context.Context) {
	start := ""
	for {
		var tables []string
		if err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
			var err error
			tables, err = repos.Tables().ListTables(txCtx, start, tablePageSize)
			return err
		}); err != nil {
			slog.Error("failed to list tables for TTL sweep", "err", err)
			return
		}
		if len(tables) == 0 {
			return
		}

		for _, tableName := range tables {
			s.sweepTable(ctx, tableName)
		}

		if len(tables) < tablePageSize {
			return
		}
		start = tables[len(tables)-1]
	}
}

func (s *Sweeper) sweepTable(ctx context.Context, tableName string) {
	var t model.Table
	if err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = repos.Tables().GetTable(txCtx, tableName)
		return err
	}); err != nil {
		slog.Error("failed to get table for TTL sweep", "table", tableName, "err", err)
		return
	}

	if err := s.prunePITRHistory(ctx, t); err != nil {
		slog.Error("failed to prune PITR history", "table", tableName, "err", err)
		return
	}

	if !t.TimeToLive.Enabled || t.TimeToLive.AttrName == "" {
		return
	}

	now := time.Now().Unix()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		deleted, err := s.deleteExpiredBatch(ctx, t, now, sweepBatchSize)
		if err != nil {
			slog.Error("failed to delete expired items", "table", tableName, "err", err)
			return
		}

		if deleted < sweepBatchSize {
			break
		}
	}
}

func (s *Sweeper) deleteExpiredBatch(ctx context.Context, t model.Table, before int64, limit int) (int64, error) {
	var deleted int64
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		expired, err := repos.TTL().GetExpiredItems(txCtx, t.Name, t.TimeToLive.AttrName, before, limit)
		if err != nil {
			return err
		}
		for _, key := range expired {
			oldItem, err := repos.Items().GetItem(txCtx, t.Name, key.PK, key.SK)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return err
			}
			if err := repos.TTL().DeleteExpiredItem(txCtx, t.Name, key.PK, key.SK); err != nil {
				return err
			}
			changedAt := time.Now().UnixMilli()
			keys := map[string]any{t.HashKey: oldItem[t.HashKey]}
			if t.RangeKey != "" {
				keys[t.RangeKey] = oldItem[t.RangeKey]
			}
			if err := s.mutationExecutor.Emit(ctx, repos, mutation.Event{
				Table:     t,
				EventName: "REMOVE",
				PK:        key.PK,
				SK:        key.SK,
				Keys:      keys,
				OldImage:  oldItem,
				NewImage:  nil,
				ChangedAt: changedAt,
			}); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	return deleted, err
}

func (s *Sweeper) prunePITRHistory(ctx context.Context, t model.Table) error {
	if !t.PITR.Enabled {
		return nil
	}
	recoveryDays := t.PITR.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = 35
	}
	nowMs := time.Now().UnixMilli()
	cutoff := nowMs - (recoveryDays * 24 * 60 * 60 * 1000)
	if cutoff <= 0 {
		return nil
	}

	return s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		_, err := repos.PITR().CompactItemChangesBefore(txCtx, t.Name, cutoff)
		return err
	})
}
