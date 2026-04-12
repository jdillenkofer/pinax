package ttl

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
	reposqlite "github.com/jdillenkofer/pinax/internal/repo/sqlite"
)

type Sweeper struct {
	db               *sql.DB
	txReposFactory   reposqlite.Factory
	mutationExecutor *mutation.Executor
	interval         time.Duration
	stopCh           chan struct{}
}

const (
	tablePageSize  = 100
	sweepBatchSize = 1000
)

func NewSweeper(db *sql.DB, txReposFactory reposqlite.Factory, interval time.Duration, mutationExecutor *mutation.Executor) *Sweeper {
	if mutationExecutor == nil {
		panic("ttl sweeper requires a mutation executor")
	}
	return &Sweeper{
		db:               db,
		txReposFactory:   txReposFactory,
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
		tables, err := s.listTablesPage(ctx, start)
		if err != nil {
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

func (s *Sweeper) listTablesPage(ctx context.Context, start string) ([]string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tables, err := s.txReposFactory.Build(tx).Tables().ListTables(ctx, start, tablePageSize)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *Sweeper) sweepTable(ctx context.Context, tableName string) {
	t, err := s.loadTable(ctx, tableName)
	if err != nil {
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	expired, err := s.txReposFactory.TTL(tx).GetExpiredItems(ctx, t.Name, t.TimeToLive.AttrName, before, limit)
	if err != nil {
		return 0, err
	}
	deleted := int64(0)
	for _, key := range expired {
		oldItem, err := s.txReposFactory.Build(tx).Items().GetItem(ctx, t.Name, key.PK, key.SK)
		if err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return 0, err
		}
		if err := s.txReposFactory.TTL(tx).DeleteExpiredItem(ctx, t.Name, key.PK, key.SK); err != nil {
			return 0, err
		}
		changedAt := time.Now().UnixMilli()
		keys := map[string]any{t.HashKey: oldItem[t.HashKey]}
		if t.RangeKey != "" {
			keys[t.RangeKey] = oldItem[t.RangeKey]
		}
		if err := s.mutationExecutor.Emit(ctx, s.txReposFactory.Build(tx), mutation.Event{
			Table:     t,
			EventName: "REMOVE",
			PK:        key.PK,
			SK:        key.SK,
			Keys:      keys,
			OldImage:  oldItem,
			NewImage:  nil,
			ChangedAt: changedAt,
		}); err != nil {
			return 0, err
		}
		deleted++
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := s.txReposFactory.Build(tx).PITR().CompactItemChangesBefore(ctx, t.Name, cutoff); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Sweeper) loadTable(ctx context.Context, tableName string) (model.Table, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Table{}, err
	}
	defer tx.Rollback()

	table, err := s.txReposFactory.Build(tx).Tables().GetTable(ctx, tableName)
	if err != nil {
		return model.Table{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.Table{}, err
	}
	return table, nil
}
