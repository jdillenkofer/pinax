package ttl

import (
	"context"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

type Sweeper struct {
	store    store.Store
	interval time.Duration
	stopCh   chan struct{}
}

const (
	tablePageSize  = 100
	sweepBatchSize = 1000
)

func NewSweeper(store store.Store, interval time.Duration) *Sweeper {
	return &Sweeper{
		store:    store,
		interval: interval,
		stopCh:   make(chan struct{}),
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
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tables, err := s.store.ListTables(ctx, tx, start, tablePageSize)
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

		tx, err := s.store.DB().BeginTx(ctx, nil)
		if err != nil {
			slog.Error("failed to start transaction for TTL batch delete", "table", tableName, "err", err)
			return
		}

		deleted, err := s.store.DeleteExpiredItems(ctx, tx, tableName, now, sweepBatchSize)
		if err != nil {
			_ = tx.Rollback()
			slog.Error("failed to delete expired items", "table", tableName, "err", err)
			return
		}

		if err := tx.Commit(); err != nil {
			slog.Error("failed to commit transaction for TTL batch delete", "table", tableName, "err", err)
			return
		}

		if deleted < sweepBatchSize {
			break
		}
	}
}

func (s *Sweeper) loadTable(ctx context.Context, tableName string) (model.Table, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return model.Table{}, err
	}
	defer tx.Rollback()

	table, err := s.store.GetTable(ctx, tx, tableName)
	if err != nil {
		return model.Table{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.Table{}, err
	}
	return table, nil
}
