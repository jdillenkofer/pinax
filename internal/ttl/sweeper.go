package ttl

import (
	"context"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/store"
)

type Sweeper struct {
	store    store.Store
	interval time.Duration
	stopCh   chan struct{}
}

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
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		slog.Error("failed to start transaction for TTL sweep", "err", err)
		return
	}
	defer tx.Rollback()

	tables, err := s.store.ListTables(ctx, tx, "", 100)
	if err != nil {
		slog.Error("failed to list tables for TTL sweep", "err", err)
		return
	}

	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit transaction for TTL list tables", "err", err)
		return
	}

	for _, tableName := range tables {
		s.sweepTable(ctx, tableName)
	}
}

func (s *Sweeper) sweepTable(ctx context.Context, tableName string) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		slog.Error("failed to start transaction for TTL sweepTable", "table", tableName, "err", err)
		return
	}
	defer tx.Rollback()

	t, err := s.store.GetTable(ctx, tx, tableName)
	if err != nil {
		slog.Error("failed to get table for TTL sweep", "table", tableName, "err", err)
		return
	}

	if !t.TimeToLive.Enabled || t.TimeToLive.AttrName == "" {
		return
	}

	now := time.Now().Unix()
	limit := 100

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		expired, err := s.store.GetExpiredItems(ctx, tx, tableName, t.TimeToLive.AttrName, now, limit)
		if err != nil {
			slog.Error("failed to get expired items", "table", tableName, "err", err)
			return
		}

		if len(expired) == 0 {
			break
		}

		for _, item := range expired {
			if err := s.store.DeleteExpiredItem(ctx, tx, tableName, item.PK, item.SK); err != nil {
				slog.Error("failed to delete expired item", "table", tableName, "pk", item.PK, "sk", item.SK, "err", err)
			}
		}

		if len(expired) < limit {
			break
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("failed to commit transaction for TTL sweepTable", "table", tableName, "err", err)
	}
}
