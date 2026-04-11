package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
)

const gsiBackfillTablePageSize = 100

func (s *Server) StartGSIBackfillWorker(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.runGSIBackfillOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runGSIBackfillOnce(ctx)
		}
	}
}

func (s *Server) runGSIBackfillOnce(ctx context.Context) {
	start := ""
	for {
		tables, err := s.listTablesPageForBackfill(ctx, start)
		if err != nil {
			slog.Error("gsi backfill worker failed to list tables", "err", err)
			return
		}
		if len(tables) == 0 {
			return
		}
		for _, tableName := range tables {
			s.backfillTableGSIs(ctx, tableName)
		}
		if len(tables) < gsiBackfillTablePageSize {
			return
		}
		start = tables[len(tables)-1]
	}
}

func (s *Server) listTablesPageForBackfill(ctx context.Context, start string) ([]string, error) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tables, err := s.store.ListTables(ctx, tx, start, gsiBackfillTablePageSize)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *Server) backfillTableGSIs(ctx context.Context, tableName string) {
	tx, err := s.store.DB().BeginTx(ctx, nil)
	if err != nil {
		slog.Error("gsi backfill worker failed to start transaction", "table", tableName, "err", err)
		return
	}
	defer tx.Rollback()

	t, err := s.store.GetTable(ctx, tx, tableName)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			slog.Error("gsi backfill worker failed to load table", "table", tableName, "err", err)
		}
		return
	}

	updated := false
	for i := range t.GSIs {
		if t.GSIs[i].Status != model.IndexStatusCreating {
			continue
		}
		if err := s.store.BackfillGSIEntries(ctx, tx, t.Name, t.GSIs[i]); err != nil {
			slog.Error("gsi backfill worker failed to backfill index", "table", tableName, "index", t.GSIs[i].IndexName, "err", err)
			return
		}
		t.GSIs[i].Status = model.IndexStatusActive
		t.GSIs[i].StatusAt = 0
		updated = true
	}

	if !updated {
		return
	}
	tableStatus := model.TableStatusActive
	for _, g := range t.GSIs {
		if g.Status == model.IndexStatusCreating || g.Status == model.IndexStatusDeleting {
			tableStatus = model.TableStatusUpdating
			break
		}
	}
	tableStatusAt := int64(0)
	if tableStatus == model.TableStatusUpdating {
		tableStatusAt = t.StatusAt
	}
	if err := s.store.UpdateTableIndexes(ctx, tx, t.Name, tableStatus, tableStatusAt, t.GSIs, t.LSIs); err != nil {
		slog.Error("gsi backfill worker failed to persist table index status", "table", tableName, "err", err)
		return
	}
	if err := tx.Commit(); err != nil {
		slog.Error("gsi backfill worker failed to commit backfill", "table", tableName, "err", err)
		return
	}
}
