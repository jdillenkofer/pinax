package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
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
	var tables []string
	if err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		tables, err = repos.Tables().ListTables(txCtx, start, gsiBackfillTablePageSize)
		return err
	}); err != nil {
		return nil, err
	}
	return tables, nil
}

func (s *Server) backfillTableGSIs(ctx context.Context, tableName string) {
	if err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := repos.Tables().GetTable(txCtx, tableName)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				slog.Error("gsi backfill worker failed to load table", "table", tableName, "err", err)
			}
			return nil
		}

		updated := false
		for i := range t.GSIs {
			if t.GSIs[i].Status != model.IndexStatusCreating {
				continue
			}
			if err := repos.Tables().BackfillGSIEntries(txCtx, t.Name, t.GSIs[i]); err != nil {
				slog.Error("gsi backfill worker failed to backfill index", "table", tableName, "index", t.GSIs[i].IndexName, "err", err)
				return nil
			}
			t.GSIs[i].Status = model.IndexStatusActive
			t.GSIs[i].StatusAt = 0
			updated = true
		}

		if !updated {
			return nil
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
		if err := repos.Tables().UpdateTableIndexes(txCtx, t.Name, tableStatus, tableStatusAt, t.GSIs, t.LSIs); err != nil {
			slog.Error("gsi backfill worker failed to persist table index status", "table", tableName, "err", err)
			return nil
		}
		return nil
	}); err != nil {
		slog.Error("gsi backfill worker failed to run backfill", "table", tableName, "err", err)
		return
	}
}
