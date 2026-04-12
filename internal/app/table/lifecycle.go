package table

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

var ErrTableNotFound = errors.New("table not found")

type LifecycleService struct{}

func NewLifecycleService() *LifecycleService {
	return &LifecycleService{}
}

func (s *LifecycleService) GetWithLifecycle(ctx context.Context, tables uow.TableRepo, tableKey string, now int64) (model.Table, error) {
	t, err := tables.GetTable(ctx, tableKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Table{}, ErrTableNotFound
		}
		return model.Table{}, err
	}
	if err := s.RefreshLifecycle(ctx, tables, &t, now); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Table{}, ErrTableNotFound
		}
		return model.Table{}, err
	}
	return t, nil
}

func (s *LifecycleService) RefreshLifecycle(ctx context.Context, tables uow.TableRepo, t *model.Table, now int64) error {
	if t.Status == model.TableStatusDeleting && t.StatusAt > 0 && now >= t.StatusAt {
		if err := tables.DeleteTable(ctx, t.Name); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if t.Status == model.TableStatusCreating && t.StatusAt > 0 && now >= t.StatusAt {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
	}
	if !advanceTableLifecycle(t, now) {
		return nil
	}
	return tables.UpdateTableIndexes(ctx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs)
}

func advanceTableLifecycle(t *model.Table, now int64) bool {
	changed := false
	updatedGSIs := make([]model.GlobalSecondaryIndex, 0, len(t.GSIs))
	pending := false

	for _, g := range t.GSIs {
		status := strings.TrimSpace(g.Status)
		if status == "" {
			status = model.IndexStatusActive
		}
		if (status == model.IndexStatusCreating || status == model.IndexStatusDeleting) && g.StatusAt > 0 && now >= g.StatusAt {
			if status == model.IndexStatusDeleting {
				changed = true
				continue
			}
			g.Status = model.IndexStatusActive
			g.StatusAt = 0
			changed = true
		}
		if g.Status == model.IndexStatusCreating || g.Status == model.IndexStatusDeleting {
			pending = true
		}
		updatedGSIs = append(updatedGSIs, g)
	}

	if len(updatedGSIs) != len(t.GSIs) {
		changed = true
	}
	t.GSIs = updatedGSIs

	if pending {
		if t.Status != model.TableStatusUpdating {
			t.Status = model.TableStatusUpdating
			changed = true
		}
	} else if t.Status != model.TableStatusActive {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
		changed = true
	}

	if t.Status == model.TableStatusUpdating && t.StatusAt > 0 && now >= t.StatusAt && !pending {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
		changed = true
	}

	return changed
}
