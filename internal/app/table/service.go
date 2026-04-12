package table

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
)

var ErrTooManyTags = errors.New("too many tags")

type TableInUseError struct {
	Status string
}

func (e *TableInUseError) Error() string {
	if e == nil {
		return "table in use"
	}
	return "table is currently " + e.Status
}

type BillingUpdate struct {
	BillingMode        string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64
}

type UpdateTableInput struct {
	TableKey      string
	NowMillis     int64
	Billing       *BillingUpdate
	ApplyOptions  func(*model.Table) error
	ApplyGSI      func(model.Table, int64) ([]model.GlobalSecondaryIndex, error)
	SetTableState func(*model.Table, int64)
}

type Service struct {
	uow       uow.UnitOfWork
	lifecycle *LifecycleService
}

func NewService(unitOfWork uow.UnitOfWork, lifecycle *LifecycleService) *Service {
	return &Service{uow: unitOfWork, lifecycle: lifecycle}
}

func (s *Service) UpdateTable(ctx context.Context, input UpdateTableInput) (model.Table, int64, error) {
	if s.lifecycle == nil {
		return model.Table{}, 0, errors.New("lifecycle service is required")
	}
	now := input.NowMillis
	if now <= 0 {
		now = time.Now().UnixMilli()
	}
	var table model.Table
	var count int64
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.lifecycle.GetWithLifecycle(txCtx, repos.Tables(), input.TableKey, now)
		if err != nil {
			return err
		}
		if t.Status != model.TableStatusActive {
			return &TableInUseError{Status: t.Status}
		}
		if input.Billing != nil {
			t.BillingMode = input.Billing.BillingMode
			t.ReadCapacityUnits = input.Billing.ReadCapacityUnits
			t.WriteCapacityUnits = input.Billing.WriteCapacityUnits
			if err := repos.Tables().UpdateTableBilling(txCtx, t.Name, t.BillingMode, t.ReadCapacityUnits, t.WriteCapacityUnits); err != nil {
				return err
			}
		}
		if input.ApplyOptions != nil {
			if err := input.ApplyOptions(&t); err != nil {
				return err
			}
		}
		if err := repos.Tables().UpdateTableOptions(txCtx, t.Name, t.TableClass, t.DeletionProtection, t.Stream, t.SSE, t.Tags); err != nil {
			return err
		}
		if input.ApplyGSI != nil {
			updatedGSIs, err := input.ApplyGSI(t, now)
			if err != nil {
				return err
			}
			t.GSIs = updatedGSIs
			if input.SetTableState != nil {
				input.SetTableState(&t, now)
			}
			if err := repos.Tables().UpdateTableIndexes(txCtx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs); err != nil {
				return err
			}
		}
		count, err = repos.Items().CountItems(txCtx, t.Name)
		if err != nil {
			return err
		}
		table = t
		return nil
	})
	return table, count, err
}

func (s *Service) TagTable(ctx context.Context, tableKey string, tags []model.Tag, now int64) error {
	if s.lifecycle == nil {
		return errors.New("lifecycle service is required")
	}
	return s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.lifecycle.GetWithLifecycle(txCtx, repos.Tables(), tableKey, now)
		if err != nil {
			return err
		}
		nextTags := mergeTags(t.Tags, tags)
		if len(nextTags) > 50 {
			return ErrTooManyTags
		}
		return repos.Tables().UpdateTableOptions(txCtx, t.Name, t.TableClass, t.DeletionProtection, t.Stream, t.SSE, nextTags)
	})
}

func (s *Service) UntagTable(ctx context.Context, tableKey string, keys []string, now int64) error {
	if s.lifecycle == nil {
		return errors.New("lifecycle service is required")
	}
	return s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.lifecycle.GetWithLifecycle(txCtx, repos.Tables(), tableKey, now)
		if err != nil {
			return err
		}
		nextTags := removeTags(t.Tags, keys)
		return repos.Tables().UpdateTableOptions(txCtx, t.Name, t.TableClass, t.DeletionProtection, t.Stream, t.SSE, nextTags)
	})
}

func (s *Service) ListTableTags(ctx context.Context, tableKey string, now int64) ([]model.Tag, error) {
	if s.lifecycle == nil {
		return nil, errors.New("lifecycle service is required")
	}
	var tags []model.Tag
	err := s.uow.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.lifecycle.GetWithLifecycle(txCtx, repos.Tables(), tableKey, now)
		if err != nil {
			return err
		}
		tags = append([]model.Tag(nil), t.Tags...)
		sort.Slice(tags, func(i, j int) bool {
			if tags[i].Key == tags[j].Key {
				return tags[i].Value < tags[j].Value
			}
			return tags[i].Key < tags[j].Key
		})
		return nil
	})
	return tags, err
}

func mergeTags(existing []model.Tag, updates []model.Tag) []model.Tag {
	out := append([]model.Tag{}, existing...)
	for _, u := range updates {
		found := false
		for i := range out {
			if out[i].Key == u.Key {
				out[i].Value = u.Value
				found = true
				break
			}
		}
		if !found {
			out = append(out, u)
		}
	}
	return out
}

func removeTags(existing []model.Tag, keys []string) []model.Tag {
	if len(keys) == 0 {
		return append([]model.Tag{}, existing...)
	}
	keySet := map[string]struct{}{}
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	next := make([]model.Tag, 0, len(existing))
	for _, tag := range existing {
		if _, ok := keySet[tag.Key]; ok {
			continue
		}
		next = append(next, tag)
	}
	return next
}
