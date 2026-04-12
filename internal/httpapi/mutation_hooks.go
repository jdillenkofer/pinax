package httpapi

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

type MutationEvent struct {
	Table     model.Table
	EventName string
	Keys      map[string]any
	OldImage  map[string]any
	NewImage  map[string]any
	ChangedAt int64
}

type MutationHook interface {
	HandleMutation(ctx context.Context, tx *sql.Tx, event MutationEvent) error
}

func (s *Server) emitMutationHooks(ctx context.Context, tx *sql.Tx, event MutationEvent) error {
	for _, hook := range s.mutationHooks {
		if hook == nil {
			continue
		}
		if err := hook.HandleMutation(ctx, tx, event); err != nil {
			return err
		}
	}
	return nil
}

type streamMutationHook struct {
	store store.Store
}

func newStreamMutationHook(store store.Store) MutationHook {
	if store == nil {
		return nil
	}
	return &streamMutationHook{store: store}
}

func (h *streamMutationHook) HandleMutation(ctx context.Context, tx *sql.Tx, event MutationEvent) error {
	if !event.Table.Stream.Enabled || strings.TrimSpace(event.Table.Stream.ARN) == "" {
		return nil
	}
	oldView, newView := filterStreamImages(event.Table.Stream.ViewType, event.OldImage, event.NewImage)
	if err := h.store.AppendStreamRecord(ctx, tx, model.StreamRecord{
		StreamARN: event.Table.Stream.ARN,
		ShardID:   streamDefaultShardID,
		EventName: event.EventName,
		Keys:      event.Keys,
		OldImage:  oldView,
		NewImage:  newView,
		ChangedAt: event.ChangedAt,
	}); err != nil {
		return err
	}
	_, err := h.store.DeleteStreamRecordsBefore(ctx, tx, event.Table.Stream.ARN, event.ChangedAt-streamRetentionMillis)
	return err
}

func filterStreamImages(viewType string, oldImage, newImage map[string]any) (map[string]any, map[string]any) {
	switch viewType {
	case "KEYS_ONLY":
		return nil, nil
	case "NEW_IMAGE":
		return nil, newImage
	case "OLD_IMAGE":
		return oldImage, nil
	case "NEW_AND_OLD_IMAGES":
		return oldImage, newImage
	default:
		return nil, nil
	}
}
