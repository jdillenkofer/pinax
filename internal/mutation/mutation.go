package mutation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

const (
	streamDefaultShardID  = "shardId-000000000000"
	streamRetentionMillis = 24 * 60 * 60 * 1000
)

type Event struct {
	Table     model.Table
	EventName string
	PK        string
	SK        string
	Keys      map[string]any
	OldImage  map[string]any
	NewImage  map[string]any
	ChangedAt int64
}

type Hook interface {
	HandleMutation(ctx context.Context, tx *sql.Tx, event Event) error
}

type Executor struct {
	hooks []Hook
}

func NewExecutor(hooks ...Hook) *Executor {
	hooksCopy := append([]Hook(nil), hooks...)
	return &Executor{hooks: hooksCopy}
}

func (e *Executor) Emit(ctx context.Context, tx *sql.Tx, event Event) error {
	if e == nil {
		return nil
	}
	for _, hook := range e.hooks {
		if hook == nil {
			continue
		}
		if err := hook.HandleMutation(ctx, tx, event); err != nil {
			return err
		}
	}
	return nil
}

func DefaultHooks(s store.Store) []Hook {
	return []Hook{NewStreamHook(s), NewPITRHook(s)}
}

type streamHook struct {
	store store.Store
}

func NewStreamHook(s store.Store) Hook {
	if s == nil {
		return nil
	}
	return &streamHook{store: s}
}

func (h *streamHook) HandleMutation(ctx context.Context, tx *sql.Tx, event Event) error {
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

type pitrHook struct {
	store store.Store
}

func NewPITRHook(s store.Store) Hook {
	if s == nil {
		return nil
	}
	return &pitrHook{store: s}
}

func (h *pitrHook) HandleMutation(ctx context.Context, tx *sql.Tx, event Event) error {
	if !event.Table.PITR.Enabled {
		return nil
	}
	pitrType := pitrTypeFromMutationEventName(event.EventName)
	if strings.TrimSpace(pitrType) == "" {
		return nil
	}
	pk := strings.TrimSpace(event.PK)
	if pk == "" {
		return fmt.Errorf("missing mutation partition key")
	}
	sk := event.SK
	if strings.TrimSpace(sk) == "" {
		sk = model.NoSortKey
	}
	changedAt := event.ChangedAt
	if changedAt <= 0 {
		changedAt = time.Now().UnixMilli()
	}
	item := event.NewImage
	if strings.EqualFold(pitrType, "DELETE") {
		item = nil
	}
	if err := h.store.AppendItemChange(ctx, tx, event.Table.Name, pk, sk, pitrType, item, changedAt); err != nil {
		return err
	}
	recoveryDays := event.Table.PITR.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = 35
	}
	cutoff := changedAt - (recoveryDays * 24 * 60 * 60 * 1000)
	if cutoff <= 0 {
		return nil
	}
	_, err := h.store.CompactItemChangesBefore(ctx, tx, event.Table.Name, cutoff)
	return err
}

func pitrTypeFromMutationEventName(eventName string) string {
	switch strings.ToUpper(strings.TrimSpace(eventName)) {
	case "INSERT", "MODIFY":
		return "PUT"
	case "REMOVE":
		return "DELETE"
	default:
		return ""
	}
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
