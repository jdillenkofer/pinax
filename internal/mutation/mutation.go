package mutation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/model"
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

type Repos interface {
	Streams() uow.StreamRepo
	PITR() uow.PITRRepo
}

type Hook interface {
	HandleMutation(ctx context.Context, repos Repos, event Event) error
}

type Executor struct {
	hooks []Hook
}

func NewExecutor(hooks ...Hook) *Executor {
	hooksCopy := append([]Hook(nil), hooks...)
	return &Executor{hooks: hooksCopy}
}

func (e *Executor) Emit(ctx context.Context, repos Repos, event Event) error {
	if e == nil {
		return nil
	}
	for _, hook := range e.hooks {
		if hook == nil {
			continue
		}
		if err := hook.HandleMutation(ctx, repos, event); err != nil {
			return err
		}
	}
	return nil
}

func DefaultHooks() []Hook {
	return []Hook{NewStreamHook(), NewPITRHook()}
}

type streamHook struct{}

func NewStreamHook() Hook {
	return &streamHook{}
}

func (h *streamHook) HandleMutation(ctx context.Context, repos Repos, event Event) error {
	if !event.Table.Stream.Enabled || strings.TrimSpace(event.Table.Stream.ARN) == "" {
		return nil
	}
	oldView, newView := filterStreamImages(event.Table.Stream.ViewType, event.OldImage, event.NewImage)
	if err := repos.Streams().AppendStreamRecord(ctx, model.StreamRecord{
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
	_, err := repos.Streams().DeleteStreamRecordsBefore(ctx, event.Table.Stream.ARN, event.ChangedAt-streamRetentionMillis)
	return err
}

type pitrHook struct{}

func NewPITRHook() Hook {
	return &pitrHook{}
}

func (h *pitrHook) HandleMutation(ctx context.Context, repos Repos, event Event) error {
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
	if err := repos.PITR().AppendItemChange(ctx, event.Table.Name, pk, sk, pitrType, item, changedAt); err != nil {
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
	_, err := repos.PITR().CompactItemChangesBefore(ctx, event.Table.Name, cutoff)
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
