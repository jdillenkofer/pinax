package mutation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
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

func DefaultHooks(s store.Store) []Hook {
	return []Hook{NewStreamHook(s), NewPITRHook(s)}
}

type streamHook struct{}

func NewStreamHook(s store.Store) Hook {
	if s == nil {
		return nil
	}
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

func NewPITRHook(s store.Store) Hook {
	if s == nil {
		return nil
	}
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

type txRepos struct {
	store store.Store
	tx    *sql.Tx
}

type txStreamRepo struct {
	store store.Store
	tx    *sql.Tx
}

type txPITRRepo struct {
	store store.Store
	tx    *sql.Tx
}

func NewTxRepos(s store.Store, tx *sql.Tx) Repos {
	return txRepos{store: s, tx: tx}
}

func (r txRepos) Streams() uow.StreamRepo {
	return txStreamRepo{store: r.store, tx: r.tx}
}

func (r txRepos) PITR() uow.PITRRepo {
	return txPITRRepo{store: r.store, tx: r.tx}
}

func (r txStreamRepo) AppendStreamRecord(ctx context.Context, record model.StreamRecord) error {
	return r.store.AppendStreamRecord(ctx, r.tx, record)
}

func (r txStreamRepo) ListStreamRecordsAfterSequence(ctx context.Context, streamARN string, sequence int64, limit int) ([]model.StreamRecord, error) {
	return r.store.ListStreamRecordsAfterSequence(ctx, r.tx, streamARN, sequence, limit)
}

func (r txStreamRepo) GetStreamSequenceBounds(ctx context.Context, streamARN string) (int64, int64, bool, error) {
	return r.store.GetStreamSequenceBounds(ctx, r.tx, streamARN)
}

func (r txStreamRepo) GetStreamRecordChangedAt(ctx context.Context, streamARN string, sequence int64) (int64, bool, error) {
	return r.store.GetStreamRecordChangedAt(ctx, r.tx, streamARN, sequence)
}

func (r txStreamRepo) DeleteStreamRecordsBefore(ctx context.Context, streamARN string, before int64) (int64, error) {
	return r.store.DeleteStreamRecordsBefore(ctx, r.tx, streamARN, before)
}

func (r txPITRRepo) AppendItemChange(ctx context.Context, tableKey, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	return r.store.AppendItemChange(ctx, r.tx, tableKey, pk, sk, changeType, item, changedAt)
}

func (r txPITRRepo) ResolveItemChangeCursorAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.ItemChangeCursor, error) {
	return r.store.ResolveItemChangeCursorAtOrBefore(ctx, r.tx, tableKey, upTo)
}

func (r txPITRRepo) ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error) {
	return r.store.ListItemChangesAfterCursorUpToCursor(ctx, r.tx, tableKey, after, upTo)
}

func (r txPITRRepo) GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	return r.store.GetLatestPITRCheckpointAtOrBeforeCursor(ctx, r.tx, tableKey, cursor)
}

func (r txPITRRepo) GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.PITRCheckpoint, error) {
	return r.store.GetLatestPITRCheckpointAtOrBefore(ctx, r.tx, tableKey, upTo)
}

func (r txPITRRepo) CreatePITRCheckpointFromCurrentState(ctx context.Context, tableKey string, changedAt int64) error {
	return r.store.CreatePITRCheckpointFromCurrentState(ctx, r.tx, tableKey, changedAt)
}

func (r txPITRRepo) CompactItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error) {
	return r.store.CompactItemChangesBefore(ctx, r.tx, tableKey, before)
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
