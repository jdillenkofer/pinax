package uow

import (
	"context"

	"github.com/jdillenkofer/pinax/internal/model"
)

type TableRepo interface {
	CreateTable(ctx context.Context, table model.Table) error
	GetTable(ctx context.Context, tableKey string) (model.Table, error)
	ListTables(ctx context.Context, start string, limit int) ([]string, error)
	DeleteTable(ctx context.Context, tableKey string) error
	DeleteGSIEntries(ctx context.Context, tableKey, indexName string) error
	PutSecondaryIndexEntry(ctx context.Context, tableKey, indexName, indexPK, indexSK, basePK, baseSK string) error
	UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error
	UpdateTableBilling(ctx context.Context, tableKey string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error
	UpdateTableOptions(ctx context.Context, tableKey string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error
	UpdateTimeToLive(ctx context.Context, tableKey string, ttl model.TimeToLive) error
	UpdatePointInTimeRecovery(ctx context.Context, tableKey string, pitr model.PointInTimeRecovery) error
}

type StreamRepo interface {
	AppendStreamRecord(ctx context.Context, record model.StreamRecord) error
	ListStreamRecordsAfterSequence(ctx context.Context, streamARN string, sequence int64, limit int) ([]model.StreamRecord, error)
	GetStreamSequenceBounds(ctx context.Context, streamARN string) (int64, int64, bool, error)
	GetStreamRecordChangedAt(ctx context.Context, streamARN string, sequence int64) (int64, bool, error)
	DeleteStreamRecordsBefore(ctx context.Context, streamARN string, before int64) (int64, error)
}

type ItemRepo interface {
	CountItems(ctx context.Context, tableKey string) (int64, error)
	GetItem(ctx context.Context, tableKey, pk, sk string) (map[string]any, error)
	PutItem(ctx context.Context, t model.Table, pk, sk string, item map[string]any) error
	DeleteItem(ctx context.Context, tableKey, pk, sk string) error
	QueryByPK(ctx context.Context, tableKey, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByGSI(ctx context.Context, tableKey, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByPKSK(ctx context.Context, tableKey, pk, sk string) ([]map[string]any, error)
	Scan(ctx context.Context, tableKey, startPK, startSK string, limit int) ([]map[string]any, error)
	GetTransactWriteIdempotency(ctx context.Context, token string, now int64) (model.TransactWriteIdempotencyRecord, error)
	PutTransactWriteIdempotency(ctx context.Context, record model.TransactWriteIdempotencyRecord) error
	DeleteExpiredTransactWriteIdempotency(ctx context.Context, now int64) error
}

type PITRRepo interface {
	AppendItemChange(ctx context.Context, tableKey, pk, sk, changeType string, item map[string]any, changedAt int64) error
	ResolveItemChangeCursorAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.ItemChangeCursor, error)
	ListItemChangesUpTo(ctx context.Context, tableKey string, upTo int64) ([]model.ItemChange, error)
	ListItemChangesUpToCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) ([]model.ItemChange, error)
	ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error)
	GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error)
	GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.PITRCheckpoint, error)
	CreatePITRCheckpointFromCurrentState(ctx context.Context, tableKey string, changedAt int64) error
	CompactItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error)
	DeleteItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error)
}

type BackupRepo interface {
	CreateBackup(ctx context.Context, backup model.Backup) error
	GetBackup(ctx context.Context, backupARN string) (model.Backup, error)
	GetBackupByName(ctx context.Context, backupName string) (model.Backup, error)
	ListBackups(ctx context.Context) ([]model.Backup, error)
	DeleteBackup(ctx context.Context, backupARN string) error
}

type ResourcePolicyRepo interface {
	PutResourcePolicy(ctx context.Context, resourceARN string, policy string, revisionID string, updatedAt int64) error
	GetResourcePolicy(ctx context.Context, resourceARN string) (string, string, error)
	DeleteResourcePolicy(ctx context.Context, resourceARN string) (string, bool, error)
}

type Repos interface {
	Tables() TableRepo
	Items() ItemRepo
	Streams() StreamRepo
	PITR() PITRRepo
	Backups() BackupRepo
	ResourcePolicies() ResourcePolicyRepo
}

type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, repos Repos) error) error
}
