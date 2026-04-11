package store

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/model"
)

type Store interface {
	DB() *sql.DB
	CreateTable(ctx context.Context, tx *sql.Tx, table model.Table) error
	GetTable(ctx context.Context, tx *sql.Tx, tableKey string) (model.Table, error)
	ListTables(ctx context.Context, tx *sql.Tx, start string, limit int) ([]string, error)
	DeleteTable(ctx context.Context, tx *sql.Tx, tableKey string) error
	CountItems(ctx context.Context, tx *sql.Tx, tableKey string) (int64, error)
	GetItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) (map[string]any, error)
	PutItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string, item map[string]any) error
	DeleteItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) error
	QueryByPK(ctx context.Context, tx *sql.Tx, tableKey, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByGSI(ctx context.Context, tx *sql.Tx, tableKey, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByPKSK(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) ([]map[string]any, error)
	Scan(ctx context.Context, tx *sql.Tx, tableKey, startPK, startSK string, limit int) ([]map[string]any, error)
	UpdateTableIndexes(ctx context.Context, tx *sql.Tx, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error
	UpdateTableBilling(ctx context.Context, tx *sql.Tx, tableKey string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error
	UpdateTableOptions(ctx context.Context, tx *sql.Tx, tableKey string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error
	UpdateTimeToLive(ctx context.Context, tx *sql.Tx, tableKey string, ttl model.TimeToLive) error
	UpdatePointInTimeRecovery(ctx context.Context, tx *sql.Tx, tableKey string, pitr model.PointInTimeRecovery) error
	AppendItemChange(ctx context.Context, tx *sql.Tx, tableKey, pk, sk, changeType string, item map[string]any, changedAt int64) error
	ListItemChangesUpTo(ctx context.Context, tx *sql.Tx, tableKey string, upTo int64) ([]model.ItemChange, error)
	ResolveItemChangeCursorAtOrBefore(ctx context.Context, tx *sql.Tx, tableKey string, upTo int64) (model.ItemChangeCursor, error)
	ListItemChangesUpToCursor(ctx context.Context, tx *sql.Tx, tableKey string, cursor model.ItemChangeCursor) ([]model.ItemChange, error)
	ListItemChangesAfterCursorUpToCursor(ctx context.Context, tx *sql.Tx, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error)
	GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tx *sql.Tx, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error)
	GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tx *sql.Tx, tableKey string, upTo int64) (model.PITRCheckpoint, error)
	CreatePITRCheckpointFromCurrentState(ctx context.Context, tx *sql.Tx, tableKey string, changedAt int64) error
	CompactItemChangesBefore(ctx context.Context, tx *sql.Tx, tableKey string, before int64) (int64, error)
	DeleteItemChangesBefore(ctx context.Context, tx *sql.Tx, tableKey string, before int64) (int64, error)
	GetTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, token string, now int64) (model.TransactWriteIdempotencyRecord, error)
	PutTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, record model.TransactWriteIdempotencyRecord) error
	DeleteExpiredTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, now int64) error
	GetExpiredItems(ctx context.Context, tx *sql.Tx, tableKey string, ttlAttr string, before int64, limit int) ([]struct {
		PK string
		SK string
	}, error)
	DeleteExpiredItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) error
	DeleteExpiredItems(ctx context.Context, tx *sql.Tx, tableKey string, before int64, limit int) (int64, error)
	CreateBackup(ctx context.Context, tx *sql.Tx, backup model.Backup) error
	GetBackup(ctx context.Context, tx *sql.Tx, backupARN string) (model.Backup, error)
	GetBackupByName(ctx context.Context, tx *sql.Tx, backupName string) (model.Backup, error)
	ListBackups(ctx context.Context, tx *sql.Tx) ([]model.Backup, error)
	DeleteBackup(ctx context.Context, tx *sql.Tx, backupARN string) error
	AppendStreamRecord(ctx context.Context, tx *sql.Tx, record model.StreamRecord) error
	ListStreamRecordsAfterSequence(ctx context.Context, tx *sql.Tx, streamARN string, sequence int64, limit int) ([]model.StreamRecord, error)
	GetStreamSequenceBounds(ctx context.Context, tx *sql.Tx, streamARN string) (int64, int64, bool, error)
	GetStreamRecordChangedAt(ctx context.Context, tx *sql.Tx, streamARN string, sequence int64) (int64, bool, error)
	DeleteStreamRecordsBefore(ctx context.Context, tx *sql.Tx, streamARN string, before int64) (int64, error)
	PutResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string, policy string, revisionID string, updatedAt int64) error
	GetResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string) (string, string, error)
	DeleteResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string) (string, bool, error)
}
