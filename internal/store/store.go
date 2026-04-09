package store

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/model"
)

type Store interface {
	DB() *sql.DB
	CreateTable(ctx context.Context, tx *sql.Tx, table model.Table) error
	GetTable(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error)
	ListTables(ctx context.Context, tx *sql.Tx, start string, limit int) ([]string, error)
	DeleteTable(ctx context.Context, tx *sql.Tx, tableName string) error
	CountItems(ctx context.Context, tx *sql.Tx, tableName string) (int64, error)
	GetItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) (map[string]any, error)
	PutItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string, item map[string]any) error
	DeleteItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error
	QueryByPK(ctx context.Context, tx *sql.Tx, tableName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByGSI(ctx context.Context, tx *sql.Tx, tableName, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByPKSK(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) ([]map[string]any, error)
	Scan(ctx context.Context, tx *sql.Tx, tableName, startPK, startSK string, limit int) ([]map[string]any, error)
	UpdateTableIndexes(ctx context.Context, tx *sql.Tx, tableName string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error
	UpdateTableBilling(ctx context.Context, tx *sql.Tx, tableName string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error
	UpdateTableOptions(ctx context.Context, tx *sql.Tx, tableName string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error
	UpdateTimeToLive(ctx context.Context, tx *sql.Tx, tableName string, ttl model.TimeToLive) error
	UpdatePointInTimeRecovery(ctx context.Context, tx *sql.Tx, tableName string, pitr model.PointInTimeRecovery) error
	AppendItemChange(ctx context.Context, tx *sql.Tx, tableName, pk, sk, changeType string, item map[string]any, changedAt int64) error
	ListItemChangesUpTo(ctx context.Context, tx *sql.Tx, tableName string, upTo int64) ([]model.ItemChange, error)
	ResolveItemChangeCursorAtOrBefore(ctx context.Context, tx *sql.Tx, tableName string, upTo int64) (model.ItemChangeCursor, error)
	ListItemChangesUpToCursor(ctx context.Context, tx *sql.Tx, tableName string, cursor model.ItemChangeCursor) ([]model.ItemChange, error)
	DeleteItemChangesBefore(ctx context.Context, tx *sql.Tx, tableName string, before int64) (int64, error)
	GetTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, token string, now int64) (model.TransactWriteIdempotencyRecord, error)
	PutTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, record model.TransactWriteIdempotencyRecord) error
	DeleteExpiredTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, now int64) error
	GetExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, ttlAttr string, before int64, limit int) ([]struct {
		PK string
		SK string
	}, error)
	DeleteExpiredItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error
	DeleteExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, before int64, limit int) (int64, error)
	CreateBackup(ctx context.Context, tx *sql.Tx, backup model.Backup) error
	GetBackup(ctx context.Context, tx *sql.Tx, backupARN string) (model.Backup, error)
	GetBackupByName(ctx context.Context, tx *sql.Tx, backupName string) (model.Backup, error)
	ListBackups(ctx context.Context, tx *sql.Tx) ([]model.Backup, error)
	DeleteBackup(ctx context.Context, tx *sql.Tx, backupARN string) error
}
