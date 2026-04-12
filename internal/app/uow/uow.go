package uow

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

type TableRepo interface {
	CreateTable(ctx context.Context, table model.Table) error
	GetTable(ctx context.Context, tableKey string) (model.Table, error)
	ListTables(ctx context.Context, start string, limit int) ([]string, error)
	DeleteTable(ctx context.Context, tableKey string) error
	UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error
	UpdateTableBilling(ctx context.Context, tableKey string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error
	UpdateTableOptions(ctx context.Context, tableKey string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error
	UpdateTimeToLive(ctx context.Context, tableKey string, ttl model.TimeToLive) error
	UpdatePointInTimeRecovery(ctx context.Context, tableKey string, pitr model.PointInTimeRecovery) error
}

type ItemRepo interface {
	CountItems(ctx context.Context, tableKey string) (int64, error)
	GetItem(ctx context.Context, tableKey, pk, sk string) (map[string]any, error)
	PutItem(ctx context.Context, tableKey, pk, sk string, item map[string]any) error
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
	ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error)
	GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error)
	GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.PITRCheckpoint, error)
	CreatePITRCheckpointFromCurrentState(ctx context.Context, tableKey string, changedAt int64) error
	CompactItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error)
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
	PITR() PITRRepo
	Backups() BackupRepo
	ResourcePolicies() ResourcePolicyRepo
}

type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, repos Repos) error) error
}

type storeUnitOfWork struct {
	store store.Store
}

type txStoreContextKey struct{}

func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	ambient, ok := ctx.Value(txStoreContextKey{}).(*txStore)
	if !ok || ambient == nil || ambient.tx == nil {
		return nil, false
	}
	return ambient.tx, true
}

func NewStoreUnitOfWork(s store.Store) UnitOfWork {
	return &storeUnitOfWork{store: s}
}

func (u *storeUnitOfWork) Do(ctx context.Context, fn func(ctx context.Context, repos Repos) error) error {
	if ambient, ok := ctx.Value(txStoreContextKey{}).(*txStore); ok && ambient != nil {
		return fn(ctx, txRepos{txs: ambient})
	}

	tx, err := u.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txs := &txStore{store: u.store, tx: tx}
	txCtx := context.WithValue(ctx, txStoreContextKey{}, txs)
	if err := fn(txCtx, txRepos{txs: txs}); err != nil {
		return err
	}
	return tx.Commit()
}

type txRepos struct {
	txs *txStore
}

func (r txRepos) Tables() TableRepo { return r.txs }
func (r txRepos) Items() ItemRepo   { return r.txs }
func (r txRepos) PITR() PITRRepo    { return r.txs }
func (r txRepos) Backups() BackupRepo {
	return r.txs
}
func (r txRepos) ResourcePolicies() ResourcePolicyRepo { return r.txs }

type txStore struct {
	store store.Store
	tx    *sql.Tx
}

func (t *txStore) CreateTable(ctx context.Context, table model.Table) error {
	return t.store.CreateTable(ctx, t.tx, table)
}
func (t *txStore) GetTable(ctx context.Context, tableKey string) (model.Table, error) {
	return t.store.GetTable(ctx, t.tx, tableKey)
}
func (t *txStore) ListTables(ctx context.Context, start string, limit int) ([]string, error) {
	return t.store.ListTables(ctx, t.tx, start, limit)
}
func (t *txStore) DeleteTable(ctx context.Context, tableKey string) error {
	return t.store.DeleteTable(ctx, t.tx, tableKey)
}
func (t *txStore) CountItems(ctx context.Context, tableKey string) (int64, error) {
	return t.store.CountItems(ctx, t.tx, tableKey)
}
func (t *txStore) GetItem(ctx context.Context, tableKey, pk, sk string) (map[string]any, error) {
	return t.store.GetItem(ctx, t.tx, tableKey, pk, sk)
}
func (t *txStore) PutItem(ctx context.Context, tableKey, pk, sk string, item map[string]any) error {
	return t.store.PutItem(ctx, t.tx, tableKey, pk, sk, item)
}
func (t *txStore) DeleteItem(ctx context.Context, tableKey, pk, sk string) error {
	return t.store.DeleteItem(ctx, t.tx, tableKey, pk, sk)
}
func (t *txStore) QueryByPK(ctx context.Context, tableKey, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	return t.store.QueryByPK(ctx, t.tx, tableKey, pk, startSK, scanForward, limit)
}
func (t *txStore) QueryByGSI(ctx context.Context, tableKey, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	return t.store.QueryByGSI(ctx, t.tx, tableKey, indexName, pk, startSK, scanForward, limit)
}
func (t *txStore) QueryByPKSK(ctx context.Context, tableKey, pk, sk string) ([]map[string]any, error) {
	return t.store.QueryByPKSK(ctx, t.tx, tableKey, pk, sk)
}
func (t *txStore) Scan(ctx context.Context, tableKey, startPK, startSK string, limit int) ([]map[string]any, error) {
	return t.store.Scan(ctx, t.tx, tableKey, startPK, startSK, limit)
}
func (t *txStore) GetTransactWriteIdempotency(ctx context.Context, token string, now int64) (model.TransactWriteIdempotencyRecord, error) {
	return t.store.GetTransactWriteIdempotency(ctx, t.tx, token, now)
}
func (t *txStore) PutTransactWriteIdempotency(ctx context.Context, record model.TransactWriteIdempotencyRecord) error {
	return t.store.PutTransactWriteIdempotency(ctx, t.tx, record)
}
func (t *txStore) DeleteExpiredTransactWriteIdempotency(ctx context.Context, now int64) error {
	return t.store.DeleteExpiredTransactWriteIdempotency(ctx, t.tx, now)
}
func (t *txStore) UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	return t.store.UpdateTableIndexes(ctx, t.tx, tableKey, tableStatus, tableStatusAt, gsis, lsis)
}
func (t *txStore) UpdateTableBilling(ctx context.Context, tableKey string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error {
	return t.store.UpdateTableBilling(ctx, t.tx, tableKey, billingMode, readCapacityUnits, writeCapacityUnits)
}
func (t *txStore) UpdateTableOptions(ctx context.Context, tableKey string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error {
	return t.store.UpdateTableOptions(ctx, t.tx, tableKey, tableClass, deletionProtection, stream, sse, tags)
}
func (t *txStore) UpdateTimeToLive(ctx context.Context, tableKey string, ttl model.TimeToLive) error {
	return t.store.UpdateTimeToLive(ctx, t.tx, tableKey, ttl)
}
func (t *txStore) UpdatePointInTimeRecovery(ctx context.Context, tableKey string, pitr model.PointInTimeRecovery) error {
	return t.store.UpdatePointInTimeRecovery(ctx, t.tx, tableKey, pitr)
}
func (t *txStore) AppendItemChange(ctx context.Context, tableKey, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	return t.store.AppendItemChange(ctx, t.tx, tableKey, pk, sk, changeType, item, changedAt)
}
func (t *txStore) ResolveItemChangeCursorAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.ItemChangeCursor, error) {
	return t.store.ResolveItemChangeCursorAtOrBefore(ctx, t.tx, tableKey, upTo)
}
func (t *txStore) ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error) {
	return t.store.ListItemChangesAfterCursorUpToCursor(ctx, t.tx, tableKey, after, upTo)
}
func (t *txStore) GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	return t.store.GetLatestPITRCheckpointAtOrBeforeCursor(ctx, t.tx, tableKey, cursor)
}
func (t *txStore) GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.PITRCheckpoint, error) {
	return t.store.GetLatestPITRCheckpointAtOrBefore(ctx, t.tx, tableKey, upTo)
}
func (t *txStore) CreatePITRCheckpointFromCurrentState(ctx context.Context, tableKey string, changedAt int64) error {
	return t.store.CreatePITRCheckpointFromCurrentState(ctx, t.tx, tableKey, changedAt)
}
func (t *txStore) CompactItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error) {
	return t.store.CompactItemChangesBefore(ctx, t.tx, tableKey, before)
}
func (t *txStore) CreateBackup(ctx context.Context, backup model.Backup) error {
	return t.store.CreateBackup(ctx, t.tx, backup)
}
func (t *txStore) GetBackup(ctx context.Context, backupARN string) (model.Backup, error) {
	return t.store.GetBackup(ctx, t.tx, backupARN)
}
func (t *txStore) GetBackupByName(ctx context.Context, backupName string) (model.Backup, error) {
	return t.store.GetBackupByName(ctx, t.tx, backupName)
}
func (t *txStore) ListBackups(ctx context.Context) ([]model.Backup, error) {
	return t.store.ListBackups(ctx, t.tx)
}
func (t *txStore) DeleteBackup(ctx context.Context, backupARN string) error {
	return t.store.DeleteBackup(ctx, t.tx, backupARN)
}
func (t *txStore) PutResourcePolicy(ctx context.Context, resourceARN string, policy string, revisionID string, updatedAt int64) error {
	return t.store.PutResourcePolicy(ctx, t.tx, resourceARN, policy, revisionID, updatedAt)
}
func (t *txStore) GetResourcePolicy(ctx context.Context, resourceARN string) (string, string, error) {
	return t.store.GetResourcePolicy(ctx, t.tx, resourceARN)
}
func (t *txStore) DeleteResourcePolicy(ctx context.Context, resourceARN string) (string, bool, error) {
	return t.store.DeleteResourcePolicy(ctx, t.tx, resourceARN)
}
