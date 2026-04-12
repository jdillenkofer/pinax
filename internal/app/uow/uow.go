package uow

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

type TxStore interface {
	CreateTable(ctx context.Context, table model.Table) error
	GetTable(ctx context.Context, tableKey string) (model.Table, error)
	DeleteTable(ctx context.Context, tableKey string) error
	CountItems(ctx context.Context, tableKey string) (int64, error)
	PutItem(ctx context.Context, tableKey, pk, sk string, item map[string]any) error
	Scan(ctx context.Context, tableKey, startPK, startSK string, limit int) ([]map[string]any, error)
	UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error
	UpdatePointInTimeRecovery(ctx context.Context, tableKey string, pitr model.PointInTimeRecovery) error
	AppendItemChange(ctx context.Context, tableKey, pk, sk, changeType string, item map[string]any, changedAt int64) error
	ResolveItemChangeCursorAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.ItemChangeCursor, error)
	ListItemChangesAfterCursorUpToCursor(ctx context.Context, tableKey string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error)
	GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tableKey string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error)
	GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tableKey string, upTo int64) (model.PITRCheckpoint, error)
	CreatePITRCheckpointFromCurrentState(ctx context.Context, tableKey string, changedAt int64) error
	CompactItemChangesBefore(ctx context.Context, tableKey string, before int64) (int64, error)
	CreateBackup(ctx context.Context, backup model.Backup) error
	GetBackup(ctx context.Context, backupARN string) (model.Backup, error)
	GetBackupByName(ctx context.Context, backupName string) (model.Backup, error)
	ListBackups(ctx context.Context) ([]model.Backup, error)
	DeleteBackup(ctx context.Context, backupARN string) error
}

type UnitOfWork interface {
	Do(ctx context.Context, fn func(txs TxStore) error) error
}

type storeUnitOfWork struct {
	store store.Store
}

func NewStoreUnitOfWork(s store.Store) UnitOfWork {
	return &storeUnitOfWork{store: s}
}

func (u *storeUnitOfWork) Do(ctx context.Context, fn func(txs TxStore) error) error {
	tx, err := u.store.DB().BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txs := &txStore{store: u.store, tx: tx}
	if err := fn(txs); err != nil {
		return err
	}
	return tx.Commit()
}

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
func (t *txStore) DeleteTable(ctx context.Context, tableKey string) error {
	return t.store.DeleteTable(ctx, t.tx, tableKey)
}
func (t *txStore) CountItems(ctx context.Context, tableKey string) (int64, error) {
	return t.store.CountItems(ctx, t.tx, tableKey)
}
func (t *txStore) PutItem(ctx context.Context, tableKey, pk, sk string, item map[string]any) error {
	return t.store.PutItem(ctx, t.tx, tableKey, pk, sk, item)
}
func (t *txStore) Scan(ctx context.Context, tableKey, startPK, startSK string, limit int) ([]map[string]any, error) {
	return t.store.Scan(ctx, t.tx, tableKey, startPK, startSK, limit)
}
func (t *txStore) UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	return t.store.UpdateTableIndexes(ctx, t.tx, tableKey, tableStatus, tableStatusAt, gsis, lsis)
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
