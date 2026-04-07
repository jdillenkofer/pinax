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
	UpdateTimeToLive(ctx context.Context, tx *sql.Tx, tableName string, ttl model.TimeToLive) error
	GetExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, ttlAttr string, before int64, limit int) ([]struct {
		PK string
		SK string
	}, error)
	DeleteExpiredItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error
}
