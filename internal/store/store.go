package store

import (
	"context"

	"github.com/jdillenkofer/pinax/internal/model"
)

type Store interface {
	CreateTable(ctx context.Context, table model.Table) error
	GetTable(ctx context.Context, tableName string) (model.Table, error)
	ListTables(ctx context.Context, start string, limit int) ([]string, error)
	DeleteTable(ctx context.Context, tableName string) error
	CountItems(ctx context.Context, tableName string) (int64, error)
	GetItem(ctx context.Context, tableName, pk, sk string) (map[string]any, error)
	PutItem(ctx context.Context, tableName, pk, sk string, item map[string]any) error
	DeleteItem(ctx context.Context, tableName, pk, sk string) error
	QueryByPK(ctx context.Context, tableName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error)
	QueryByPKSK(ctx context.Context, tableName, pk, sk string) ([]map[string]any, error)
	Scan(ctx context.Context, tableName, startPK, startSK string, limit int) ([]map[string]any, error)
}
