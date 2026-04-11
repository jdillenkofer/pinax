package store

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
)

type loggingStore struct {
	Store
}

func NewLoggingStore(base Store) Store {
	if base == nil {
		return nil
	}
	return &loggingStore{Store: base}
}

func (s *loggingStore) CreateTable(ctx context.Context, tx *sql.Tx, table model.Table) error {
	start := time.Now()
	err := s.Store.CreateTable(ctx, tx, table)
	s.logResult(ctx, "CreateTable", err, "tableKey", table.Name, "durationMs", time.Since(start).Milliseconds())
	return err
}

func (s *loggingStore) GetTable(ctx context.Context, tx *sql.Tx, tableKey string) (model.Table, error) {
	start := time.Now()
	t, err := s.Store.GetTable(ctx, tx, tableKey)
	s.logResult(ctx, "GetTable", err, "tableKey", tableKey, "durationMs", time.Since(start).Milliseconds())
	return t, err
}

func (s *loggingStore) ListTables(ctx context.Context, tx *sql.Tx, startName string, limit int) ([]string, error) {
	start := time.Now()
	out, err := s.Store.ListTables(ctx, tx, startName, limit)
	attrs := []any{"start", startName, "limit", limit, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "count", len(out))
	}
	s.logResult(ctx, "ListTables", err, attrs...)
	return out, err
}

func (s *loggingStore) DeleteTable(ctx context.Context, tx *sql.Tx, tableKey string) error {
	start := time.Now()
	err := s.Store.DeleteTable(ctx, tx, tableKey)
	s.logResult(ctx, "DeleteTable", err, "tableKey", tableKey, "durationMs", time.Since(start).Milliseconds())
	return err
}

func (s *loggingStore) PutItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string, item map[string]any) error {
	start := time.Now()
	err := s.Store.PutItem(ctx, tx, tableKey, pk, sk, item)
	s.logResult(ctx, "PutItem", err, "tableKey", tableKey, "pk", pk, "sk", sk, "durationMs", time.Since(start).Milliseconds())
	return err
}

func (s *loggingStore) GetItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) (map[string]any, error) {
	start := time.Now()
	item, err := s.Store.GetItem(ctx, tx, tableKey, pk, sk)
	attrs := []any{"tableKey", tableKey, "pk", pk, "sk", sk, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "found", item != nil)
	}
	s.logResult(ctx, "GetItem", err, attrs...)
	return item, err
}

func (s *loggingStore) DeleteItem(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) error {
	start := time.Now()
	err := s.Store.DeleteItem(ctx, tx, tableKey, pk, sk)
	s.logResult(ctx, "DeleteItem", err, "tableKey", tableKey, "pk", pk, "sk", sk, "durationMs", time.Since(start).Milliseconds())
	return err
}

func (s *loggingStore) QueryByPK(ctx context.Context, tx *sql.Tx, tableKey, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	start := time.Now()
	out, err := s.Store.QueryByPK(ctx, tx, tableKey, pk, startSK, scanForward, limit)
	attrs := []any{"tableKey", tableKey, "pk", pk, "startSK", startSK, "scanForward", scanForward, "limit", limit, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "count", len(out))
	}
	s.logResult(ctx, "QueryByPK", err, attrs...)
	return out, err
}

func (s *loggingStore) QueryByGSI(ctx context.Context, tx *sql.Tx, tableKey, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	start := time.Now()
	out, err := s.Store.QueryByGSI(ctx, tx, tableKey, indexName, pk, startSK, scanForward, limit)
	attrs := []any{"tableKey", tableKey, "index", indexName, "pk", pk, "startSK", startSK, "scanForward", scanForward, "limit", limit, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "count", len(out))
	}
	s.logResult(ctx, "QueryByGSI", err, attrs...)
	return out, err
}

func (s *loggingStore) QueryByPKSK(ctx context.Context, tx *sql.Tx, tableKey, pk, sk string) ([]map[string]any, error) {
	start := time.Now()
	out, err := s.Store.QueryByPKSK(ctx, tx, tableKey, pk, sk)
	attrs := []any{"tableKey", tableKey, "pk", pk, "sk", sk, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "count", len(out))
	}
	s.logResult(ctx, "QueryByPKSK", err, attrs...)
	return out, err
}

func (s *loggingStore) Scan(ctx context.Context, tx *sql.Tx, tableKey, startPK, startSK string, limit int) ([]map[string]any, error) {
	start := time.Now()
	out, err := s.Store.Scan(ctx, tx, tableKey, startPK, startSK, limit)
	attrs := []any{"tableKey", tableKey, "startPK", startPK, "startSK", startSK, "limit", limit, "durationMs", time.Since(start).Milliseconds()}
	if err == nil {
		attrs = append(attrs, "count", len(out))
	}
	s.logResult(ctx, "Scan", err, attrs...)
	return out, err
}

func (s *loggingStore) logResult(ctx context.Context, operation string, err error, attrs ...any) {
	if err != nil {
		slog.WarnContext(ctx, "store operation failed", append([]any{"operation", operation, "err", err}, attrs...)...)
		return
	}
	slog.DebugContext(ctx, "store operation", append([]any{"operation", operation}, attrs...)...)
}
