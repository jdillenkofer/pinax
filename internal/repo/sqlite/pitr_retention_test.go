package sqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"

	_ "github.com/mattn/go-sqlite3"
)

func TestDeleteItemChangesBeforePrunesOlderHistory(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewTxRepo(s, tx)
	defer tx.Rollback()

	table := model.Table{Name: "retention_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "old"}}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "mid"}}, 2000); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "new"}}, 3000); err != nil {
		t.Fatal(err)
	}

	deleted, err := repo.PITRRepo().DeleteItemChangesBefore(ctx, table.Name, 2500)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted history rows, got %d", deleted)
	}

	changes, err := repo.PITRRepo().ListItemChangesUpTo(ctx, table.Name, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 remaining history row, got %d", len(changes))
	}
	if changes[0].ChangedAt != 3000 {
		t.Fatalf("expected remaining change at 3000, got %d", changes[0].ChangedAt)
	}
}

func TestPutItemDoesNotWritePITRHistoryInline(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, err := New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo := NewTxRepo(s, tx)
	defer tx.Rollback()

	nowMs := time.Now().UnixMilli()
	table := model.Table{
		Name:      "retention_on_put",
		HashKey:   "pk",
		HashType:  "S",
		CreatedAt: 1,
		PITR:      model.PointInTimeRecovery{Enabled: true, RecoveryPeriodInDays: 1},
	}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	oldTs := nowMs - (2 * 24 * 60 * 60 * 1000)
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "old"}}, oldTs); err != nil {
		t.Fatal(err)
	}

	item := map[string]any{
		"pk": map[string]any{"S": "a"},
		"v":  map[string]any{"S": "new"},
	}
	if err := repo.ItemRepo().PutItem(ctx, table, "S|a", model.NoSortKey, item); err != nil {
		t.Fatal(err)
	}

	changes, err := repo.PITRRepo().ListItemChangesUpTo(ctx, table.Name, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected no inline PITR history append on PutItem and existing history preserved, got %d rows", len(changes))
	}
	if changes[0].ChangedAt != oldTs {
		t.Fatalf("expected existing PITR history row to remain unchanged, got %d", changes[0].ChangedAt)
	}
}
