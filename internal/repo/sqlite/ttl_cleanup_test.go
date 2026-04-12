package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pinax/internal/model"

	_ "github.com/mattn/go-sqlite3"
)

func TestDeleteExpiredItemsBatched(t *testing.T) {
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

	table := model.Table{
		Name:      "ttl_cleanup",
		HashKey:   "pk",
		HashType:  "S",
		CreatedAt: 1,
		TimeToLive: model.TimeToLive{
			Enabled:  true,
			AttrName: "ttl",
		},
	}
	if err := repo.CreateTable(ctx, table); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	for i := 0; i < 250; i++ {
		item := map[string]any{
			"pk":  map[string]any{"S": fmt.Sprintf("e#%03d", i)},
			"ttl": map[string]any{"N": "100"},
		}
		if err := repo.PutItem(ctx, table.Name, "S|"+fmt.Sprintf("e#%03d", i), model.NoSortKey, item); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	validItem := map[string]any{
		"pk":  map[string]any{"S": "valid"},
		"ttl": map[string]any{"N": "9999999999"},
	}
	if err := repo.PutItem(ctx, table.Name, "S|valid", model.NoSortKey, validItem); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo = NewTxRepo(s, tx)
	defer tx.Rollback()

	deleted1, err := repo.DeleteExpiredItems(ctx, table.Name, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	deleted2, err := repo.DeleteExpiredItems(ctx, table.Name, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}
	deleted3, err := repo.DeleteExpiredItems(ctx, table.Name, 1000, 100)
	if err != nil {
		t.Fatal(err)
	}

	if deleted1 != 100 || deleted2 != 100 || deleted3 != 50 {
		t.Fatalf("unexpected batch deletes: got (%d, %d, %d)", deleted1, deleted2, deleted3)
	}

	valid, err := repo.GetItem(ctx, table.Name, "S|valid", model.NoSortKey)
	if err != nil {
		t.Fatal(err)
	}
	if valid == nil {
		t.Fatal("expected non-expired item to remain")
	}
}
