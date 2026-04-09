package ttl

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSweeperDeletesExpiredItemsInBatches(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	table := model.Table{
		Name:      "sweep_batch",
		HashKey:   "pk",
		HashType:  "S",
		CreatedAt: 1,
		TimeToLive: model.TimeToLive{
			Enabled:  true,
			AttrName: "ttl",
		},
	}
	if err := store.CreateTable(ctx, tx, table); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	for i := 0; i < sweepBatchSize+150; i++ {
		pk := fmt.Sprintf("e#%04d", i)
		item := map[string]any{
			"pk":  map[string]any{"S": pk},
			"ttl": map[string]any{"N": "100"},
		}
		if err := store.PutItem(ctx, tx, table.Name, "S|"+pk, model.NoSortKey, item); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}

	valid := map[string]any{
		"pk":  map[string]any{"S": "valid"},
		"ttl": map[string]any{"N": "9999999999"},
	}
	if err := store.PutItem(ctx, tx, table.Name, "S|valid", model.NoSortKey, valid); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	NewSweeper(store, 0).RunOnce(ctx)

	tx, err = store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	count, err := store.CountItems(ctx, tx, table.Name)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 non-expired item after sweep, got %d", count)
	}
	if _, err := store.GetItem(ctx, tx, table.Name, "S|valid", model.NoSortKey); err != nil {
		t.Fatal(err)
	}
}
