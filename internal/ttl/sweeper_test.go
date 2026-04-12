package ttl

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"
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

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := backend.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo := sqlite.NewTxRepo(backend, tx)

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
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	for i := 0; i < sweepBatchSize+150; i++ {
		pk := fmt.Sprintf("e#%04d", i)
		item := map[string]any{
			"pk":  map[string]any{"S": pk},
			"ttl": map[string]any{"N": "100"},
		}
		if err := repo.ItemRepo().PutItem(ctx, table, "S|"+pk, model.NoSortKey, item); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}

	valid := map[string]any{
		"pk":  map[string]any{"S": "valid"},
		"ttl": map[string]any{"N": "9999999999"},
	}
	if err := repo.ItemRepo().PutItem(ctx, table, "S|valid", model.NoSortKey, valid); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	NewSweeper(backend.DB(), sqlite.NewFactory(backend), 0, mutation.NewExecutor()).RunOnce(ctx)

	tx, err = backend.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	repo = sqlite.NewTxRepo(backend, tx)

	count, err := repo.ItemRepo().CountItems(ctx, table.Name)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 non-expired item after sweep, got %d", count)
	}
	if _, err := repo.ItemRepo().GetItem(ctx, table.Name, "S|valid", model.NoSortKey); err != nil {
		t.Fatal(err)
	}
}

func TestSweeperPrunesPITRHistoryWithoutTTL(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := backend.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	repo := sqlite.NewTxRepo(backend, tx)

	nowMs := time.Now().UnixMilli()
	table := model.Table{
		Name:      "sweep_pitr",
		HashKey:   "pk",
		HashType:  "S",
		CreatedAt: 1,
		PITR:      model.PointInTimeRecovery{Enabled: true, RecoveryPeriodInDays: 1, EnabledAt: nowMs},
	}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	oldTs := nowMs - (2 * 24 * 60 * 60 * 1000)
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|old", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "old"}}, oldTs); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|new", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "new"}}, nowMs); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	NewSweeper(backend.DB(), sqlite.NewFactory(backend), 0, mutation.NewExecutor()).RunOnce(ctx)

	tx, err = backend.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	repo = sqlite.NewTxRepo(backend, tx)

	changes, err := repo.PITRRepo().ListItemChangesUpTo(ctx, table.Name, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected old PITR history to be pruned, got %d changes", len(changes))
	}
	if changes[0].PK != "S|new" {
		t.Fatalf("expected newest PITR change to remain, got pk=%s", changes[0].PK)
	}
}
