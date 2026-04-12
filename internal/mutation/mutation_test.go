package mutation

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestPITRHookAppendsAndPrunesHistory(t *testing.T) {
	testutils.SkipIfIntegration(t)

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	repo := sqlite.NewTxRepo(s, tx)

	nowMs := time.Now().UnixMilli()
	table := model.Table{
		Name:      "hook_pitr",
		HashKey:   "pk",
		HashType:  "S",
		CreatedAt: 1,
		PITR:      model.PointInTimeRecovery{Enabled: true, RecoveryPeriodInDays: 1},
	}
	if err := repo.CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	oldTs := nowMs - (2 * 24 * 60 * 60 * 1000)
	if err := repo.AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "old"}}, oldTs); err != nil {
		t.Fatal(err)
	}

	h := NewPITRHook()
	if err := h.HandleMutation(ctx, sqlite.NewRepos(s, tx), Event{
		Table:     table,
		EventName: "MODIFY",
		PK:        "S|a",
		SK:        model.NoSortKey,
		NewImage:  map[string]any{"pk": map[string]any{"S": "a"}, "v": map[string]any{"S": "new"}},
		ChangedAt: nowMs,
	}); err != nil {
		t.Fatal(err)
	}

	changes, err := repo.ListItemChangesUpTo(ctx, table.Name, nowMs)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected old row pruned and new row appended, got %d", len(changes))
	}
	if changes[0].ChangedAt != nowMs {
		t.Fatalf("expected newest mutation at %d, got %d", nowMs, changes[0].ChangedAt)
	}
}
