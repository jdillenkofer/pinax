package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/jdillenkofer/pinax/internal/model"

	_ "github.com/mattn/go-sqlite3"
)

func TestCompactItemChangesBeforeCreatesCheckpointAndPreservesRebuild(t *testing.T) {
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

	table := model.Table{Name: "checkpoint_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "a1"}}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|b", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "b1"}}, 1200); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|b", model.NoSortKey, "DELETE", nil, 1300); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|c", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "c1"}}, 1600); err != nil {
		t.Fatal(err)
	}

	deleted, err := repo.PITRRepo().CompactItemChangesBefore(ctx, table.Name, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 3 {
		t.Fatalf("expected 3 rows deleted before cutoff, got %d", deleted)
	}

	boundaryCursor, err := repo.PITRRepo().ResolveItemChangeCursorAtOrBefore(ctx, table.Name, 1500)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err := repo.PITRRepo().GetLatestPITRCheckpointAtOrBefore(ctx, table.Name, 1500)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Found {
		t.Fatal("expected checkpoint to exist after compaction")
	}

	boundaryChanges, err := repo.PITRRepo().ListItemChangesAfterCursorUpToCursor(ctx, table.Name, model.ItemChangeCursor{Found: checkpoint.Found, ChangedAt: checkpoint.ChangedAt, Sequence: checkpoint.Sequence}, boundaryCursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(boundaryChanges) != 0 {
		t.Fatalf("expected no post-checkpoint changes up to boundary, got %d", len(boundaryChanges))
	}

	boundaryState := rebuildFromCheckpointAndChanges(checkpoint, boundaryChanges)
	if len(boundaryState) != 1 {
		t.Fatalf("expected 1 item at boundary state, got %d", len(boundaryState))
	}
	if _, ok := boundaryState["S|a\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected key S|a to exist at boundary state, got keys=%v", keysOf(boundaryState))
	}

	laterCursor, err := repo.PITRRepo().ResolveItemChangeCursorAtOrBefore(ctx, table.Name, 2000)
	if err != nil {
		t.Fatal(err)
	}
	laterCheckpoint, err := repo.PITRRepo().GetLatestPITRCheckpointAtOrBefore(ctx, table.Name, 2000)
	if err != nil {
		t.Fatal(err)
	}
	laterChanges, err := repo.PITRRepo().ListItemChangesAfterCursorUpToCursor(ctx, table.Name, model.ItemChangeCursor{Found: laterCheckpoint.Found, ChangedAt: laterCheckpoint.ChangedAt, Sequence: laterCheckpoint.Sequence}, laterCursor)
	if err != nil {
		t.Fatal(err)
	}
	laterState := rebuildFromCheckpointAndChanges(laterCheckpoint, laterChanges)
	if len(laterState) != 2 {
		t.Fatalf("expected 2 items at later state, got %d", len(laterState))
	}
	if _, ok := laterState["S|a\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected key S|a to exist at later state, got keys=%v", keysOf(laterState))
	}
	if _, ok := laterState["S|c\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected key S|c to exist at later state, got keys=%v", keysOf(laterState))
	}
}

func TestCreatePITRCheckpointFromCurrentStateBootstrapsWithoutHistory(t *testing.T) {
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

	table := model.Table{Name: "bootstrap_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	if err := repo.ItemRepo().PutItem(ctx, table.Name, "S|a", model.NoSortKey, map[string]any{"pk": map[string]any{"S": "a"}, "v": map[string]any{"S": "1"}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.ItemRepo().PutItem(ctx, table.Name, "S|b", model.NoSortKey, map[string]any{"pk": map[string]any{"S": "b"}, "v": map[string]any{"S": "2"}}); err != nil {
		t.Fatal(err)
	}

	if err := repo.PITRRepo().CreatePITRCheckpointFromCurrentState(ctx, table.Name, 5000); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := repo.PITRRepo().GetLatestPITRCheckpointAtOrBefore(ctx, table.Name, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Found {
		t.Fatal("expected bootstrap checkpoint to be created")
	}
	if len(checkpoint.Items) != 2 {
		t.Fatalf("expected 2 items in bootstrap checkpoint, got %d", len(checkpoint.Items))
	}
	state := map[string]map[string]any{}
	for _, item := range checkpoint.Items {
		state[item.PK+"\x00"+item.SK] = item.Item
	}
	if _, ok := state["S|a\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected key S|a in bootstrap checkpoint, got keys=%v", keysOf(state))
	}
	if _, ok := state["S|b\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected key S|b in bootstrap checkpoint, got keys=%v", keysOf(state))
	}
}

func TestCompactItemChangesBeforeBuildsNextCheckpointFromPreviousCheckpoint(t *testing.T) {
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

	table := model.Table{Name: "checkpoint_chain_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := repo.TableRepo().CreateTable(ctx, table); err != nil {
		t.Fatal(err)
	}

	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|steady", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "steady"}}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|moving", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "v1"}}, 1100); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.PITRRepo().CompactItemChangesBefore(ctx, table.Name, 1500); err != nil {
		t.Fatal(err)
	}

	if err := repo.PITRRepo().AppendItemChange(ctx, table.Name, "S|moving", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "v2"}}, 2100); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.PITRRepo().CompactItemChangesBefore(ctx, table.Name, 2500); err != nil {
		t.Fatal(err)
	}

	checkpoint, err := repo.PITRRepo().GetLatestPITRCheckpointAtOrBefore(ctx, table.Name, 2500)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.Found {
		t.Fatal("expected latest checkpoint after second compaction")
	}

	state := map[string]map[string]any{}
	for _, item := range checkpoint.Items {
		state[item.PK+"\x00"+item.SK] = item.Item
	}

	if len(state) != 2 {
		t.Fatalf("expected 2 items in chained checkpoint, got %d", len(state))
	}
	if _, ok := state["S|steady\x00"+model.NoSortKey]; !ok {
		t.Fatalf("expected steady key to remain via previous checkpoint, got keys=%v", keysOf(state))
	}
	moving, ok := state["S|moving\x00"+model.NoSortKey]
	if !ok {
		t.Fatalf("expected moving key in chained checkpoint, got keys=%v", keysOf(state))
	}
	v, ok := moving["v"].(map[string]any)
	if !ok {
		t.Fatalf("expected moving item to contain map value, got %#v", moving)
	}
	if v["S"] != "v2" {
		t.Fatalf("expected moving value v2 after incremental checkpoint, got %#v", v)
	}
}

func rebuildFromCheckpointAndChanges(checkpoint model.PITRCheckpoint, changes []model.ItemChange) map[string]map[string]any {
	state := map[string]map[string]any{}
	if checkpoint.Found {
		for _, item := range checkpoint.Items {
			state[item.PK+"\x00"+item.SK] = item.Item
		}
	}
	for _, change := range changes {
		key := change.PK + "\x00" + change.SK
		if strings.EqualFold(change.ChangeType, "DELETE") {
			delete(state, key)
			continue
		}
		if change.Item != nil {
			state[key] = change.Item
		}
	}
	return state
}

func keysOf(m map[string]map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
