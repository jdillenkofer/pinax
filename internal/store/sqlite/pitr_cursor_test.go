package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jdillenkofer/pinax/internal/model"

	_ "github.com/mattn/go-sqlite3"
)

func TestListItemChangesUpToCursorUsesStableBoundary(t *testing.T) {
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
	defer tx.Rollback()

	table := model.Table{Name: "cursor_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := s.CreateTable(ctx, tx, table); err != nil {
		t.Fatal(err)
	}

	if err := s.AppendItemChange(ctx, tx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "old"}}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendItemChange(ctx, tx, table.Name, "S|a", model.NoSortKey, "PUT", map[string]any{"v": map[string]any{"S": "new"}}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendItemChange(ctx, tx, table.Name, "S|a", model.NoSortKey, "DELETE", nil, 1001); err != nil {
		t.Fatal(err)
	}

	cursor, err := s.ResolveItemChangeCursorAtOrBefore(ctx, tx, table.Name, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !cursor.Found {
		t.Fatal("expected cursor to be found")
	}

	changes, err := s.ListItemChangesUpToCursor(ctx, tx, table.Name, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes up to cursor, got %d", len(changes))
	}
	if changes[1].Sequence != cursor.Sequence {
		t.Fatalf("expected replay boundary sequence %d, got %d", cursor.Sequence, changes[1].Sequence)
	}
	if changes[1].ChangeType != "PUT" {
		t.Fatalf("expected last change to be PUT, got %q", changes[1].ChangeType)
	}
}

func TestResolveItemChangeCursorAtOrBeforeReturnsNotFoundWhenEmpty(t *testing.T) {
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
	defer tx.Rollback()

	table := model.Table{Name: "empty_cursor_table", HashKey: "pk", HashType: "S", CreatedAt: 1}
	if err := s.CreateTable(ctx, tx, table); err != nil {
		t.Fatal(err)
	}

	cursor, err := s.ResolveItemChangeCursorAtOrBefore(ctx, tx, table.Name, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Found {
		t.Fatalf("expected no cursor, got %+v", cursor)
	}

	changes, err := s.ListItemChangesUpToCursor(ctx, tx, table.Name, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 {
		t.Fatalf("expected no changes, got %d", len(changes))
	}
}
