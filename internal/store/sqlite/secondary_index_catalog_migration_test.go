package sqlite

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pinax/internal/testing"
	_ "github.com/mattn/go-sqlite3"
)

func TestSecondaryIndexCatalogMigrationBackfillsLegacyData(t *testing.T) {
	testutils.SkipIfIntegration(t)

	dbPath := filepath.Join(t.TempDir(), "migration.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m, err := createMigrateInstance(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Migrate(17); err != nil {
		t.Fatal(err)
	}

	now := time.Now().Unix()
	gsiJSON := `[{"IndexName":"status-index","HashKey":"status","HashType":"S","RangeKey":"createdAt","RangeType":"N","Status":"ACTIVE","ProjectionType":"ALL","NonKeyAttrs":[]}]`
	lsiJSON := `[{"IndexName":"priority-index","RangeKey":"priority","RangeType":"N","ProjectionType":"INCLUDE","NonKeyAttrs":["summary"]}]`
	_, err = db.Exec(`
		INSERT INTO tables(name, hash_key, hash_type, range_key, range_type, created_at, gsi_json, lsi_json)
		VALUES (?, 'pk', 'S', 'sk', 'S', ?, ?, ?)
	`, "orders", now, gsiJSON, lsiJSON)
	if err != nil {
		t.Fatal(err)
	}

	item := map[string]any{
		"pk":        map[string]any{"S": "o#1"},
		"sk":        map[string]any{"S": "item#1"},
		"status":    map[string]any{"S": "OPEN"},
		"createdAt": map[string]any{"N": "1"},
		"priority":  map[string]any{"N": "10"},
		"summary":   map[string]any{"S": "hello"},
	}
	raw, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		INSERT INTO items(table_key, pk, sk, item_json, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`, "orders", "S|o#1", "S|item#1", raw, now)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}

	var indexCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM secondary_indexes WHERE table_key = ?`, "orders").Scan(&indexCount); err != nil {
		t.Fatal(err)
	}
	if indexCount != 2 {
		t.Fatalf("expected 2 secondary index definitions, got %d", indexCount)
	}

	var entryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM secondary_index_entries WHERE table_key = ?`, "orders").Scan(&entryCount); err != nil {
		t.Fatal(err)
	}
	if entryCount != 2 {
		t.Fatalf("expected 2 secondary index entries, got %d", entryCount)
	}

	if _, err := db.Exec(`SELECT 1 FROM item_gsis LIMIT 1`); err == nil {
		t.Fatal("expected legacy item_gsis table to be dropped")
	}

	columns, err := tableColumns(db, "tables")
	if err != nil {
		t.Fatal(err)
	}
	for _, col := range columns {
		if col == "gsi_json" || col == "lsi_json" {
			t.Fatalf("expected legacy column %q to be removed", col)
		}
	}

	sourceErr, databaseErr := m.Close()
	if sourceErr != nil {
		t.Fatal(sourceErr)
	}
	if databaseErr != nil {
		t.Fatal(databaseErr)
	}
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}
}

func tableColumns(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
