package sqlite

import (
	"database/sql"
	"testing"

	testutils "github.com/jdillenkofer/pinax/internal/testing"
	_ "github.com/mattn/go-sqlite3"
)

func TestMigrateUpAndDownAndUp(t *testing.T) {
	testutils.SkipIfIntegration(t)

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	m, err := createMigrateInstance(db)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
	if err := m.Down(); err != nil {
		t.Fatal(err)
	}
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
}
