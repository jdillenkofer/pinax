package sqlite

import (
	"database/sql"
	"embed"
	"errors"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

type Backend struct {
	db *sql.DB
}

func New(db *sql.DB) (*Backend, error) {
	s := &Backend{db: db}
	if err := s.setupDatabase(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Backend) DB() *sql.DB {
	return s.db
}

func (s *Backend) setupDatabase() error {
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	s.db.SetConnMaxIdleTime(0)
	s.db.SetConnMaxLifetime(0)

	if err := enableAutoVacuumFullMode(s.db); err != nil {
		return err
	}
	if err := enableWALJournalMode(s.db); err != nil {
		return err
	}
	if err := enableNormalSynchronous(s.db); err != nil {
		return err
	}
	if err := applyDatabaseMigrations(s.db); err != nil {
		return err
	}
	if err := enableForeignKeyConstraints(s.db); err != nil {
		return err
	}
	return nil
}

func enableAutoVacuumFullMode(db *sql.DB) error {
	_, err := db.Exec("PRAGMA auto_vacuum = FULL;")
	return err
}

func enableWALJournalMode(db *sql.DB) error {
	_, err := db.Exec("PRAGMA journal_mode = WAL;")
	return err
}

func enableNormalSynchronous(db *sql.DB) error {
	_, err := db.Exec("PRAGMA synchronous = NORMAL;")
	return err
}

func enableForeignKeyConstraints(db *sql.DB) error {
	_, err := db.Exec("PRAGMA foreign_keys = ON;")
	return err
}

func createMigrateInstance(db *sql.DB) (*migrate.Migrate, error) {
	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations")
	if err != nil {
		return nil, err
	}

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, err
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func applyDatabaseMigrations(db *sql.DB) error {
	m, err := createMigrateInstance(db)
	if err != nil {
		return err
	}
	err = m.Up()
	if err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
