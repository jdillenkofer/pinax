package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jdillenkofer/pinax/internal/model"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

type Store struct {
	db *sql.DB
}

func New(db *sql.DB) (*Store, error) {
	s := &Store{db: db}
	if err := s.setupDatabase(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) setupDatabase() error {
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

func (s *Store) CreateTable(ctx context.Context, tx *sql.Tx, t model.Table) error {
	gsiJSON, err := json.Marshal(t.GSIs)
	if err != nil {
		return err
	}
	ttlEnabled := 0
	if t.TimeToLive.Enabled {
		ttlEnabled = 1
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO tables(name, hash_key, hash_type, range_key, range_type, gsi_json, created_at, ttl_enabled, ttl_attribute)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, t.Name, t.HashKey, t.HashType, nullIfEmpty(t.RangeKey), nullIfEmpty(t.RangeType), string(gsiJSON), t.CreatedAt, ttlEnabled, nullIfEmpty(t.TimeToLive.AttrName))
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) GetTable(ctx context.Context, tx *sql.Tx, name string) (model.Table, error) {
	var t model.Table
	var rangeKey sql.NullString
	var rangeType sql.NullString
	var gsiJSON string
	var ttlEnabled int
	var ttlAttr sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT name, hash_key, hash_type, range_key, range_type, gsi_json, created_at, ttl_enabled, ttl_attribute
		FROM tables
		WHERE name = ?
	`, name).Scan(&t.Name, &t.HashKey, &t.HashType, &rangeKey, &rangeType, &gsiJSON, &t.CreatedAt, &ttlEnabled, &ttlAttr)
	if err != nil {
		return model.Table{}, err
	}
	t.RangeKey = rangeKey.String
	t.RangeType = rangeType.String
	if strings.TrimSpace(gsiJSON) != "" {
		if err := json.Unmarshal([]byte(gsiJSON), &t.GSIs); err != nil {
			return model.Table{}, err
		}
	}
	t.TimeToLive.Enabled = ttlEnabled == 1
	t.TimeToLive.AttrName = ttlAttr.String
	return t, nil
}

func (s *Store) ListTables(ctx context.Context, tx *sql.Tx, start string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT name FROM tables
		WHERE name > ?
		ORDER BY name ASC
		LIMIT ?
	`, start, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (s *Store) DeleteTable(ctx context.Context, tx *sql.Tx, name string) error {
	res, err := tx.ExecContext(ctx, `DELETE FROM tables WHERE name = ?`, name)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) CountItems(ctx context.Context, tx *sql.Tx, tableName string) (int64, error) {
	var n int64
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE table_name = ?`, tableName).Scan(&n)
	return n, err
}

func (s *Store) GetItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) (map[string]any, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT item_json FROM items
		WHERE table_name = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk).Scan(&raw)
	if err != nil {
		return nil, err
	}
	return decodeItem(raw)
}

func (s *Store) PutItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string, item map[string]any) error {
	t, err := s.GetTable(ctx, tx, tableName)
	if err != nil {
		return err
	}

	ttl, hasTTL := model.ExtractTTL(t, item)
	var ttlVal any
	if hasTTL {
		ttlVal = ttl
	}

	raw, err := json.Marshal(item)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO items(table_name, pk, sk, item_json, updated_at, ttl)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(table_name, pk, sk)
		DO UPDATE SET item_json = excluded.item_json, updated_at = excluded.updated_at, ttl = excluded.ttl
	`, tableName, pk, sk, raw, time.Now().Unix(), ttlVal)
	if err != nil {
		return err
	}

	// Delete old GSI entries
	_, err = tx.ExecContext(ctx, `DELETE FROM item_gsis WHERE table_name = ? AND pk = ? AND sk = ?`, tableName, pk, sk)
	if err != nil {
		return err
	}

	for _, gsi := range t.GSIs {
		gpk, gsk, ok := model.ExtractGSIKeys(gsi, item)
		if !ok {
			continue
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO item_gsis(table_name, pk, sk, index_name, gsi_pk, gsi_sk)
			VALUES (?, ?, ?, ?, ?, ?)
		`, tableName, pk, sk, gsi.IndexName, gpk, gsk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) DeleteItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_name = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	return err
}

func (s *Store) QueryByPK(ctx context.Context, tx *sql.Tx, tableName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	order := "ASC"
	comp := ">"
	if !scanForward {
		order = "DESC"
		comp = "<"
	}
	q := fmt.Sprintf(`
		SELECT item_json FROM items
		WHERE table_name = ? AND pk = ? AND sk %s ?
		ORDER BY sk %s
	`, comp, order)
	args := []any{tableName, pk, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) QueryByGSI(ctx context.Context, tx *sql.Tx, tableName, indexName, pk, startSK string, scanForward bool, limit int) ([]map[string]any, error) {
	order := "ASC"
	comp := ">"
	if !scanForward {
		order = "DESC"
		comp = "<"
	}
	q := fmt.Sprintf(`
		SELECT i.item_json FROM item_gsis g
		JOIN items i ON g.table_name = i.table_name AND g.pk = i.pk AND g.sk = i.sk
		WHERE g.table_name = ? AND g.index_name = ? AND g.gsi_pk = ? AND g.gsi_sk %s ?
		ORDER BY g.gsi_sk %s
	`, comp, order)
	args := []any{tableName, indexName, pk, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) QueryByPKSK(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) ([]map[string]any, error) {
	item, err := s.GetItem(ctx, tx, tableName, pk, sk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return []map[string]any{item}, nil
}

func (s *Store) Scan(ctx context.Context, tx *sql.Tx, tableName, startPK, startSK string, limit int) ([]map[string]any, error) {
	q := `
		SELECT item_json FROM items
		WHERE table_name = ? AND (pk > ? OR (pk = ? AND sk > ?))
		ORDER BY pk ASC, sk ASC
	`
	args := []any{tableName, startPK, startPK, startSK}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		item, err := decodeItem(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func decodeItem(raw []byte) (map[string]any, error) {
	var item map[string]any
	if err := json.Unmarshal(raw, &item); err != nil {
		return nil, fmt.Errorf("decode item: %w", err)
	}
	return item, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *Store) UpdateTimeToLive(ctx context.Context, tx *sql.Tx, tableName string, ttl model.TimeToLive) error {
	ttlEnabled := 0
	if ttl.Enabled {
		ttlEnabled = 1
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE tables SET ttl_enabled = ?, ttl_attribute = ? WHERE name = ?
	`, ttlEnabled, nullIfEmpty(ttl.AttrName), tableName)
	return err
}

func (s *Store) GetExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, ttlAttr string, before int64, limit int) ([]struct {
	PK string
	SK string
}, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk FROM items
		WHERE table_name = ? AND ttl > 0 AND ttl < ?
		LIMIT ?
	`, tableName, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var expired []struct {
		PK string
		SK string
	}
	for rows.Next() {
		var pk, sk string
		if err := rows.Scan(&pk, &sk); err != nil {
			return nil, err
		}
		expired = append(expired, struct {
			PK string
			SK string
		}{PK: pk, SK: sk})
	}
	return expired, rows.Err()
}

func (s *Store) DeleteExpiredItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_name = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	return err
}

func (s *Store) DeleteExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, before int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	res, err := tx.ExecContext(ctx, `
		DELETE FROM items
		WHERE rowid IN (
			SELECT rowid FROM items
			WHERE table_name = ? AND ttl > 0 AND ttl < ?
			ORDER BY ttl ASC
			LIMIT ?
		)
	`, tableName, before, limit)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return deleted, nil
}
