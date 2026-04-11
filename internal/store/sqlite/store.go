package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	lsiJSON, err := json.Marshal(t.LSIs)
	if err != nil {
		return err
	}
	ttlEnabled := 0
	if t.TimeToLive.Enabled {
		ttlEnabled = 1
	}
	ttlStatus := t.TimeToLive.Status
	if strings.TrimSpace(ttlStatus) == "" {
		ttlStatus = model.TTLStatusDisabled
	}
	tagsJSON, err := json.Marshal(t.Tags)
	if err != nil {
		return err
	}
	deletionProtection := 0
	if t.DeletionProtection {
		deletionProtection = 1
	}
	streamEnabled := 0
	if t.Stream.Enabled {
		streamEnabled = 1
	}
	sseEnabled := 0
	if t.SSE.Enabled {
		sseEnabled = 1
	}
	sseStatus := firstNonEmpty(t.SSE.Status, "DISABLED")
	pitrEnabled := 0
	if t.PITR.Enabled {
		pitrEnabled = 1
	}
	pitrRecoveryDays := t.PITR.RecoveryPeriodInDays
	if pitrRecoveryDays <= 0 {
		pitrRecoveryDays = 35
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO tables(name, hash_key, hash_type, range_key, range_type, billing_mode, read_capacity_units, write_capacity_units, table_class, deletion_protection_enabled, stream_enabled, stream_view_type, stream_arn, stream_label, sse_enabled, sse_type, sse_status, sse_kms_key_id, tags_json, table_status, table_status_at, gsi_json, lsi_json, created_at, ttl_enabled, ttl_attribute, ttl_status, ttl_status_at, pitr_enabled, pitr_recovery_period_days, pitr_enabled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, t.Name, t.HashKey, t.HashType, nullIfEmpty(t.RangeKey), nullIfEmpty(t.RangeType), firstNonEmpty(t.BillingMode, "PAY_PER_REQUEST"), t.ReadCapacityUnits, t.WriteCapacityUnits, firstNonEmpty(t.TableClass, "STANDARD"), deletionProtection, streamEnabled, nullIfEmpty(t.Stream.ViewType), nullIfEmpty(t.Stream.ARN), nullIfEmpty(t.Stream.Label), sseEnabled, nullIfEmpty(t.SSE.SSEType), sseStatus, nullIfEmpty(t.SSE.KMSMasterKeyID), string(tagsJSON), nullIfEmpty(firstNonEmpty(t.Status, model.TableStatusActive)), t.StatusAt, string(gsiJSON), string(lsiJSON), t.CreatedAt, ttlEnabled, nullIfEmpty(t.TimeToLive.AttrName), ttlStatus, t.TimeToLive.StatusAt, pitrEnabled, pitrRecoveryDays, t.PITR.EnabledAt)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) GetTable(ctx context.Context, tx *sql.Tx, name string) (model.Table, error) {
	var t model.Table
	var rangeKey sql.NullString
	var rangeType sql.NullString
	var tableStatus string
	var tableStatusAt int64
	var billingMode string
	var tableClass string
	var deletionProtection int
	var streamEnabled int
	var streamViewType sql.NullString
	var streamARN sql.NullString
	var streamLabel sql.NullString
	var sseEnabled int
	var sseType sql.NullString
	var sseStatus string
	var sseKMSKeyID sql.NullString
	var tagsJSON string
	var readCapacityUnits int64
	var writeCapacityUnits int64
	var gsiJSON string
	var lsiJSON string
	var ttlEnabled int
	var ttlAttr sql.NullString
	var ttlStatus string
	var ttlStatusAt int64
	var pitrEnabled int
	var pitrRecoveryDays int64
	var pitrEnabledAt int64
	err := tx.QueryRowContext(ctx, `
		SELECT name, hash_key, hash_type, range_key, range_type, billing_mode, read_capacity_units, write_capacity_units, table_class, deletion_protection_enabled, stream_enabled, stream_view_type, stream_arn, stream_label, sse_enabled, sse_type, sse_status, sse_kms_key_id, tags_json, table_status, table_status_at, gsi_json, lsi_json, created_at, ttl_enabled, ttl_attribute, ttl_status, ttl_status_at, pitr_enabled, pitr_recovery_period_days, pitr_enabled_at
		FROM tables
		WHERE name = ?
	`, name).Scan(&t.Name, &t.HashKey, &t.HashType, &rangeKey, &rangeType, &billingMode, &readCapacityUnits, &writeCapacityUnits, &tableClass, &deletionProtection, &streamEnabled, &streamViewType, &streamARN, &streamLabel, &sseEnabled, &sseType, &sseStatus, &sseKMSKeyID, &tagsJSON, &tableStatus, &tableStatusAt, &gsiJSON, &lsiJSON, &t.CreatedAt, &ttlEnabled, &ttlAttr, &ttlStatus, &ttlStatusAt, &pitrEnabled, &pitrRecoveryDays, &pitrEnabledAt)
	if err != nil {
		return model.Table{}, err
	}
	t.RangeKey = rangeKey.String
	t.RangeType = rangeType.String
	t.BillingMode = billingMode
	t.TableClass = tableClass
	t.DeletionProtection = deletionProtection == 1
	t.Stream.Enabled = streamEnabled == 1
	t.Stream.ViewType = streamViewType.String
	t.Stream.ARN = streamARN.String
	t.Stream.Label = streamLabel.String
	t.SSE.Enabled = sseEnabled == 1
	t.SSE.SSEType = sseType.String
	t.SSE.Status = sseStatus
	t.SSE.KMSMasterKeyID = sseKMSKeyID.String
	if strings.TrimSpace(tagsJSON) != "" {
		if err := json.Unmarshal([]byte(tagsJSON), &t.Tags); err != nil {
			return model.Table{}, err
		}
	}
	t.ReadCapacityUnits = readCapacityUnits
	t.WriteCapacityUnits = writeCapacityUnits
	t.Status = tableStatus
	t.StatusAt = tableStatusAt
	if strings.TrimSpace(gsiJSON) != "" {
		if err := json.Unmarshal([]byte(gsiJSON), &t.GSIs); err != nil {
			return model.Table{}, err
		}
	}
	if strings.TrimSpace(lsiJSON) != "" {
		if err := json.Unmarshal([]byte(lsiJSON), &t.LSIs); err != nil {
			return model.Table{}, err
		}
	}
	t.TimeToLive.Enabled = ttlEnabled == 1
	t.TimeToLive.AttrName = ttlAttr.String
	t.TimeToLive.Status = ttlStatus
	t.TimeToLive.StatusAt = ttlStatusAt
	t.PITR.Enabled = pitrEnabled == 1
	t.PITR.RecoveryPeriodInDays = pitrRecoveryDays
	t.PITR.EnabledAt = pitrEnabledAt
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
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE table_key = ?`, tableName).Scan(&n)
	return n, err
}

func (s *Store) GetItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) (map[string]any, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
		SELECT item_json FROM items
		WHERE table_key = ? AND pk = ? AND sk = ?
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
		INSERT INTO items(table_key, pk, sk, item_json, updated_at, ttl)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(table_key, pk, sk)
		DO UPDATE SET item_json = excluded.item_json, updated_at = excluded.updated_at, ttl = excluded.ttl
	`, tableName, pk, sk, raw, time.Now().Unix(), ttlVal)
	if err != nil {
		return err
	}

	// Delete old GSI entries
	_, err = tx.ExecContext(ctx, `DELETE FROM item_gsis WHERE table_key = ? AND pk = ? AND sk = ?`, tableName, pk, sk)
	if err != nil {
		return err
	}

	for _, gsi := range t.GSIs {
		gpk, gsk, ok := model.ExtractGSIKeys(gsi, item)
		if !ok {
			continue
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO item_gsis(table_key, pk, sk, index_name, gsi_pk, gsi_sk)
			VALUES (?, ?, ?, ?, ?, ?)
		`, tableName, pk, sk, gsi.IndexName, gpk, gsk)
		if err != nil {
			return err
		}
	}
	if t.PITR.Enabled {
		nowMs := time.Now().UnixMilli()
		if err := s.appendItemHistory(ctx, tx, tableName, pk, sk, "PUT", item, nowMs); err != nil {
			return err
		}
		if err := s.pruneItemHistoryByRetention(ctx, tx, t, nowMs); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) DeleteItem(ctx context.Context, tx *sql.Tx, tableName, pk, sk string) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_key = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	if err != nil {
		return err
	}
	t, err := s.GetTable(ctx, tx, tableName)
	if err != nil {
		return err
	}
	if t.PITR.Enabled {
		nowMs := time.Now().UnixMilli()
		if err := s.appendItemHistory(ctx, tx, tableName, pk, sk, "DELETE", nil, nowMs); err != nil {
			return err
		}
		if err := s.pruneItemHistoryByRetention(ctx, tx, t, nowMs); err != nil {
			return err
		}
	}
	return nil
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
		WHERE table_key = ? AND pk = ? AND sk %s ?
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
		JOIN items i ON g.table_key = i.table_key AND g.pk = i.pk AND g.sk = i.sk
		WHERE g.table_key = ? AND g.index_name = ? AND g.gsi_pk = ? AND g.gsi_sk %s ?
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
		WHERE table_key = ? AND (pk > ? OR (pk = ? AND sk > ?))
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

func (s *Store) UpdateTableIndexes(ctx context.Context, tx *sql.Tx, tableName string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	gsiJSON, err := json.Marshal(gsis)
	if err != nil {
		return err
	}
	lsiJSON, err := json.Marshal(lsis)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE tables
		SET table_status = ?, table_status_at = ?, gsi_json = ?, lsi_json = ?
		WHERE name = ?
	`, firstNonEmpty(tableStatus, model.TableStatusActive), tableStatusAt, string(gsiJSON), string(lsiJSON), tableName)
	return err
}

func (s *Store) UpdateTableBilling(ctx context.Context, tx *sql.Tx, tableName string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE tables
		SET billing_mode = ?, read_capacity_units = ?, write_capacity_units = ?
		WHERE name = ?
	`, firstNonEmpty(billingMode, "PAY_PER_REQUEST"), readCapacityUnits, writeCapacityUnits, tableName)
	return err
}

func (s *Store) UpdateTableOptions(ctx context.Context, tx *sql.Tx, tableName string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error {
	deletionProtectionInt := 0
	if deletionProtection {
		deletionProtectionInt = 1
	}
	streamEnabled := 0
	if stream.Enabled {
		streamEnabled = 1
	}
	sseEnabled := 0
	if sse.Enabled {
		sseEnabled = 1
	}
	tagsJSON, err := json.Marshal(tags)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE tables
		SET table_class = ?, deletion_protection_enabled = ?, stream_enabled = ?, stream_view_type = ?, stream_arn = ?, stream_label = ?, sse_enabled = ?, sse_type = ?, sse_status = ?, sse_kms_key_id = ?, tags_json = ?
		WHERE name = ?
	`, firstNonEmpty(tableClass, "STANDARD"), deletionProtectionInt, streamEnabled, nullIfEmpty(stream.ViewType), nullIfEmpty(stream.ARN), nullIfEmpty(stream.Label), sseEnabled, nullIfEmpty(sse.SSEType), firstNonEmpty(sse.Status, "DISABLED"), nullIfEmpty(sse.KMSMasterKeyID), string(tagsJSON), tableName)
	return err
}

func firstNonEmpty(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

func (s *Store) UpdateTimeToLive(ctx context.Context, tx *sql.Tx, tableName string, ttl model.TimeToLive) error {
	ttlEnabled := 0
	if ttl.Enabled {
		ttlEnabled = 1
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE tables SET ttl_enabled = ?, ttl_attribute = ?, ttl_status = ?, ttl_status_at = ? WHERE name = ?
	`, ttlEnabled, nullIfEmpty(ttl.AttrName), ttl.Status, ttl.StatusAt, tableName)
	return err
}

func (s *Store) UpdatePointInTimeRecovery(ctx context.Context, tx *sql.Tx, tableName string, pitr model.PointInTimeRecovery) error {
	pitrEnabled := 0
	if pitr.Enabled {
		pitrEnabled = 1
	}
	recoveryDays := pitr.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = 35
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE tables SET pitr_enabled = ?, pitr_recovery_period_days = ?, pitr_enabled_at = ? WHERE name = ?
	`, pitrEnabled, recoveryDays, pitr.EnabledAt, tableName)
	return err
}

func (s *Store) ListItemChangesUpTo(ctx context.Context, tx *sql.Tx, tableName string, upTo int64) ([]model.ItemChange, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at ASC, id ASC
	`, tableName, upTo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ResolveItemChangeCursorAtOrBefore(ctx context.Context, tx *sql.Tx, tableName string, upTo int64) (model.ItemChangeCursor, error) {
	var cursor model.ItemChangeCursor
	err := tx.QueryRowContext(ctx, `
		SELECT changed_at, id
		FROM item_history
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at DESC, id DESC
		LIMIT 1
	`, tableName, upTo).Scan(&cursor.ChangedAt, &cursor.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.ItemChangeCursor{}, nil
		}
		return model.ItemChangeCursor{}, err
	}
	cursor.Found = true
	return cursor, nil
}

func (s *Store) ListItemChangesUpToCursor(ctx context.Context, tx *sql.Tx, tableName string, cursor model.ItemChangeCursor) ([]model.ItemChange, error) {
	if !cursor.Found {
		return []model.ItemChange{}, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ?
		  AND (changed_at < ? OR (changed_at = ? AND id <= ?))
		ORDER BY changed_at ASC, id ASC
	`, tableName, cursor.ChangedAt, cursor.ChangedAt, cursor.Sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ListItemChangesAfterCursorUpToCursor(ctx context.Context, tx *sql.Tx, tableName string, after model.ItemChangeCursor, upTo model.ItemChangeCursor) ([]model.ItemChange, error) {
	if !upTo.Found {
		return []model.ItemChange{}, nil
	}
	if !after.Found {
		return s.ListItemChangesUpToCursor(ctx, tx, tableName, upTo)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk, change_type, item_json, changed_at, id
		FROM item_history
		WHERE table_key = ?
		  AND (changed_at > ? OR (changed_at = ? AND id > ?))
		  AND (changed_at < ? OR (changed_at = ? AND id <= ?))
		ORDER BY changed_at ASC, id ASC
	`, tableName, after.ChangedAt, after.ChangedAt, after.Sequence, upTo.ChangedAt, upTo.ChangedAt, upTo.Sequence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.ItemChange, 0)
	for rows.Next() {
		var change model.ItemChange
		var raw []byte
		if err := rows.Scan(&change.PK, &change.SK, &change.ChangeType, &raw, &change.ChangedAt, &change.Sequence); err != nil {
			return nil, err
		}
		change.TableName = tableName
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &change.Item); err != nil {
				return nil, err
			}
		}
		out = append(out, change)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) GetLatestPITRCheckpointAtOrBefore(ctx context.Context, tx *sql.Tx, tableName string, upTo int64) (model.PITRCheckpoint, error) {
	if upTo <= 0 {
		return model.PITRCheckpoint{}, nil
	}
	var cursor model.ItemChangeCursor
	err := tx.QueryRowContext(ctx, `
		SELECT changed_at, history_sequence
		FROM pitr_checkpoints
		WHERE table_key = ? AND changed_at <= ?
		ORDER BY changed_at DESC, history_sequence DESC
		LIMIT 1
	`, tableName, upTo).Scan(&cursor.ChangedAt, &cursor.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PITRCheckpoint{}, nil
		}
		return model.PITRCheckpoint{}, err
	}
	cursor.Found = true
	return s.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tx, tableName, cursor)
}

func (s *Store) GetLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tx *sql.Tx, tableName string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	return s.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tx, tableName, cursor)
}

func (s *Store) CreatePITRCheckpointFromCurrentState(ctx context.Context, tx *sql.Tx, tableName string, changedAt int64) error {
	cursor, err := s.ResolveItemChangeCursorAtOrBefore(ctx, tx, tableName, changedAt)
	if err != nil {
		return err
	}
	sequence := int64(0)
	if cursor.Found {
		sequence = cursor.Sequence
	} else if changedAt > 0 {
		sequence = -changedAt
	} else {
		sequence = -1
	}

	var existing int
	err = tx.QueryRowContext(ctx, `
		SELECT 1
		FROM pitr_checkpoints
		WHERE table_key = ? AND history_sequence = ?
		LIMIT 1
	`, tableName, sequence).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk, item_json
		FROM items
		WHERE table_key = ?
		ORDER BY pk ASC, sk ASC
	`, tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	items := make([]model.PITRCheckpointItem, 0)
	for rows.Next() {
		var item model.PITRCheckpointItem
		var raw []byte
		if err := rows.Scan(&item.PK, &item.SK, &raw); err != nil {
			return err
		}
		item.Item = map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &item.Item); err != nil {
				return err
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return s.insertPITRCheckpoint(ctx, tx, tableName, changedAt, sequence, items)
}

func (s *Store) getLatestPITRCheckpointAtOrBeforeCursor(ctx context.Context, tx *sql.Tx, tableName string, cursor model.ItemChangeCursor) (model.PITRCheckpoint, error) {
	if !cursor.Found {
		return model.PITRCheckpoint{}, nil
	}
	var checkpoint model.PITRCheckpoint
	var checkpointID int64
	err := tx.QueryRowContext(ctx, `
		SELECT id, changed_at, history_sequence
		FROM pitr_checkpoints
		WHERE table_key = ?
		  AND (changed_at < ? OR (changed_at = ? AND history_sequence <= ?))
		ORDER BY changed_at DESC, history_sequence DESC
		LIMIT 1
	`, tableName, cursor.ChangedAt, cursor.ChangedAt, cursor.Sequence).Scan(&checkpointID, &checkpoint.ChangedAt, &checkpoint.Sequence)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.PITRCheckpoint{}, nil
		}
		return model.PITRCheckpoint{}, err
	}
	checkpoint.Found = true
	items, err := s.listPITRCheckpointItems(ctx, tx, checkpointID)
	if err != nil {
		return model.PITRCheckpoint{}, err
	}
	checkpoint.Items = items
	return checkpoint, nil
}

func (s *Store) listPITRCheckpointItems(ctx context.Context, tx *sql.Tx, checkpointID int64) ([]model.PITRCheckpointItem, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT pk, sk, item_json
		FROM pitr_checkpoint_items
		WHERE checkpoint_id = ?
	`, checkpointID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]model.PITRCheckpointItem, 0)
	for rows.Next() {
		var item model.PITRCheckpointItem
		var raw []byte
		if err := rows.Scan(&item.PK, &item.SK, &raw); err != nil {
			return nil, err
		}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &item.Item); err != nil {
				return nil, err
			}
		} else {
			item.Item = map[string]any{}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) DeleteItemChangesBefore(ctx context.Context, tx *sql.Tx, tableName string, before int64) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		DELETE FROM item_history
		WHERE table_key = ? AND changed_at < ?
	`, tableName, before)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *Store) CompactItemChangesBefore(ctx context.Context, tx *sql.Tx, tableName string, before int64) (int64, error) {
	if before <= 0 {
		return 0, nil
	}
	var hasRows int
	err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM item_history
		WHERE table_key = ? AND changed_at < ?
		LIMIT 1
	`, tableName, before).Scan(&hasRows)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	cursor, err := s.ResolveItemChangeCursorAtOrBefore(ctx, tx, tableName, before)
	if err != nil {
		return 0, err
	}
	if cursor.Found {
		if err := s.createPITRCheckpointForCursor(ctx, tx, tableName, cursor); err != nil {
			return 0, err
		}
	}

	return s.DeleteItemChangesBefore(ctx, tx, tableName, before)
}

func (s *Store) createPITRCheckpointForCursor(ctx context.Context, tx *sql.Tx, tableName string, cursor model.ItemChangeCursor) error {
	if !cursor.Found {
		return nil
	}

	var existing int
	err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM pitr_checkpoints
		WHERE table_key = ? AND history_sequence = ?
		LIMIT 1
	`, tableName, cursor.Sequence).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	previousCheckpoint, err := s.getLatestPITRCheckpointAtOrBeforeCursor(ctx, tx, tableName, cursor)
	if err != nil {
		return err
	}

	after := model.ItemChangeCursor{}
	if previousCheckpoint.Found {
		after = model.ItemChangeCursor{Found: true, ChangedAt: previousCheckpoint.ChangedAt, Sequence: previousCheckpoint.Sequence}
	}
	changes, err := s.ListItemChangesAfterCursorUpToCursor(ctx, tx, tableName, after, cursor)
	if err != nil {
		return err
	}

	state := make(map[string]map[string]any)
	if previousCheckpoint.Found {
		for _, item := range previousCheckpoint.Items {
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

	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	items := make([]model.PITRCheckpointItem, 0, len(keys))
	for _, key := range keys {
		sep := strings.IndexByte(key, '\x00')
		if sep < 0 {
			continue
		}
		items = append(items, model.PITRCheckpointItem{
			PK:   key[:sep],
			SK:   key[sep+1:],
			Item: state[key],
		})
	}

	return s.insertPITRCheckpoint(ctx, tx, tableName, cursor.ChangedAt, cursor.Sequence, items)
}

func (s *Store) insertPITRCheckpoint(ctx context.Context, tx *sql.Tx, tableName string, changedAt int64, sequence int64, items []model.PITRCheckpointItem) error {
	res, err := tx.ExecContext(ctx, `
		INSERT INTO pitr_checkpoints(table_key, changed_at, history_sequence, created_at)
		VALUES (?, ?, ?, ?)
	`, tableName, changedAt, sequence, time.Now().UnixMilli())
	if err != nil {
		return err
	}

	checkpointID, err := res.LastInsertId()
	if err != nil {
		return err
	}

	for _, item := range items {
		raw, err := json.Marshal(item.Item)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO pitr_checkpoint_items(checkpoint_id, pk, sk, item_json)
			VALUES (?, ?, ?, ?)
		`, checkpointID, item.PK, item.SK, raw); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) pruneItemHistoryByRetention(ctx context.Context, tx *sql.Tx, table model.Table, nowMs int64) error {
	if !table.PITR.Enabled {
		return nil
	}
	recoveryDays := table.PITR.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = 35
	}
	cutoff := nowMs - (recoveryDays * 24 * 60 * 60 * 1000)
	if cutoff <= 0 {
		return nil
	}
	_, err := s.CompactItemChangesBefore(ctx, tx, table.Name, cutoff)
	return err
}

func (s *Store) AppendItemChange(ctx context.Context, tx *sql.Tx, tableName, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	return s.appendItemHistory(ctx, tx, tableName, pk, sk, changeType, item, changedAt)
}

func (s *Store) appendItemHistory(ctx context.Context, tx *sql.Tx, tableName, pk, sk, changeType string, item map[string]any, changedAt int64) error {
	var raw any
	if item != nil {
		payload, err := json.Marshal(item)
		if err != nil {
			return err
		}
		raw = string(payload)
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO item_history(table_key, pk, sk, change_type, item_json, changed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, tableName, pk, sk, changeType, raw, changedAt)
	return err
}

func (s *Store) GetTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, token string, now int64) (model.TransactWriteIdempotencyRecord, error) {
	var rec model.TransactWriteIdempotencyRecord
	var responseJSON []byte
	err := tx.QueryRowContext(ctx, `
		SELECT token, request_hash, response_json, created_at, expires_at
		FROM transact_write_idempotency
		WHERE token = ? AND expires_at > ?
	`, token, now).Scan(&rec.Token, &rec.RequestHash, &responseJSON, &rec.CreatedAt, &rec.ExpiresAt)
	if err != nil {
		return model.TransactWriteIdempotencyRecord{}, err
	}
	if err := json.Unmarshal(responseJSON, &rec.Response); err != nil {
		return model.TransactWriteIdempotencyRecord{}, err
	}
	return rec, nil
}

func (s *Store) PutTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, record model.TransactWriteIdempotencyRecord) error {
	responseJSON, err := json.Marshal(record.Response)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO transact_write_idempotency(token, request_hash, response_json, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(token) DO UPDATE SET request_hash = excluded.request_hash, response_json = excluded.response_json, created_at = excluded.created_at, expires_at = excluded.expires_at
	`, record.Token, record.RequestHash, responseJSON, record.CreatedAt, record.ExpiresAt)
	return err
}

func (s *Store) DeleteExpiredTransactWriteIdempotency(ctx context.Context, tx *sql.Tx, now int64) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM transact_write_idempotency WHERE expires_at <= ?`, now)
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
		WHERE table_key = ? AND ttl > 0 AND ttl < ?
		ORDER BY ttl ASC, pk ASC, sk ASC
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
	t, err := s.GetTable(ctx, tx, tableName)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM items WHERE table_key = ? AND pk = ? AND sk = ?
	`, tableName, pk, sk)
	if err != nil {
		return err
	}

	if t.PITR.Enabled {
		nowMs := time.Now().UnixMilli()
		if err := s.appendItemHistory(ctx, tx, tableName, pk, sk, "DELETE", nil, nowMs); err != nil {
			return err
		}
		if err := s.pruneItemHistoryByRetention(ctx, tx, t, nowMs); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) DeleteExpiredItems(ctx context.Context, tx *sql.Tx, tableName string, before int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 100
	}
	expired, err := s.GetExpiredItems(ctx, tx, tableName, "", before, limit)
	if err != nil {
		return 0, err
	}
	for _, key := range expired {
		if err := s.DeleteExpiredItem(ctx, tx, tableName, key.PK, key.SK); err != nil {
			return 0, err
		}
	}
	return int64(len(expired)), nil
}

func (s *Store) CreateBackup(ctx context.Context, tx *sql.Tx, backup model.Backup) error {
	sourceTableDetailsJSON, err := json.Marshal(backup.SourceTableDetails)
	if err != nil {
		return err
	}
	sourceTableFeatureDetailsJSON, err := json.Marshal(backup.SourceTableFeatureDetails)
	if err != nil {
		return err
	}
	snapshotTableJSON, err := json.Marshal(backup.SnapshotTable)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO backups(
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, backup.BackupARN, backup.BackupName, backup.TableName, backup.TableARN, backup.TableID, backup.BackupStatus, backup.BackupType, backup.BackupCreationDateTime, backup.BackupSizeBytes, string(sourceTableDetailsJSON), string(sourceTableFeatureDetailsJSON), string(snapshotTableJSON))
	if err != nil {
		return err
	}
	for i, item := range backup.SnapshotItems {
		raw, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO backup_items(backup_arn, ordinal, item_json)
			VALUES (?, ?, ?)
		`, backup.BackupARN, i, raw); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) GetBackup(ctx context.Context, tx *sql.Tx, backupARN string) (model.Backup, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
		WHERE backup_arn = ?
	`, backupARN)
	backup, err := scanBackupMetadata(row)
	if err != nil {
		return model.Backup{}, err
	}
	items, err := loadBackupItems(ctx, tx, backup.BackupARN)
	if err != nil {
		return model.Backup{}, err
	}
	backup.SnapshotItems = items
	return backup, nil
}

func (s *Store) GetBackupByName(ctx context.Context, tx *sql.Tx, backupName string) (model.Backup, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
		WHERE backup_name = ?
	`, backupName)
	backup, err := scanBackupMetadata(row)
	if err != nil {
		return model.Backup{}, err
	}
	items, err := loadBackupItems(ctx, tx, backup.BackupARN)
	if err != nil {
		return model.Backup{}, err
	}
	backup.SnapshotItems = items
	return backup, nil
}

func (s *Store) ListBackups(ctx context.Context, tx *sql.Tx) ([]model.Backup, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			backup_arn,
			backup_name,
			table_key,
			table_arn,
			table_id,
			backup_status,
			backup_type,
			backup_creation_date_time,
			backup_size_bytes,
			source_table_details_json,
			source_table_feature_details_json,
			snapshot_table_json
		FROM backups
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.Backup, 0)
	for rows.Next() {
		backup, err := scanBackupMetadata(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, backup)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) DeleteBackup(ctx context.Context, tx *sql.Tx, backupARN string) error {
	res, err := tx.ExecContext(ctx, `DELETE FROM backups WHERE backup_arn = ?`, backupARN)
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

func (s *Store) AppendStreamRecord(ctx context.Context, tx *sql.Tx, record model.StreamRecord) error {
	keysJSON, err := json.Marshal(record.Keys)
	if err != nil {
		return err
	}
	var oldImage any
	if record.OldImage != nil {
		oldJSON, err := json.Marshal(record.OldImage)
		if err != nil {
			return err
		}
		oldImage = oldJSON
	}
	var newImage any
	if record.NewImage != nil {
		newJSON, err := json.Marshal(record.NewImage)
		if err != nil {
			return err
		}
		newImage = newJSON
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO stream_records(table_key, stream_arn, shard_id, event_name, keys_json, old_image_json, new_image_json, changed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, streamTableNameFromARN(record.StreamARN), record.StreamARN, firstNonEmpty(record.ShardID, "shardId-000000000000"), record.EventName, keysJSON, oldImage, newImage, record.ChangedAt)
	return err
}

func (s *Store) ListStreamRecordsAfterSequence(ctx context.Context, tx *sql.Tx, streamARN string, sequence int64, limit int) ([]model.StreamRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, shard_id, event_name, keys_json, old_image_json, new_image_json, changed_at
		FROM stream_records
		WHERE stream_arn = ? AND id > ?
		ORDER BY id ASC
		LIMIT ?
	`, streamARN, sequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]model.StreamRecord, 0)
	for rows.Next() {
		var rec model.StreamRecord
		var keysJSON []byte
		var oldJSON []byte
		var newJSON []byte
		if err := rows.Scan(&rec.Sequence, &rec.ShardID, &rec.EventName, &keysJSON, &oldJSON, &newJSON, &rec.ChangedAt); err != nil {
			return nil, err
		}
		rec.StreamARN = streamARN
		if err := json.Unmarshal(keysJSON, &rec.Keys); err != nil {
			return nil, err
		}
		if len(oldJSON) > 0 {
			if err := json.Unmarshal(oldJSON, &rec.OldImage); err != nil {
				return nil, err
			}
		}
		if len(newJSON) > 0 {
			if err := json.Unmarshal(newJSON, &rec.NewImage); err != nil {
				return nil, err
			}
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) GetStreamSequenceBounds(ctx context.Context, tx *sql.Tx, streamARN string) (int64, int64, bool, error) {
	var first int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM stream_records WHERE stream_arn = ? ORDER BY id ASC LIMIT 1`, streamARN).Scan(&first)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	var last int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM stream_records WHERE stream_arn = ? ORDER BY id DESC LIMIT 1`, streamARN).Scan(&last)
	if err != nil {
		return 0, 0, false, err
	}
	return first, last, true, nil
}

func (s *Store) GetStreamRecordChangedAt(ctx context.Context, tx *sql.Tx, streamARN string, sequence int64) (int64, bool, error) {
	var changedAt int64
	err := tx.QueryRowContext(ctx, `SELECT changed_at FROM stream_records WHERE stream_arn = ? AND id = ?`, streamARN, sequence).Scan(&changedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return changedAt, true, nil
}

func (s *Store) DeleteStreamRecordsBefore(ctx context.Context, tx *sql.Tx, streamARN string, before int64) (int64, error) {
	res, err := tx.ExecContext(ctx, `DELETE FROM stream_records WHERE stream_arn = ? AND changed_at < ?`, streamARN, before)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func streamTableNameFromARN(streamARN string) string {
	accountID := "000000000000"
	parts := strings.SplitN(strings.TrimSpace(streamARN), ":", 6)
	if len(parts) >= 5 && strings.TrimSpace(parts[4]) != "" {
		accountID = strings.TrimSpace(parts[4])
	}
	marker := "/table/"
	start := strings.Index(streamARN, marker)
	if start < 0 {
		marker = ":table/"
		start = strings.Index(streamARN, marker)
	}
	if start < 0 {
		return ""
	}
	remainder := streamARN[start+len(marker):]
	if slash := strings.Index(remainder, "/"); slash >= 0 {
		remainder = remainder[:slash]
	}
	remainder = strings.TrimSpace(remainder)
	if remainder == "" {
		return ""
	}
	return accountID + "#" + remainder
}

func (s *Store) PutResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string, policy string, revisionID string, updatedAt int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO resource_policies(resource_arn, policy_json, revision_id, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(resource_arn) DO UPDATE SET
			policy_json = excluded.policy_json,
			revision_id = excluded.revision_id,
			updated_at = excluded.updated_at
	`, resourceARN, policy, revisionID, updatedAt)
	return err
}

func (s *Store) GetResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string) (string, string, error) {
	var policy string
	var revisionID string
	err := tx.QueryRowContext(ctx, `SELECT policy_json, revision_id FROM resource_policies WHERE resource_arn = ?`, resourceARN).Scan(&policy, &revisionID)
	if err != nil {
		return "", "", err
	}
	return policy, revisionID, nil
}

func (s *Store) DeleteResourcePolicy(ctx context.Context, tx *sql.Tx, resourceARN string) (string, bool, error) {
	_, revisionID, err := s.GetResourcePolicy(ctx, tx, resourceARN)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM resource_policies WHERE resource_arn = ?`, resourceARN)
	if err != nil {
		return "", false, err
	}
	return revisionID, true, nil
}

type backupScanner interface {
	Scan(dest ...any) error
}

func scanBackupMetadata(scanner backupScanner) (model.Backup, error) {
	var backup model.Backup
	var sourceTableDetailsJSON string
	var sourceTableFeatureDetailsJSON string
	var snapshotTableJSON string
	if err := scanner.Scan(
		&backup.BackupARN,
		&backup.BackupName,
		&backup.TableName,
		&backup.TableARN,
		&backup.TableID,
		&backup.BackupStatus,
		&backup.BackupType,
		&backup.BackupCreationDateTime,
		&backup.BackupSizeBytes,
		&sourceTableDetailsJSON,
		&sourceTableFeatureDetailsJSON,
		&snapshotTableJSON,
	); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(sourceTableDetailsJSON), &backup.SourceTableDetails); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(sourceTableFeatureDetailsJSON), &backup.SourceTableFeatureDetails); err != nil {
		return model.Backup{}, err
	}
	if err := json.Unmarshal([]byte(snapshotTableJSON), &backup.SnapshotTable); err != nil {
		return model.Backup{}, err
	}
	return backup, nil
}

func loadBackupItems(ctx context.Context, tx *sql.Tx, backupARN string) ([]map[string]any, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT item_json
		FROM backup_items
		WHERE backup_arn = ?
		ORDER BY ordinal ASC
	`, backupARN)
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
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
