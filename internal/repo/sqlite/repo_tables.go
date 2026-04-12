package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/jdillenkofer/pinax/internal/model"
	"strings"
)

func (r tableRepo) CreateTable(ctx context.Context, t model.Table) error {
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
	_, err = r.tx.ExecContext(ctx, `
		INSERT INTO tables("key", hash_key, hash_type, range_key, range_type, billing_mode, read_capacity_units, write_capacity_units, table_class, deletion_protection_enabled, stream_enabled, stream_view_type, stream_arn, stream_label, sse_enabled, sse_type, sse_status, sse_kms_key_id, tags_json, table_status, table_status_at, created_at, ttl_enabled, ttl_attribute, ttl_status, ttl_status_at, pitr_enabled, pitr_recovery_period_days, pitr_enabled_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, t.Name, t.HashKey, t.HashType, nullIfEmpty(t.RangeKey), nullIfEmpty(t.RangeType), firstNonEmpty(t.BillingMode, "PAY_PER_REQUEST"), t.ReadCapacityUnits, t.WriteCapacityUnits, firstNonEmpty(t.TableClass, "STANDARD"), deletionProtection, streamEnabled, nullIfEmpty(t.Stream.ViewType), nullIfEmpty(t.Stream.ARN), nullIfEmpty(t.Stream.Label), sseEnabled, nullIfEmpty(t.SSE.SSEType), sseStatus, nullIfEmpty(t.SSE.KMSMasterKeyID), string(tagsJSON), nullIfEmpty(firstNonEmpty(t.Status, model.TableStatusActive)), t.StatusAt, t.CreatedAt, ttlEnabled, nullIfEmpty(t.TimeToLive.AttrName), ttlStatus, t.TimeToLive.StatusAt, pitrEnabled, pitrRecoveryDays, t.PITR.EnabledAt)
	if err != nil {
		return err
	}
	if err := r.replaceSecondaryIndexes(ctx, t.Name, t.GSIs, t.LSIs); err != nil {
		return err
	}
	return nil
}

func (r tableRepo) GetTable(ctx context.Context, name string) (model.Table, error) {
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
	var ttlEnabled int
	var ttlAttr sql.NullString
	var ttlStatus string
	var ttlStatusAt int64
	var pitrEnabled int
	var pitrRecoveryDays int64
	var pitrEnabledAt int64
	err := r.tx.QueryRowContext(ctx, `
		SELECT "key", hash_key, hash_type, range_key, range_type, billing_mode, read_capacity_units, write_capacity_units, table_class, deletion_protection_enabled, stream_enabled, stream_view_type, stream_arn, stream_label, sse_enabled, sse_type, sse_status, sse_kms_key_id, tags_json, table_status, table_status_at, created_at, ttl_enabled, ttl_attribute, ttl_status, ttl_status_at, pitr_enabled, pitr_recovery_period_days, pitr_enabled_at
		FROM tables
		WHERE "key" = ?
	`, name).Scan(&t.Name, &t.HashKey, &t.HashType, &rangeKey, &rangeType, &billingMode, &readCapacityUnits, &writeCapacityUnits, &tableClass, &deletionProtection, &streamEnabled, &streamViewType, &streamARN, &streamLabel, &sseEnabled, &sseType, &sseStatus, &sseKMSKeyID, &tagsJSON, &tableStatus, &tableStatusAt, &t.CreatedAt, &ttlEnabled, &ttlAttr, &ttlStatus, &ttlStatusAt, &pitrEnabled, &pitrRecoveryDays, &pitrEnabledAt)
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
	gsis, lsis, err := r.loadSecondaryIndexes(ctx, t.Name)
	if err != nil {
		return model.Table{}, err
	}
	t.GSIs = gsis
	t.LSIs = lsis
	t.TimeToLive.Enabled = ttlEnabled == 1
	t.TimeToLive.AttrName = ttlAttr.String
	t.TimeToLive.Status = ttlStatus
	t.TimeToLive.StatusAt = ttlStatusAt
	t.PITR.Enabled = pitrEnabled == 1
	t.PITR.RecoveryPeriodInDays = pitrRecoveryDays
	t.PITR.EnabledAt = pitrEnabledAt
	return t, nil
}

func (r tableRepo) ListTables(ctx context.Context, start string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.tx.QueryContext(ctx, `
		SELECT "key" FROM tables
		WHERE "key" > ?
		ORDER BY "key" ASC
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

func (r tableRepo) DeleteTable(ctx context.Context, name string) error {
	res, err := r.tx.ExecContext(ctx, `DELETE FROM tables WHERE "key" = ?`, name)
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

func (r tableRepo) BackfillGSIEntries(ctx context.Context, tableName string, gsi model.GlobalSecondaryIndex) error {
	const backfillPageSize = 500

	t, err := r.GetTable(ctx, tableName)
	if err != nil {
		return err
	}
	if _, err := r.tx.ExecContext(ctx, `DELETE FROM secondary_index_entries WHERE table_key = ? AND index_name = ?`, tableName, gsi.IndexName); err != nil {
		return err
	}

	startPK := ""
	startSK := ""
	for {
		items, err := r.item().Scan(ctx, tableName, startPK, startSK, backfillPageSize)
		if err != nil {
			return err
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			pk, sk, err := model.ExtractItemKeys(t, item)
			if err != nil {
				return err
			}
			startPK, startSK = pk, sk

			gpk, gsk, ok := model.ExtractGSIKeys(gsi, item)
			if !ok {
				continue
			}
			if _, err := r.tx.ExecContext(ctx, `
				INSERT INTO secondary_index_entries(table_key, index_name, index_pk, index_sk, base_pk, base_sk)
				VALUES (?, ?, ?, ?, ?, ?)
			`, tableName, gsi.IndexName, gpk, gsk, pk, sk); err != nil {
				return err
			}
		}

		if len(items) < backfillPageSize {
			break
		}
	}
	return nil
}

func (r tableRepo) loadSecondaryIndexes(ctx context.Context, tableName string) ([]model.GlobalSecondaryIndex, []model.LocalSecondaryIndex, error) {
	rows, err := r.tx.QueryContext(ctx, `
		SELECT index_name, index_type, hash_key, hash_type, range_key, range_type, index_status, index_status_at, read_capacity_units, write_capacity_units, projection_type
		FROM secondary_indexes
		WHERE table_key = ?
		ORDER BY index_name ASC
	`, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	type rowData struct {
		indexType string
		gsi       model.GlobalSecondaryIndex
		lsi       model.LocalSecondaryIndex
	}
	indexRows := make([]rowData, 0)
	for rows.Next() {
		var d rowData
		var hashKey sql.NullString
		var hashType sql.NullString
		var rangeKey sql.NullString
		var rangeType sql.NullString
		var status sql.NullString
		var projectionType sql.NullString
		if err := rows.Scan(&d.gsi.IndexName, &d.indexType, &hashKey, &hashType, &rangeKey, &rangeType, &status, &d.gsi.StatusAt, &d.gsi.ReadCapacity, &d.gsi.WriteCapacity, &projectionType); err != nil {
			return nil, nil, err
		}
		d.gsi.HashKey = hashKey.String
		d.gsi.HashType = hashType.String
		d.gsi.RangeKey = rangeKey.String
		d.gsi.RangeType = rangeType.String
		d.gsi.Status = status.String
		d.gsi.ProjectionType = firstNonEmpty(projectionType.String, "ALL")

		d.lsi.IndexName = d.gsi.IndexName
		d.lsi.RangeKey = d.gsi.RangeKey
		d.lsi.RangeType = d.gsi.RangeType
		d.lsi.ProjectionType = d.gsi.ProjectionType
		indexRows = append(indexRows, d)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	attrsRows, err := r.tx.QueryContext(ctx, `
		SELECT index_name, attr_name
		FROM secondary_index_non_key_attrs
		WHERE table_key = ?
		ORDER BY index_name ASC, ordinal ASC
	`, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer attrsRows.Close()

	nonKeyAttrsByIndex := map[string][]string{}
	for attrsRows.Next() {
		var indexName string
		var attrName string
		if err := attrsRows.Scan(&indexName, &attrName); err != nil {
			return nil, nil, err
		}
		nonKeyAttrsByIndex[indexName] = append(nonKeyAttrsByIndex[indexName], attrName)
	}
	if err := attrsRows.Err(); err != nil {
		return nil, nil, err
	}

	gsis := make([]model.GlobalSecondaryIndex, 0)
	lsis := make([]model.LocalSecondaryIndex, 0)
	for _, d := range indexRows {
		switch d.indexType {
		case "GSI":
			d.gsi.NonKeyAttrs = append([]string(nil), nonKeyAttrsByIndex[d.gsi.IndexName]...)
			gsis = append(gsis, d.gsi)
		case "LSI":
			d.lsi.NonKeyAttrs = append([]string(nil), nonKeyAttrsByIndex[d.lsi.IndexName]...)
			lsis = append(lsis, d.lsi)
		}
	}
	return gsis, lsis, nil
}

func (r tableRepo) replaceSecondaryIndexes(ctx context.Context, tableName string, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	existingRows, err := r.tx.QueryContext(ctx, `SELECT index_name FROM secondary_indexes WHERE table_key = ?`, tableName)
	if err != nil {
		return err
	}
	existing := map[string]struct{}{}
	for existingRows.Next() {
		var indexName string
		if err := existingRows.Scan(&indexName); err != nil {
			existingRows.Close()
			return err
		}
		existing[indexName] = struct{}{}
	}
	if err := existingRows.Err(); err != nil {
		existingRows.Close()
		return err
	}
	existingRows.Close()

	if _, err := r.tx.ExecContext(ctx, `DELETE FROM secondary_indexes WHERE table_key = ?`, tableName); err != nil {
		return err
	}
	if _, err := r.tx.ExecContext(ctx, `DELETE FROM secondary_index_non_key_attrs WHERE table_key = ?`, tableName); err != nil {
		return err
	}

	keep := map[string]struct{}{}

	for _, g := range gsis {
		_, err := r.tx.ExecContext(ctx, `
			INSERT INTO secondary_indexes(table_key, index_name, index_type, hash_key, hash_type, range_key, range_type, index_status, index_status_at, read_capacity_units, write_capacity_units, projection_type)
			VALUES (?, ?, 'GSI', ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, tableName, g.IndexName, g.HashKey, g.HashType, nullIfEmpty(g.RangeKey), nullIfEmpty(g.RangeType), firstNonEmpty(g.Status, model.IndexStatusActive), g.StatusAt, g.ReadCapacity, g.WriteCapacity, firstNonEmpty(g.ProjectionType, "ALL"))
		if err != nil {
			return err
		}
		keep[g.IndexName] = struct{}{}
		for i, attr := range g.NonKeyAttrs {
			if _, err := r.tx.ExecContext(ctx, `
				INSERT INTO secondary_index_non_key_attrs(table_key, index_name, attr_name, ordinal)
				VALUES (?, ?, ?, ?)
			`, tableName, g.IndexName, attr, i); err != nil {
				return err
			}
		}
	}

	for _, l := range lsis {
		_, err := r.tx.ExecContext(ctx, `
			INSERT INTO secondary_indexes(table_key, index_name, index_type, hash_key, hash_type, range_key, range_type, index_status, index_status_at, read_capacity_units, write_capacity_units, projection_type)
			VALUES (?, ?, 'LSI', NULL, NULL, ?, ?, 'ACTIVE', 0, 0, 0, ?)
		`, tableName, l.IndexName, l.RangeKey, l.RangeType, firstNonEmpty(l.ProjectionType, "ALL"))
		if err != nil {
			return err
		}
		keep[l.IndexName] = struct{}{}
		for i, attr := range l.NonKeyAttrs {
			if _, err := r.tx.ExecContext(ctx, `
				INSERT INTO secondary_index_non_key_attrs(table_key, index_name, attr_name, ordinal)
				VALUES (?, ?, ?, ?)
			`, tableName, l.IndexName, attr, i); err != nil {
				return err
			}
		}
	}

	for indexName := range existing {
		if _, ok := keep[indexName]; ok {
			continue
		}
		if _, err := r.tx.ExecContext(ctx, `DELETE FROM secondary_index_entries WHERE table_key = ? AND index_name = ?`, tableName, indexName); err != nil {
			return err
		}
	}

	return nil
}

func (r tableRepo) UpdateTableIndexes(ctx context.Context, tableName string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	if err := r.replaceSecondaryIndexes(ctx, tableName, gsis, lsis); err != nil {
		return err
	}
	_, err := r.tx.ExecContext(ctx, `
		UPDATE tables
		SET table_status = ?, table_status_at = ?
		WHERE "key" = ?
	`, firstNonEmpty(tableStatus, model.TableStatusActive), tableStatusAt, tableName)
	return err
}

func (r tableRepo) UpdateTableBilling(ctx context.Context, tableName string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error {
	_, err := r.tx.ExecContext(ctx, `
		UPDATE tables
		SET billing_mode = ?, read_capacity_units = ?, write_capacity_units = ?
		WHERE "key" = ?
	`, firstNonEmpty(billingMode, "PAY_PER_REQUEST"), readCapacityUnits, writeCapacityUnits, tableName)
	return err
}

func (r tableRepo) UpdateTableOptions(ctx context.Context, tableName string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error {
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
	_, err = r.tx.ExecContext(ctx, `
		UPDATE tables
		SET table_class = ?, deletion_protection_enabled = ?, stream_enabled = ?, stream_view_type = ?, stream_arn = ?, stream_label = ?, sse_enabled = ?, sse_type = ?, sse_status = ?, sse_kms_key_id = ?, tags_json = ?
		WHERE "key" = ?
	`, firstNonEmpty(tableClass, "STANDARD"), deletionProtectionInt, streamEnabled, nullIfEmpty(stream.ViewType), nullIfEmpty(stream.ARN), nullIfEmpty(stream.Label), sseEnabled, nullIfEmpty(sse.SSEType), firstNonEmpty(sse.Status, "DISABLED"), nullIfEmpty(sse.KMSMasterKeyID), string(tagsJSON), tableName)
	return err
}

func (r tableRepo) UpdateTimeToLive(ctx context.Context, tableName string, ttl model.TimeToLive) error {
	ttlEnabled := 0
	if ttl.Enabled {
		ttlEnabled = 1
	}
	_, err := r.tx.ExecContext(ctx, `
		UPDATE tables SET ttl_enabled = ?, ttl_attribute = ?, ttl_status = ?, ttl_status_at = ? WHERE "key" = ?
	`, ttlEnabled, nullIfEmpty(ttl.AttrName), ttl.Status, ttl.StatusAt, tableName)
	return err
}

func (r tableRepo) UpdatePointInTimeRecovery(ctx context.Context, tableName string, pitr model.PointInTimeRecovery) error {
	pitrEnabled := 0
	if pitr.Enabled {
		pitrEnabled = 1
	}
	recoveryDays := pitr.RecoveryPeriodInDays
	if recoveryDays <= 0 {
		recoveryDays = 35
	}
	_, err := r.tx.ExecContext(ctx, `
		UPDATE tables SET pitr_enabled = ?, pitr_recovery_period_days = ?, pitr_enabled_at = ? WHERE "key" = ?
	`, pitrEnabled, recoveryDays, pitr.EnabledAt, tableName)
	return err
}
