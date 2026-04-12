package httpapi

import (
	"database/sql"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	backupapp "github.com/jdillenkofer/pinax/internal/app/backup"
	pitrapp "github.com/jdillenkofer/pinax/internal/app/pitr"
	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/model"
)

var backupNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)

type createBackupRequest struct {
	BackupName string `json:"BackupName"`
	TableName  string `json:"TableName"`
}

type backupArnRequest struct {
	BackupArn string `json:"BackupArn"`
}

type listBackupsRequest struct {
	BackupType              string   `json:"BackupType"`
	ExclusiveStartBackupArn string   `json:"ExclusiveStartBackupArn"`
	Limit                   int      `json:"Limit"`
	TableName               string   `json:"TableName"`
	TimeRangeLowerBound     *float64 `json:"TimeRangeLowerBound"`
	TimeRangeUpperBound     *float64 `json:"TimeRangeUpperBound"`
}

type restoreTableFromBackupRequest struct {
	BackupArn                  string `json:"BackupArn"`
	TargetTableName            string `json:"TargetTableName"`
	BillingModeOverride        string `json:"BillingModeOverride"`
	OnDemandThroughputOverride *struct {
		MaxReadRequestUnits  int64 `json:"MaxReadRequestUnits"`
		MaxWriteRequestUnits int64 `json:"MaxWriteRequestUnits"`
	} `json:"OnDemandThroughputOverride"`
	ProvisionedThroughputOverride *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughputOverride"`
	SSESpecificationOverride *struct {
		Enabled        bool   `json:"Enabled"`
		SSEType        string `json:"SSEType"`
		KMSMasterKeyID string `json:"KMSMasterKeyId"`
	} `json:"SSESpecificationOverride"`
	GlobalSecondaryIndexOverride []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
	} `json:"GlobalSecondaryIndexOverride"`
	LocalSecondaryIndexOverride []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
	} `json:"LocalSecondaryIndexOverride"`
}

type updateContinuousBackupsRequest struct {
	TableName                        string `json:"TableName"`
	PointInTimeRecoverySpecification *struct {
		PointInTimeRecoveryEnabled *bool `json:"PointInTimeRecoveryEnabled"`
		RecoveryPeriodInDays       int64 `json:"RecoveryPeriodInDays"`
	} `json:"PointInTimeRecoverySpecification"`
}

type describeContinuousBackupsRequest struct {
	TableName string `json:"TableName"`
}

type restoreTableToPointInTimeRequest struct {
	SourceTableName            string   `json:"SourceTableName"`
	SourceTableArn             string   `json:"SourceTableArn"`
	TargetTableName            string   `json:"TargetTableName"`
	UseLatestRestorableTime    bool     `json:"UseLatestRestorableTime"`
	RestoreDateTime            *float64 `json:"RestoreDateTime"`
	BillingModeOverride        string   `json:"BillingModeOverride"`
	OnDemandThroughputOverride *struct {
		MaxReadRequestUnits  int64 `json:"MaxReadRequestUnits"`
		MaxWriteRequestUnits int64 `json:"MaxWriteRequestUnits"`
	} `json:"OnDemandThroughputOverride"`
	ProvisionedThroughputOverride *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughputOverride"`
	SSESpecificationOverride *struct {
		Enabled        bool   `json:"Enabled"`
		SSEType        string `json:"SSEType"`
		KMSMasterKeyID string `json:"KMSMasterKeyId"`
	} `json:"SSESpecificationOverride"`
	GlobalSecondaryIndexOverride []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
	} `json:"GlobalSecondaryIndexOverride"`
	LocalSecondaryIndexOverride []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
	} `json:"LocalSecondaryIndexOverride"`
}

func (s *Server) createBackup(r *http.Request, body []byte) (map[string]any, error) {
	var req createBackupRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.BackupName) == "" {
		return nil, awserr.Validation("BackupName is required")
	}
	if !backupNamePattern.MatchString(req.BackupName) {
		return nil, awserr.Validation("BackupName must match [a-zA-Z0-9_.-]+ and be between 3 and 255 characters")
	}
	if strings.TrimSpace(req.TableName) == "" {
		return nil, awserr.Validation("TableName is required")
	}

	tableKey, err := scopedTableNameFromIdentifier(r.Context(), req.TableName)
	if err != nil {
		return nil, err
	}

	backup, err := s.backupService.CreateBackup(
		r.Context(),
		tableKey,
		req.BackupName,
		time.Now().UnixMilli(),
		func(t model.Table, count int64, items []map[string]any) (model.Backup, error) {
			tableDesc := t.Description(count)
			now := time.Now().Unix()
			return model.Backup{
				BackupARN:                 localBackupARN(t.Name, req.BackupName, now),
				BackupName:                req.BackupName,
				TableName:                 t.Name,
				TableARN:                  anyString(tableDesc["TableArn"]),
				TableID:                   anyString(tableDesc["TableId"]),
				BackupStatus:              model.BackupStatusAvailable,
				BackupType:                model.BackupTypeUser,
				BackupCreationDateTime:    now,
				BackupSizeBytes:           estimateBackupSizeBytes(items),
				SourceTableDetails:        sourceTableDetailsFromDescription(tableDesc, count),
				SourceTableFeatureDetails: sourceTableFeatureDetailsFromDescription(tableDesc),
				SnapshotTable:             t,
				SnapshotItems:             items,
			}, nil
		},
	)
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(tableapp.ErrTableNotFound, badRequestAPIError("TableNotFoundException", "Cannot do operations on a non-existent table")),
			mapErr(backupapp.ErrTargetTableInUse, badRequestAPIError("TableInUseException", "A target table with the specified name is either being created or deleted.")),
			mapErr(backupapp.ErrBackupExists, badRequestAPIError("BackupInUseException", "Backup with the requested name already exists")),
		)
	}
	return map[string]any{"BackupDetails": backupDetailsMap(backup)}, nil
}

func (s *Server) describeBackup(r *http.Request, body []byte) (map[string]any, error) {
	var req backupArnRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.BackupArn) == "" {
		return nil, awserr.Validation("BackupArn is required")
	}
	if _, err := validateTableARNAccountForRequest(r.Context(), req.BackupArn); err != nil {
		return nil, err
	}

	backup, err := s.backupService.DescribeBackup(r.Context(), req.BackupArn)
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(sql.ErrNoRows, badRequestAPIError("BackupNotFoundException", "Backup not found for the given BackupARN.")),
		)
	}
	return map[string]any{"BackupDescription": backupDescriptionMap(backup)}, nil
}

func (s *Server) deleteBackup(r *http.Request, body []byte) (map[string]any, error) {
	var req backupArnRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.BackupArn) == "" {
		return nil, awserr.Validation("BackupArn is required")
	}
	if _, err := validateTableARNAccountForRequest(r.Context(), req.BackupArn); err != nil {
		return nil, err
	}

	deleted, err := s.backupService.DeleteBackup(r.Context(), req.BackupArn)
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(sql.ErrNoRows, badRequestAPIError("BackupNotFoundException", "Backup not found for the given BackupARN.")),
		)
	}
	return map[string]any{"BackupDescription": backupDescriptionMap(deleted)}, nil
}

func (s *Server) listBackups(r *http.Request, body []byte) (map[string]any, error) {
	var req listBackupsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	backupType := strings.TrimSpace(req.BackupType)
	if backupType == "" {
		backupType = model.BackupTypeUser
	}
	switch backupType {
	case model.BackupTypeUser, "SYSTEM", "AWS_BACKUP", "ALL":
	default:
		return nil, awserr.Validation("invalid BackupType")
	}
	if req.Limit < 0 || req.Limit > 100 {
		return nil, awserr.Validation("Limit must be between 1 and 100")
	}
	if req.Limit == 0 {
		req.Limit = 100
	}

	backups, err := s.backupService.ListBackups(r.Context())
	if err != nil {
		return nil, err
	}

	currentAccountID := accountIDFromContext(r.Context())
	tableNameFilter := normalizeTableNameIdentifier(req.TableName)
	tableARNFilter := strings.TrimSpace(req.TableName)
	if strings.HasPrefix(tableARNFilter, "arn:") {
		var err error
		tableNameFilter, err = validateTableARNAccountForRequest(r.Context(), tableARNFilter)
		if err != nil {
			return nil, err
		}
	}
	filtered := make([]model.Backup, 0, len(backups))
	for _, b := range backups {
		backupAccountID, backupTableName := splitScopedTableKey(b.TableName)
		if backupAccountID != currentAccountID {
			continue
		}
		if backupType == model.BackupTypeUser && b.BackupType != model.BackupTypeUser {
			continue
		}
		if backupType == "SYSTEM" || backupType == "AWS_BACKUP" {
			continue
		}
		if tableNameFilter != "" && backupTableName != tableNameFilter && b.TableARN != tableARNFilter {
			continue
		}
		if req.TimeRangeLowerBound != nil && float64(b.BackupCreationDateTime) < *req.TimeRangeLowerBound {
			continue
		}
		if req.TimeRangeUpperBound != nil && float64(b.BackupCreationDateTime) >= *req.TimeRangeUpperBound {
			continue
		}
		b.TableName = backupTableName
		filtered = append(filtered, b)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].BackupCreationDateTime == filtered[j].BackupCreationDateTime {
			return filtered[i].BackupARN > filtered[j].BackupARN
		}
		return filtered[i].BackupCreationDateTime > filtered[j].BackupCreationDateTime
	})

	start := 0
	if strings.TrimSpace(req.ExclusiveStartBackupArn) != "" {
		found := -1
		for i, b := range filtered {
			if b.BackupARN == req.ExclusiveStartBackupArn {
				found = i
				break
			}
		}
		if found < 0 {
			return nil, awserr.Validation("ExclusiveStartBackupArn not found")
		}
		start = found + 1
	}

	if start > len(filtered) {
		start = len(filtered)
	}
	end := start + req.Limit
	if end > len(filtered) {
		end = len(filtered)
	}
	page := filtered[start:end]

	summaries := make([]map[string]any, 0, len(page))
	for _, b := range page {
		summaries = append(summaries, backupSummaryMap(b))
	}

	resp := map[string]any{"BackupSummaries": summaries}
	if end < len(filtered) && len(page) > 0 {
		resp["LastEvaluatedBackupArn"] = page[len(page)-1].BackupARN
	}

	return resp, nil
}

func (s *Server) restoreTableFromBackup(r *http.Request, body []byte) (map[string]any, error) {
	var req restoreTableFromBackupRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.BackupArn) == "" {
		return nil, awserr.Validation("BackupArn is required")
	}
	if _, err := validateTableARNAccountForRequest(r.Context(), req.BackupArn); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.TargetTableName) == "" {
		return nil, awserr.Validation("TargetTableName is required")
	}
	if !backupNamePattern.MatchString(req.TargetTableName) {
		return nil, awserr.Validation("TargetTableName must match [a-zA-Z0-9_.-]+ and be between 3 and 255 characters")
	}

	targetScopedTableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), req.TargetTableName)
	tableToCreate, restoredItems, err := s.backupService.RestoreTableFromBackup(r.Context(), req.BackupArn, targetScopedTableKey, func(backup model.Backup) (model.Table, error) {
		t := backup.SnapshotTable
		t.Name = targetScopedTableKey
		t.Status = model.TableStatusCreating
		t.StatusAt = lifecycleNow() + lifecycleDelayMillis()
		t.CreatedAt = time.Now().Unix()
		t.PITR = model.PointInTimeRecovery{Enabled: false, RecoveryPeriodInDays: 35}

		billingMode := t.BillingMode
		readCapacity := t.ReadCapacityUnits
		writeCapacity := t.WriteCapacityUnits
		if strings.TrimSpace(req.BillingModeOverride) != "" || req.ProvisionedThroughputOverride != nil {
			var err error
			billingMode, readCapacity, writeCapacity, err = normalizeBillingConfig(req.BillingModeOverride, req.ProvisionedThroughputOverride)
			if err != nil {
				return model.Table{}, awserr.Validation(err.Error())
			}
		}
		t.BillingMode = billingMode
		t.ReadCapacityUnits = readCapacity
		t.WriteCapacityUnits = writeCapacity

		if req.SSESpecificationOverride != nil {
			normalizedSSE, err := normalizeSSESpecCreate(&struct {
				Enabled        bool   `json:"Enabled"`
				SSEType        string `json:"SSEType"`
				KMSMasterKeyID string `json:"KMSMasterKeyId"`
			}{
				Enabled:        req.SSESpecificationOverride.Enabled,
				SSEType:        req.SSESpecificationOverride.SSEType,
				KMSMasterKeyID: req.SSESpecificationOverride.KMSMasterKeyID,
			})
			if err != nil {
				return model.Table{}, awserr.Validation(err.Error())
			}
			t.SSE = normalizedSSE
		}

		if req.OnDemandThroughputOverride != nil {
			if req.OnDemandThroughputOverride.MaxReadRequestUnits < 0 || req.OnDemandThroughputOverride.MaxWriteRequestUnits < 0 {
				return model.Table{}, awserr.Validation("OnDemandThroughputOverride values must be greater than or equal to 0")
			}
		}

		updatedGSIs, err := applyRestoreGSIOverride(t.GSIs, req.GlobalSecondaryIndexOverride, t.BillingMode)
		if err != nil {
			return model.Table{}, awserr.Validation(err.Error())
		}
		t.GSIs = updatedGSIs
		updatedLSIs, err := applyRestoreLSIOverride(t.LSIs, req.LocalSecondaryIndexOverride)
		if err != nil {
			return model.Table{}, awserr.Validation(err.Error())
		}
		t.LSIs = updatedLSIs
		return t, nil
	})
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(sql.ErrNoRows, badRequestAPIError("BackupNotFoundException", "Backup not found for the given BackupARN.")),
			mapErr(backupapp.ErrTargetTableInUse, badRequestAPIError("TableInUseException", "A target table with the specified name is either being created or deleted.")),
			mapErr(backupapp.ErrTargetTableExists, badRequestAPIError("TableAlreadyExistsException", "A target table with the specified name already exists.")),
		)
	}

	return map[string]any{"TableDescription": tableToCreate.Description(int64(restoredItems))}, nil
}

func (s *Server) updateContinuousBackups(r *http.Request, body []byte) (map[string]any, error) {
	var req updateContinuousBackupsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.TableName) == "" {
		return nil, awserr.Validation("TableName is required")
	}
	if req.PointInTimeRecoverySpecification == nil || req.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled == nil {
		return nil, awserr.Validation("PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled is required")
	}
	if req.PointInTimeRecoverySpecification.RecoveryPeriodInDays != 0 {
		if req.PointInTimeRecoverySpecification.RecoveryPeriodInDays < 1 || req.PointInTimeRecoverySpecification.RecoveryPeriodInDays > 35 {
			return nil, awserr.Validation("RecoveryPeriodInDays must be between 1 and 35")
		}
	}
	tableKey, err := scopedTableNameFromIdentifier(r.Context(), req.TableName)
	if err != nil {
		return nil, err
	}
	t, nowMs, err := s.pitrService.UpdateContinuousBackups(r.Context(), pitrapp.UpdateContinuousBackupsInput{
		TableKey:              tableKey,
		Enable:                *req.PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled,
		RecoveryPeriodInDays:  req.PointInTimeRecoverySpecification.RecoveryPeriodInDays,
		NowMillis:             time.Now().UnixMilli(),
		DefaultRecoveryWindow: 35,
	})
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(tableapp.ErrTableNotFound, badRequestAPIError("TableNotFoundException", "Cannot do operations on a non-existent table")),
		)
	}
	return map[string]any{"ContinuousBackupsDescription": pitrapp.ContinuousBackupsDescription(t, nowMs, s.pitrLatestRestorableLagMillis)}, nil
}

func (s *Server) describeContinuousBackups(r *http.Request, body []byte) (map[string]any, error) {
	var req describeContinuousBackupsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.TableName) == "" {
		return nil, awserr.Validation("TableName is required")
	}
	tableKey, err := scopedTableNameFromIdentifier(r.Context(), req.TableName)
	if err != nil {
		return nil, err
	}

	t, nowMs, err := s.pitrService.DescribeContinuousBackups(r.Context(), tableKey, time.Now().UnixMilli())
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(tableapp.ErrTableNotFound, badRequestAPIError("TableNotFoundException", "Cannot do operations on a non-existent table")),
		)
	}
	return map[string]any{"ContinuousBackupsDescription": pitrapp.ContinuousBackupsDescription(t, nowMs, s.pitrLatestRestorableLagMillis)}, nil
}

func (s *Server) restoreTableToPointInTime(r *http.Request, body []byte) (map[string]any, error) {
	var req restoreTableToPointInTimeRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.TargetTableName) == "" {
		return nil, awserr.Validation("TargetTableName is required")
	}
	if !backupNamePattern.MatchString(req.TargetTableName) {
		return nil, awserr.Validation("TargetTableName must match [a-zA-Z0-9_.-]+ and be between 3 and 255 characters")
	}
	if strings.TrimSpace(req.SourceTableName) == "" && strings.TrimSpace(req.SourceTableArn) == "" {
		return nil, awserr.Validation("SourceTableName or SourceTableArn is required")
	}
	if strings.TrimSpace(req.SourceTableName) != "" && strings.TrimSpace(req.SourceTableArn) != "" {
		return nil, awserr.Validation("Specify only one of SourceTableName or SourceTableArn")
	}
	if req.UseLatestRestorableTime && req.RestoreDateTime != nil {
		return nil, awserr.Validation("UseLatestRestorableTime and RestoreDateTime are mutually exclusive")
	}
	if !req.UseLatestRestorableTime && req.RestoreDateTime == nil {
		return nil, awserr.Validation("RestoreDateTime is required unless UseLatestRestorableTime is true")
	}

	sourceName := firstNonEmpty(strings.TrimSpace(req.SourceTableName), strings.TrimSpace(req.SourceTableArn))
	sourceTableKey, err := scopedTableNameFromIdentifier(r.Context(), sourceName)
	if err != nil {
		return nil, err
	}
	restoreAtMillis := int64(0)
	if req.RestoreDateTime != nil {
		restoreAtMillis = int64(math.Round(*req.RestoreDateTime * 1000))
	}

	targetScopedTableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), req.TargetTableName)
	tableToCreate, count, err := s.pitrService.RestoreTableToPointInTime(r.Context(), pitrapp.RestoreTableToPointInTimeInput{
		SourceTableKey:            sourceTableKey,
		TargetScopedTableKey:      targetScopedTableKey,
		UseLatestRestorableTime:   req.UseLatestRestorableTime,
		RestoreDateTimeMillis:     restoreAtMillis,
		PITRLatestRestorableLagMs: s.pitrLatestRestorableLagMillis,
		NowMillis:                 time.Now().UnixMilli(),
	}, func(source model.Table) (model.Table, error) {
		table := source
		table.Name = targetScopedTableKey
		table.Status = model.TableStatusCreating
		table.StatusAt = lifecycleNow() + lifecycleDelayMillis()
		table.CreatedAt = time.Now().Unix()
		table.Tags = nil
		table.Stream = model.StreamSpecification{}
		table.TimeToLive = model.TimeToLive{Enabled: false, Status: model.TTLStatusDisabled}
		table.PITR = model.PointInTimeRecovery{Enabled: false, RecoveryPeriodInDays: source.PITR.RecoveryPeriodInDays}

		billingMode := table.BillingMode
		readCapacity := table.ReadCapacityUnits
		writeCapacity := table.WriteCapacityUnits
		if strings.TrimSpace(req.BillingModeOverride) != "" || req.ProvisionedThroughputOverride != nil {
			var err error
			billingMode, readCapacity, writeCapacity, err = normalizeBillingConfig(req.BillingModeOverride, req.ProvisionedThroughputOverride)
			if err != nil {
				return model.Table{}, awserr.Validation(err.Error())
			}
		}
		table.BillingMode = billingMode
		table.ReadCapacityUnits = readCapacity
		table.WriteCapacityUnits = writeCapacity

		if req.SSESpecificationOverride != nil {
			normalizedSSE, err := normalizeSSESpecCreate(&struct {
				Enabled        bool   `json:"Enabled"`
				SSEType        string `json:"SSEType"`
				KMSMasterKeyID string `json:"KMSMasterKeyId"`
			}{
				Enabled:        req.SSESpecificationOverride.Enabled,
				SSEType:        req.SSESpecificationOverride.SSEType,
				KMSMasterKeyID: req.SSESpecificationOverride.KMSMasterKeyID,
			})
			if err != nil {
				return model.Table{}, awserr.Validation(err.Error())
			}
			table.SSE = normalizedSSE
		}
		if req.OnDemandThroughputOverride != nil {
			if req.OnDemandThroughputOverride.MaxReadRequestUnits < 0 || req.OnDemandThroughputOverride.MaxWriteRequestUnits < 0 {
				return model.Table{}, awserr.Validation("OnDemandThroughputOverride values must be greater than or equal to 0")
			}
		}

		updatedGSIs, err := applyRestoreGSIOverride(table.GSIs, req.GlobalSecondaryIndexOverride, table.BillingMode)
		if err != nil {
			return model.Table{}, awserr.Validation(err.Error())
		}
		table.GSIs = updatedGSIs
		updatedLSIs, err := applyRestoreLSIOverride(table.LSIs, req.LocalSecondaryIndexOverride)
		if err != nil {
			return model.Table{}, awserr.Validation(err.Error())
		}
		table.LSIs = updatedLSIs

		return table, nil
	})
	if err != nil {
		return nil, mapAPIError(err,
			mapErr(tableapp.ErrTableNotFound, badRequestAPIError("TableNotFoundException", "Cannot do operations on a non-existent table")),
			mapErr(pitrapp.ErrPointInTimeRecoveryUnavailable, badRequestAPIError("PointInTimeRecoveryUnavailableException", "Point in time recovery has not yet been enabled for this source table.")),
			mapErr(pitrapp.ErrInvalidRestoreTime, badRequestAPIError("InvalidRestoreTimeException", "RestoreDateTime must be between EarliestRestorableDateTime and LatestRestorableDateTime.")),
			mapErr(pitrapp.ErrTargetTableInUse, badRequestAPIError("TableInUseException", "A target table with the specified name is either being created or deleted.")),
			mapErr(pitrapp.ErrTargetTableExists, badRequestAPIError("TableAlreadyExistsException", "A target table with the specified name already exists.")),
		)
	}

	return map[string]any{"TableDescription": tableToCreate.Description(count)}, nil
}
