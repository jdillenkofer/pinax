package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/expr"
	"github.com/jdillenkofer/pinax/internal/identity"
	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
)

type createTableRequest struct {
	TableName             string `json:"TableName"`
	BillingMode           string `json:"BillingMode"`
	TableClass            string `json:"TableClass"`
	DeletionProtection    bool   `json:"DeletionProtectionEnabled"`
	ProvisionedThroughput *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughput"`
	StreamSpecification *struct {
		StreamEnabled  bool   `json:"StreamEnabled"`
		StreamViewType string `json:"StreamViewType"`
	} `json:"StreamSpecification"`
	SSESpecification *struct {
		Enabled        bool   `json:"Enabled"`
		SSEType        string `json:"SSEType"`
		KMSMasterKeyID string `json:"KMSMasterKeyId"`
	} `json:"SSESpecification"`
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
	AttributeDefinitions []struct {
		AttributeName string `json:"AttributeName"`
		AttributeType string `json:"AttributeType"`
	} `json:"AttributeDefinitions"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType       string `json:"KeyType"`
	} `json:"KeySchema"`
	GlobalSecondaryIndexes []struct {
		IndexName             string `json:"IndexName"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
	} `json:"GlobalSecondaryIndexes"`
	LocalSecondaryIndexes []struct {
		IndexName string `json:"IndexName"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
	} `json:"LocalSecondaryIndexes"`
}

func (s *Server) createTable(r *http.Request, body []byte) (map[string]any, error) {
	var req createTableRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if req.TableName == "" {
		return nil, awserr.Validation("TableName is required")
	}

	attrType := map[string]string{}
	for _, d := range req.AttributeDefinitions {
		if d.AttributeName != "" {
			attrType[d.AttributeName] = d.AttributeType
		}
	}

	var hashKey string
	var rangeKey string
	for _, k := range req.KeySchema {
		switch k.KeyType {
		case "HASH":
			hashKey = k.AttributeName
		case "RANGE":
			rangeKey = k.AttributeName
		}
	}
	if hashKey == "" {
		return nil, awserr.Validation("KeySchema must include HASH key")
	}
	hashType := attrType[hashKey]
	if hashType == "" {
		return nil, awserr.Validation("AttributeDefinitions missing HASH key type")
	}
	billingMode, readCapacityUnits, writeCapacityUnits, err := normalizeBillingConfig(req.BillingMode, req.ProvisionedThroughput)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	tableClass, err := normalizeTableClass(req.TableClass)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	streamSpec, err := normalizeStreamSpec(req.StreamSpecification)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	accountID := accountIDFromContext(r.Context())
	scopedTableKey := scopedTableKeyFromAccountAndName(accountID, req.TableName)
	if streamSpec.Enabled {
		setStreamMetadata(&streamSpec, accountID, req.TableName)
	}
	sseSpec, err := normalizeSSESpecCreate(req.SSESpecification)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	tags, err := normalizeTags(req.Tags)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	now := lifecycleNow()
	t := model.Table{
		Name:               scopedTableKey,
		HashKey:            hashKey,
		HashType:           hashType,
		RangeKey:           rangeKey,
		RangeType:          attrType[rangeKey],
		BillingMode:        billingMode,
		ReadCapacityUnits:  readCapacityUnits,
		WriteCapacityUnits: writeCapacityUnits,
		TableClass:         tableClass,
		DeletionProtection: req.DeletionProtection,
		Status:             model.TableStatusCreating,
		StatusAt:           now,
		Stream:             streamSpec,
		SSE:                sseSpec,
		Tags:               tags,
		CreatedAt:          time.Now().Unix(),
	}

	indexNames := map[string]struct{}{}

	for _, g := range req.GlobalSecondaryIndexes {
		var gHash string
		var gRange string
		for _, k := range g.KeySchema {
			switch k.KeyType {
			case "HASH":
				gHash = k.AttributeName
			case "RANGE":
				gRange = k.AttributeName
			}
		}
		if gHash == "" {
			return nil, awserr.Validation("GSI KeySchema must include HASH key")
		}
		if g.IndexName == "" {
			return nil, awserr.Validation("GSI IndexName is required")
		}
		if _, exists := indexNames[g.IndexName]; exists {
			return nil, awserr.Validation("duplicate secondary index name " + g.IndexName)
		}
		indexNames[g.IndexName] = struct{}{}
		projectionType, nonKeyAttrs, err := normalizeIndexProjection(g.Projection.ProjectionType, g.Projection.NonKeyAttributes)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		gReadCapacity, gWriteCapacity, err := normalizeGSIThroughputForCreate(billingMode, g.ProvisionedThroughput)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		t.GSIs = append(t.GSIs, model.GlobalSecondaryIndex{
			IndexName:      g.IndexName,
			HashKey:        gHash,
			HashType:       attrType[gHash],
			RangeKey:       gRange,
			RangeType:      attrType[gRange],
			Status:         model.IndexStatusActive,
			ReadCapacity:   gReadCapacity,
			WriteCapacity:  gWriteCapacity,
			ProjectionType: projectionType,
			NonKeyAttrs:    nonKeyAttrs,
		})
	}

	for _, l := range req.LocalSecondaryIndexes {
		if t.RangeKey == "" {
			return nil, awserr.Validation("LocalSecondaryIndexes require table RANGE key")
		}
		if l.IndexName == "" {
			return nil, awserr.Validation("LSI IndexName is required")
		}
		if _, exists := indexNames[l.IndexName]; exists {
			return nil, awserr.Validation("duplicate secondary index name " + l.IndexName)
		}
		indexNames[l.IndexName] = struct{}{}

		var lHash string
		var lRange string
		for _, k := range l.KeySchema {
			switch k.KeyType {
			case "HASH":
				lHash = k.AttributeName
			case "RANGE":
				lRange = k.AttributeName
			}
		}
		if lHash == "" || lRange == "" {
			return nil, awserr.Validation("LSI KeySchema must include HASH and RANGE keys")
		}
		if lHash != t.HashKey {
			return nil, awserr.Validation("LSI HASH key must match table HASH key")
		}
		if attrType[lRange] == "" {
			return nil, awserr.Validation("AttributeDefinitions missing LSI RANGE key type")
		}
		projectionType, nonKeyAttrs, err := normalizeIndexProjection(l.Projection.ProjectionType, l.Projection.NonKeyAttributes)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}

		t.LSIs = append(t.LSIs, model.LocalSecondaryIndex{
			IndexName:      l.IndexName,
			RangeKey:       lRange,
			RangeType:      attrType[lRange],
			ProjectionType: projectionType,
			NonKeyAttrs:    nonKeyAttrs,
		})
	}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		if err := repos.Tables().CreateTable(txCtx, t); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "unique") {
				return awserr.ResourceInUse("Table already exists: " + req.TableName)
			}
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return map[string]any{"TableDescription": t.Description(0)}, nil
}

type tableNameRequest struct {
	TableName string `json:"TableName"`
}

func (s *Server) describeTable(r *http.Request, body []byte) (map[string]any, error) {
	var req tableNameRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	var t model.Table
	count := int64(0)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		count, err = repos.Items().CountItems(txCtx, t.Name)
		return err
	}); err != nil {
		return nil, err
	}

	return map[string]any{"Table": t.Description(count)}, nil
}

func (s *Server) describeLimits(_ *http.Request, body []byte) (map[string]any, error) {
	var req struct{}
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	return map[string]any{
		"AccountMaxReadCapacityUnits":  defaultAccountMaxReadCapacityUnits,
		"AccountMaxWriteCapacityUnits": defaultAccountMaxWriteCapacityUnits,
		"TableMaxReadCapacityUnits":    defaultTableMaxReadCapacityUnits,
		"TableMaxWriteCapacityUnits":   defaultTableMaxWriteCapacityUnits,
	}, nil
}

func (s *Server) describeEndpoints(r *http.Request, body []byte) (map[string]any, error) {
	var req struct{}
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	address := strings.TrimSpace(r.Host)
	if address == "" {
		address = "localhost"
	}
	return map[string]any{
		"Endpoints": []map[string]any{{
			"Address":              address,
			"CachePeriodInMinutes": defaultDescribeEndpointsCachePeriod,
		}},
	}, nil
}

type listTablesRequest struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName"`
	Limit                   int    `json:"Limit"`
}

func (s *Server) listTables(r *http.Request, body []byte) (map[string]any, error) {
	var req listTablesRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	accountID := accountIDFromContext(r.Context())
	startName := ""
	if strings.TrimSpace(req.ExclusiveStartTableName) != "" {
		startName = scopedTableKeyFromAccountAndName(accountID, req.ExclusiveStartTableName)
	}
	batchLimit := req.Limit
	if batchLimit <= 0 {
		batchLimit = 100
	}
	filtered := make([]string, 0, batchLimit)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for {
			names, err := repos.Tables().ListTables(txCtx, startName, batchLimit)
			if err != nil {
				return err
			}
			if len(names) == 0 {
				break
			}
			for _, name := range names {
				startName = name
				storedAccountID, _ := splitScopedTableKey(name)
				if storedAccountID != accountID {
					continue
				}
				t, err := s.getTableWithLifecycleByKey(txCtx, repos.Tables(), name)
				if err != nil {
					var apiErr *awserr.APIError
					if errors.As(err, &apiErr) && apiErr.Code == "ResourceNotFoundException" {
						continue
					}
					return err
				}
				filtered = append(filtered, logicalTableNameFromKey(t.Name))
				if req.Limit > 0 && len(filtered) >= req.Limit {
					break
				}
			}
			if req.Limit > 0 && len(filtered) >= req.Limit {
				break
			}
			if len(names) < batchLimit {
				break
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"TableNames": filtered}
	if req.Limit > 0 && len(filtered) >= req.Limit {
		resp["LastEvaluatedTableName"] = filtered[len(filtered)-1]
	}
	return resp, nil
}

func (s *Server) deleteTable(r *http.Request, body []byte) (map[string]any, error) {
	var req tableNameRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	var t model.Table
	count := int64(0)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		if t.Status == model.TableStatusDeleting {
			return awserr.ResourceInUse("Table is currently " + t.Status)
		}
		count, err = repos.Items().CountItems(txCtx, t.Name)
		if err != nil {
			return err
		}
		t.Status = model.TableStatusDeleting
		t.StatusAt = lifecycleNow() + lifecycleDelayMillis()
		return repos.Tables().UpdateTableIndexes(txCtx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs)
	}); err != nil {
		return nil, err
	}

	return map[string]any{"TableDescription": t.Description(count)}, nil
}

func keyAttributesFromItem(t model.Table, item map[string]any) map[string]any {
	keys := map[string]any{}
	keys[t.HashKey] = item[t.HashKey]
	if t.RangeKey != "" {
		keys[t.RangeKey] = item[t.RangeKey]
	}
	return keys
}

func keyAttributesFromKey(t model.Table, key map[string]any) map[string]any {
	keys := map[string]any{}
	keys[t.HashKey] = key[t.HashKey]
	if t.RangeKey != "" {
		keys[t.RangeKey] = key[t.RangeKey]
	}
	return keys
}

func (s *Server) emitMutationEventForWrite(ctx context.Context, repos uow.Repos, t model.Table, eventName string, keys, oldImage, newImage map[string]any, changedAt int64) error {
	pk, sk, err := primaryKeyStringsFromMutationKeys(t, keys)
	if err != nil {
		return err
	}
	return s.mutationExecutor.Emit(ctx, repos, mutation.Event{
		Table:     t,
		EventName: eventName,
		PK:        pk,
		SK:        sk,
		Keys:      keys,
		OldImage:  oldImage,
		NewImage:  newImage,
		ChangedAt: changedAt,
	})
}

func primaryKeyStringsFromMutationKeys(t model.Table, keys map[string]any) (string, string, error) {
	hashValue, ok := keys[t.HashKey]
	if !ok {
		return "", "", fmt.Errorf("missing hash key attribute in mutation event")
	}
	pk, err := model.SerializeKeyValue(hashValue)
	if err != nil {
		return "", "", err
	}
	sk := model.NoSortKey
	if strings.TrimSpace(t.RangeKey) != "" {
		rangeValue, ok := keys[t.RangeKey]
		if !ok {
			return "", "", fmt.Errorf("missing range key attribute in mutation event")
		}
		sk, err = model.SerializeKeyValue(rangeValue)
		if err != nil {
			return "", "", err
		}
	}
	return pk, sk, nil
}

type putItemRequest struct {
	TableName                           string            `json:"TableName"`
	Item                                map[string]any    `json:"Item"`
	ReturnValues                        string            `json:"ReturnValues"`
	ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
	ConditionExpression                 string            `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
	ReturnConsumedCapacity              string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) putItem(r *http.Request, body []byte) (map[string]any, error) {
	var req putItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if model.ItemTooLarge(req.Item) {
		return nil, awserr.Validation("Item size has exceeded the maximum allowed size")
	}
	if err := validateReturnValuesOnConditionCheckFailure(req.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	var (
		t          model.Table
		current    map[string]any
		existed    bool
		writeUnits float64
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var txErr error
		t, txErr = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if txErr != nil {
			return txErr
		}
		pk, sk, txErr := model.ExtractItemKeys(t, req.Item)
		if txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		if txErr := model.ValidateSecondaryIndexKeyTypes(t, req.Item); txErr != nil {
			return awserr.Validation(txErr.Error())
		}

		current, txErr = repos.Items().GetItem(txCtx, t.Name, pk, sk)
		existed = true
		if txErr != nil && !errors.Is(txErr, sql.ErrNoRows) {
			return txErr
		}
		if errors.Is(txErr, sql.ErrNoRows) {
			existed = false
			current = map[string]any{}
		}
		condOK, txErr := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if txErr != nil {
			return awserr.Validation(conditionExpressionValidationMessage(txErr))
		}
		if !condOK {
			item := itemForConditionFailure(req.ReturnValuesOnConditionCheckFailure, current, existed)
			return awserr.ConditionalCheckFailedWithItem("The conditional request failed", item)
		}
		writeUnits = model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(req.Item))
		if txErr := s.ensureWriteCapacity(t, writeUnits); txErr != nil {
			return txErr
		}
		changedAt := time.Now().UnixMilli()
		if txErr := repos.Items().PutItem(txCtx, t, pk, sk, req.Item); txErr != nil {
			return txErr
		}
		eventName := "INSERT"
		streamOld := current
		if existed {
			eventName = "MODIFY"
		} else {
			streamOld = nil
		}
		return s.emitMutationEventForWrite(txCtx, repos, t, eventName, keyAttributesFromItem(t, req.Item), streamOld, req.Item, changedAt)
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, writeUnits)

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return resp, nil
	}
	if req.ReturnValues != "ALL_OLD" {
		return nil, awserr.Validation("PutItem ReturnValues must be NONE or ALL_OLD")
	}
	if existed {
		resp["Attributes"] = current
	}
	return resp, nil
}

type getItemRequest struct {
	TableName                string            `json:"TableName"`
	Key                      map[string]any    `json:"Key"`
	AttributesToGet          []string          `json:"AttributesToGet"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
	ConsistentRead           bool              `json:"ConsistentRead"`
	ReturnConsumedCapacity   string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) getItem(r *http.Request, body []byte) (map[string]any, error) {
	var req getItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	projectionExpression, err := normalizeLegacyAttributesProjection(req.AttributesToGet, req.ProjectionExpression)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if _, err := applyProjection(map[string]any{}, projectionExpression, req.ExpressionAttributeNames); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	var t model.Table
	item := map[string]any{}
	readUnits := 0.0
	found := false
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		pk, sk, err := model.ExtractKey(t, req.Key)
		if err != nil {
			return awserr.Validation(err.Error())
		}
		item, err = repos.Items().GetItem(txCtx, t.Name, pk, sk)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				readUnits = model.CalculateReadCapacityUnits(1, req.ConsistentRead)
				return nil
			}
			return err
		}
		found = true
		readUnits = model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := s.ensureReadCapacity(t, readUnits); err != nil {
		return nil, err
	}
	if !found {
		resp := map[string]any{}
		setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, readUnits, 0)
		return resp, nil
	}

	projected, err := applyProjection(item, projectionExpression, req.ExpressionAttributeNames)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resp := map[string]any{"Item": projected}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, readUnits, 0)
	return resp, nil
}

type deleteItemRequest struct {
	TableName                           string            `json:"TableName"`
	Key                                 map[string]any    `json:"Key"`
	ReturnValues                        string            `json:"ReturnValues"`
	ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
	ConditionExpression                 string            `json:"ConditionExpression"`
	ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
	ReturnConsumedCapacity              string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) deleteItem(r *http.Request, body []byte) (map[string]any, error) {
	var req deleteItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateReturnValuesOnConditionCheckFailure(req.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	var (
		t          model.Table
		current    map[string]any
		existed    bool
		writeUnits float64
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var txErr error
		t, txErr = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if txErr != nil {
			return txErr
		}
		pk, sk, txErr := model.ExtractKey(t, req.Key)
		if txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		current, txErr = repos.Items().GetItem(txCtx, t.Name, pk, sk)
		existed = true
		if txErr != nil && !errors.Is(txErr, sql.ErrNoRows) {
			return txErr
		}
		if errors.Is(txErr, sql.ErrNoRows) {
			existed = false
			current = map[string]any{}
		}
		condOK, txErr := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if txErr != nil {
			return awserr.Validation(conditionExpressionValidationMessage(txErr))
		}
		if !condOK {
			item := itemForConditionFailure(req.ReturnValuesOnConditionCheckFailure, current, existed)
			return awserr.ConditionalCheckFailedWithItem("The conditional request failed", item)
		}
		writeUnits = model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
		if txErr := s.ensureWriteCapacity(t, writeUnits); txErr != nil {
			return txErr
		}
		changedAt := time.Now().UnixMilli()
		if txErr := repos.Items().DeleteItem(txCtx, t.Name, pk, sk); txErr != nil {
			return txErr
		}
		if existed {
			return s.emitMutationEventForWrite(txCtx, repos, t, "REMOVE", keyAttributesFromItem(t, current), current, nil, changedAt)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, writeUnits)

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return resp, nil
	}
	if req.ReturnValues != "ALL_OLD" {
		return nil, awserr.Validation("DeleteItem ReturnValues must be NONE or ALL_OLD")
	}
	if existed {
		resp["Attributes"] = current
	}
	return resp, nil
}

type updateItemRequest struct {
	TableName                 string            `json:"TableName"`
	Key                       map[string]any    `json:"Key"`
	UpdateExpression          string            `json:"UpdateExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	ConditionExpression       string            `json:"ConditionExpression"`
	ReturnValues              string            `json:"ReturnValues"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) updateItem(r *http.Request, body []byte) (map[string]any, error) {
	var req updateItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	var (
		t           model.Table
		oldItem     map[string]any
		updated     map[string]any
		plan        updatePlan
		itemExisted bool
		writeUnits  float64
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var txErr error
		t, txErr = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if txErr != nil {
			return txErr
		}
		pk, sk, txErr := model.ExtractKey(t, req.Key)
		if txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		current, txErr := repos.Items().GetItem(txCtx, t.Name, pk, sk)
		oldItem = map[string]any{}
		itemExisted = true
		if txErr != nil {
			if errors.Is(txErr, sql.ErrNoRows) {
				itemExisted = false
				current = map[string]any{t.HashKey: req.Key[t.HashKey]}
				if t.RangeKey != "" {
					current[t.RangeKey] = req.Key[t.RangeKey]
				}
			} else {
				return txErr
			}
		}
		if itemExisted {
			oldItem = cloneItem(current)
		}

		condOK, txErr := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if txErr != nil {
			return awserr.Validation(conditionExpressionValidationMessage(txErr))
		}
		if !condOK {
			return awserr.ConditionalCheckFailed("The conditional request failed")
		}

		plan, txErr = parseUpdateExpression(req.UpdateExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		updated, _, txErr = applyUpdatePlan(current, plan)
		if txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		if txErr := model.ValidateSecondaryIndexKeyTypes(t, updated); txErr != nil {
			return awserr.Validation(txErr.Error())
		}
		if model.ItemTooLarge(updated) {
			return awserr.Validation("Item size has exceeded the maximum allowed size")
		}
		writeUnits = model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated))
		if txErr := s.ensureWriteCapacity(t, writeUnits); txErr != nil {
			return txErr
		}
		changedAt := time.Now().UnixMilli()
		if txErr := repos.Items().PutItem(txCtx, t, pk, sk, updated); txErr != nil {
			return txErr
		}
		eventName := "INSERT"
		streamOld := oldItem
		if itemExisted {
			eventName = "MODIFY"
		} else {
			streamOld = nil
		}
		return s.emitMutationEventForWrite(txCtx, repos, t, eventName, keyAttributesFromKey(t, req.Key), streamOld, updated, changedAt)
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, writeUnits)

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return resp, nil
	}
	attributes := map[string]any{}
	switch req.ReturnValues {
	case "ALL_OLD":
		if itemExisted {
			attributes = oldItem
		}
	case "UPDATED_OLD":
		for attr := range plan.TouchedAttrs {
			if v, ok := oldItem[attr]; ok {
				attributes[attr] = v
			}
		}
	case "ALL_NEW":
		attributes = updated
	case "UPDATED_NEW":
		for attr := range plan.TouchedAttrs {
			if v, ok := updated[attr]; ok {
				attributes[attr] = v
			}
		}
	default:
		return nil, awserr.Validation("unsupported UpdateItem ReturnValues")
	}

	if len(attributes) == 0 {
		return resp, nil
	}
	resp["Attributes"] = attributes
	return resp, nil
}

type updateTableRequest struct {
	TableName             string `json:"TableName"`
	BillingMode           string `json:"BillingMode"`
	TableClass            string `json:"TableClass"`
	DeletionProtection    *bool  `json:"DeletionProtectionEnabled"`
	ProvisionedThroughput *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughput"`
	StreamSpecification *struct {
		StreamEnabled  bool   `json:"StreamEnabled"`
		StreamViewType string `json:"StreamViewType"`
	} `json:"StreamSpecification"`
	SSESpecification *struct {
		Enabled        *bool  `json:"Enabled"`
		SSEType        string `json:"SSEType"`
		KMSMasterKeyID string `json:"KMSMasterKeyId"`
	} `json:"SSESpecification"`
	ReplicaUpdates       []any `json:"ReplicaUpdates"`
	AttributeDefinitions []struct {
		AttributeName string `json:"AttributeName"`
		AttributeType string `json:"AttributeType"`
	} `json:"AttributeDefinitions"`
	GlobalSecondaryIndexUpdates []struct {
		Create struct {
			IndexName             string `json:"IndexName"`
			ProvisionedThroughput *struct {
				ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
				WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
			} `json:"ProvisionedThroughput"`
			KeySchema []struct {
				AttributeName string `json:"AttributeName"`
				KeyType       string `json:"KeyType"`
			} `json:"KeySchema"`
			Projection struct {
				ProjectionType   string   `json:"ProjectionType"`
				NonKeyAttributes []string `json:"NonKeyAttributes"`
			} `json:"Projection"`
		} `json:"Create"`
		Delete struct {
			IndexName string `json:"IndexName"`
		} `json:"Delete"`
		Update struct {
			IndexName             string `json:"IndexName"`
			ProvisionedThroughput *struct {
				ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
				WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
			} `json:"ProvisionedThroughput"`
		} `json:"Update"`
	} `json:"GlobalSecondaryIndexUpdates"`
}

func (s *Server) updateTable(r *http.Request, body []byte) (map[string]any, error) {
	if err := validateUpdateTablePayload(body); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	var req updateTableRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.TableName) == "" {
		return nil, awserr.Validation("TableName is required")
	}
	tableKey, err := identity.ScopedTableKeyFromIdentifier(req.TableName, accountIDFromContext(r.Context()))
	if err != nil {
		return nil, mapIdentityRequestError(err)
	}

	if len(req.ReplicaUpdates) > 0 {
		return nil, awserr.Validation("ReplicaUpdates is not supported")
	}

	var billing *tableapp.BillingUpdate
	if strings.TrimSpace(req.BillingMode) != "" || req.ProvisionedThroughput != nil {
		billingMode, readCapacityUnits, writeCapacityUnits, err := normalizeBillingConfig(req.BillingMode, req.ProvisionedThroughput)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		billing = &tableapp.BillingUpdate{BillingMode: billingMode, ReadCapacityUnits: readCapacityUnits, WriteCapacityUnits: writeCapacityUnits}
	}
	attrTypes := map[string]string{}
	for _, d := range req.AttributeDefinitions {
		if strings.TrimSpace(d.AttributeName) != "" {
			attrTypes[d.AttributeName] = d.AttributeType
		}
	}
	t, count, err := s.tableService.UpdateTable(r.Context(), tableapp.UpdateTableInput{
		TableKey:  tableKey,
		NowMillis: lifecycleNow(),
		Billing:   billing,
		ApplyOptions: func(t *model.Table) error {
			return applyUpdateTableOptions(t, req)
		},
		ApplyGSI: func(table model.Table, now int64) ([]model.GlobalSecondaryIndex, error) {
			if len(req.GlobalSecondaryIndexUpdates) == 0 {
				return table.GSIs, nil
			}
			return applyGSIUpdates(table, req.GlobalSecondaryIndexUpdates, attrTypes, now, table.BillingMode)
		},
		SetTableState: func(t *model.Table, now int64) {
			if len(req.GlobalSecondaryIndexUpdates) == 0 {
				return
			}
			t.Status = model.TableStatusUpdating
			t.StatusAt = now + lifecycleDelayMillis()
		},
	})
	if err != nil {
		return nil, mapUpdateTableError(err)
	}

	return map[string]any{"TableDescription": t.Description(count)}, nil
}

func (s *Server) refreshTableLifecycle(ctx context.Context, tx *sql.Tx, t *model.Table) error {
	repo := s.txReposFactory.Build(tx).Tables()
	if err := s.tableLifecycle.RefreshLifecycle(ctx, repo, t, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return sql.ErrNoRows
		}
		return err
	}
	return nil
}

func (s *Server) getTableWithLifecycle(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error) {
	scopedTableName, err := identity.ScopedTableKeyFromIdentifier(tableName, accountIDFromContext(ctx))
	if err != nil {
		return model.Table{}, mapIdentityRequestError(err)
	}
	return s.getTableWithLifecycleByKey(ctx, s.txReposFactory.Build(tx).Tables(), scopedTableName)
}

func (s *Server) getTableWithLifecycleByKey(ctx context.Context, tables uow.TableRepo, scopedTableName string) (model.Table, error) {
	t, err := s.tableLifecycle.GetWithLifecycle(ctx, tables, scopedTableName, lifecycleNow())
	if err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return model.Table{}, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return model.Table{}, err
	}
	return t, nil
}

func (s *Server) getTableWithLifecycleFromRepo(ctx context.Context, tables uow.TableRepo, tableName string) (model.Table, error) {
	scopedTableName, err := identity.ScopedTableKeyFromIdentifier(tableName, accountIDFromContext(ctx))
	if err != nil {
		return model.Table{}, mapIdentityRequestError(err)
	}
	return s.getTableWithLifecycleByKey(ctx, tables, scopedTableName)
}

func (s *Server) getActiveTable(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error) {
	t, err := s.getTableWithLifecycle(ctx, tx, tableName)
	if err != nil {
		return model.Table{}, err
	}
	return ensureTableActive(t)
}

func (s *Server) getActiveTableFromRepo(ctx context.Context, tables uow.TableRepo, tableName string) (model.Table, error) {
	t, err := s.getTableWithLifecycleFromRepo(ctx, tables, tableName)
	if err != nil {
		return model.Table{}, err
	}
	return ensureTableActive(t)
}

func ensureTableActive(t model.Table) (model.Table, error) {
	if t.Status != model.TableStatusActive {
		return model.Table{}, awserr.ResourceInUse("Table is currently " + t.Status)
	}
	return t, nil
}

func validateUpdateTablePayload(body []byte) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	if _, exists := raw["LocalSecondaryIndexUpdates"]; exists {
		return fmt.Errorf("LocalSecondaryIndexUpdates is not supported; local secondary indexes can only be specified at CreateTable")
	}
	if _, exists := raw["LocalSecondaryIndexes"]; exists {
		return fmt.Errorf("LocalSecondaryIndexes cannot be modified by UpdateTable")
	}
	return nil
}

type gsiUpdateRequest struct {
	Create struct {
		IndexName             string
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64
			WriteCapacityUnits int64
		}
		KeySchema []struct {
			AttributeName string
			KeyType       string
		}
		Projection struct {
			ProjectionType   string
			NonKeyAttributes []string
		}
	}
	Delete struct {
		IndexName string
	}
	Update struct {
		IndexName             string
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64
			WriteCapacityUnits int64
		}
	}
}

func applyGSIUpdates(table model.Table, updates []struct {
	Create struct {
		IndexName             string `json:"IndexName"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
		KeySchema []struct {
			AttributeName string `json:"AttributeName"`
			KeyType       string `json:"KeyType"`
		} `json:"KeySchema"`
		Projection struct {
			ProjectionType   string   `json:"ProjectionType"`
			NonKeyAttributes []string `json:"NonKeyAttributes"`
		} `json:"Projection"`
	} `json:"Create"`
	Delete struct {
		IndexName string `json:"IndexName"`
	} `json:"Delete"`
	Update struct {
		IndexName             string `json:"IndexName"`
		ProvisionedThroughput *struct {
			ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
			WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
		} `json:"ProvisionedThroughput"`
	} `json:"Update"`
}, attrTypes map[string]string, now int64, billingMode string) ([]model.GlobalSecondaryIndex, error) {
	gsis := append([]model.GlobalSecondaryIndex{}, table.GSIs...)
	touched := map[string]struct{}{}
	for _, u := range updates {
		hasCreate := strings.TrimSpace(u.Create.IndexName) != ""
		hasDelete := strings.TrimSpace(u.Delete.IndexName) != ""
		hasUpdate := strings.TrimSpace(u.Update.IndexName) != "" || u.Update.ProvisionedThroughput != nil
		actions := 0
		if hasCreate {
			actions++
		}
		if hasDelete {
			actions++
		}
		if hasUpdate {
			actions++
		}
		if actions != 1 {
			return nil, fmt.Errorf("each GlobalSecondaryIndexUpdates entry must include exactly one of Create, Update or Delete")
		}
		if hasDelete {
			name := strings.TrimSpace(u.Delete.IndexName)
			if _, exists := touched[name]; exists {
				return nil, fmt.Errorf("multiple updates for index %q are not allowed", name)
			}
			touched[name] = struct{}{}
			found := false
			next := make([]model.GlobalSecondaryIndex, 0, len(gsis))
			for _, g := range gsis {
				if g.IndexName == name {
					found = true
					if g.Status == model.IndexStatusDeleting {
						return nil, fmt.Errorf("index %q is already being deleted", name)
					}
					g.Status = model.IndexStatusDeleting
					g.StatusAt = now + lifecycleDelayMillis()
					next = append(next, g)
					continue
				}
				next = append(next, g)
			}
			if !found {
				return nil, fmt.Errorf("cannot delete unknown index %q", name)
			}
			gsis = next
			continue
		}
		if hasUpdate {
			name := strings.TrimSpace(u.Update.IndexName)
			if name == "" {
				return nil, fmt.Errorf("GSI IndexName is required")
			}
			if _, exists := touched[name]; exists {
				return nil, fmt.Errorf("multiple updates for index %q are not allowed", name)
			}
			touched[name] = struct{}{}
			if strings.TrimSpace(billingMode) != "PROVISIONED" {
				return nil, fmt.Errorf("GlobalSecondaryIndexUpdates Update is only supported for PROVISIONED billing mode")
			}
			if u.Update.ProvisionedThroughput == nil {
				return nil, fmt.Errorf("ProvisionedThroughput is required for GlobalSecondaryIndexUpdates Update")
			}
			if u.Update.ProvisionedThroughput.ReadCapacityUnits <= 0 || u.Update.ProvisionedThroughput.WriteCapacityUnits <= 0 {
				return nil, fmt.Errorf("ProvisionedThroughput ReadCapacityUnits and WriteCapacityUnits must be greater than 0")
			}
			found := false
			for i := range gsis {
				if gsis[i].IndexName != name {
					continue
				}
				if gsis[i].Status == model.IndexStatusCreating || gsis[i].Status == model.IndexStatusDeleting {
					return nil, fmt.Errorf("index %q is currently %s", name, gsis[i].Status)
				}
				gsis[i].ReadCapacity = u.Update.ProvisionedThroughput.ReadCapacityUnits
				gsis[i].WriteCapacity = u.Update.ProvisionedThroughput.WriteCapacityUnits
				found = true
				break
			}
			if !found {
				return nil, fmt.Errorf("cannot update unknown index %q", name)
			}
			continue
		}

		name := strings.TrimSpace(u.Create.IndexName)
		if name == "" {
			return nil, fmt.Errorf("GSI IndexName is required")
		}
		if _, exists := touched[name]; exists {
			return nil, fmt.Errorf("multiple updates for index %q are not allowed", name)
		}
		touched[name] = struct{}{}
		if _, ok := table.GetLSI(name); ok {
			return nil, fmt.Errorf("duplicate secondary index name %s", name)
		}
		for _, g := range gsis {
			if g.IndexName == name {
				return nil, fmt.Errorf("duplicate secondary index name %s", name)
			}
		}

		var hashKey, rangeKey string
		for _, key := range u.Create.KeySchema {
			switch key.KeyType {
			case "HASH":
				hashKey = key.AttributeName
			case "RANGE":
				rangeKey = key.AttributeName
			}
		}
		if strings.TrimSpace(hashKey) == "" {
			return nil, fmt.Errorf("GSI KeySchema must include HASH key")
		}
		hashType := attrTypes[hashKey]
		if hashType == "" {
			return nil, fmt.Errorf("AttributeDefinitions missing GSI HASH key type")
		}
		rangeType := ""
		if strings.TrimSpace(rangeKey) != "" {
			rangeType = attrTypes[rangeKey]
			if rangeType == "" {
				return nil, fmt.Errorf("AttributeDefinitions missing GSI RANGE key type")
			}
		}
		projectionType, nonKeyAttrs, err := normalizeIndexProjection(u.Create.Projection.ProjectionType, u.Create.Projection.NonKeyAttributes)
		if err != nil {
			return nil, err
		}
		gReadCapacity, gWriteCapacity, err := normalizeGSIThroughputForCreate(billingMode, u.Create.ProvisionedThroughput)
		if err != nil {
			return nil, err
		}
		gsis = append(gsis, model.GlobalSecondaryIndex{
			IndexName:      name,
			HashKey:        hashKey,
			HashType:       hashType,
			RangeKey:       rangeKey,
			RangeType:      rangeType,
			Status:         model.IndexStatusCreating,
			StatusAt:       0,
			ReadCapacity:   gReadCapacity,
			WriteCapacity:  gWriteCapacity,
			ProjectionType: projectionType,
			NonKeyAttrs:    nonKeyAttrs,
		})
	}
	sort.Slice(gsis, func(i, j int) bool { return gsis[i].IndexName < gsis[j].IndexName })
	return gsis, nil
}
