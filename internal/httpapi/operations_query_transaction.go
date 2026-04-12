package httpapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/expr"
	"github.com/jdillenkofer/pinax/internal/model"
)

type queryRequest struct {
	TableName              string `json:"TableName"`
	IndexName              string `json:"IndexName"`
	KeyConditionExpression string `json:"KeyConditionExpression"`
	KeyConditions          map[string]struct {
		AttributeValueList []any  `json:"AttributeValueList"`
		ComparisonOperator string `json:"ComparisonOperator"`
	} `json:"KeyConditions"`
	FilterExpression string `json:"FilterExpression"`
	QueryFilter      map[string]struct {
		AttributeValueList []any  `json:"AttributeValueList"`
		ComparisonOperator string `json:"ComparisonOperator"`
	} `json:"QueryFilter"`
	ConditionalOperator       string            `json:"ConditionalOperator"`
	AttributesToGet           []string          `json:"AttributesToGet"`
	Select                    string            `json:"Select"`
	ProjectionExpression      string            `json:"ProjectionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	ScanIndexForward          *bool             `json:"ScanIndexForward"`
	Limit                     int               `json:"Limit"`
	ExclusiveStartKey         map[string]any    `json:"ExclusiveStartKey"`
	ConsistentRead            bool              `json:"ConsistentRead"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) query(r *http.Request, body []byte) (map[string]any, error) {
	var req queryRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if len(req.QueryFilter) > 0 {
		return nil, awserr.Validation("QueryFilter is not supported")
	}
	if strings.TrimSpace(req.ConditionalOperator) != "" {
		return nil, awserr.Validation("ConditionalOperator is not supported")
	}

	var t model.Table
	var items []map[string]any
	targetHashKey := ""
	targetRangeKey := ""
	targetRangeType := ""
	var queryGSI *model.GlobalSecondaryIndex
	var queryLSI *model.LocalSecondaryIndex
	selectMode := ""
	projectionExpression := ""
	var expressionValues map[string]any
	var skToken *sortKeyCondition
	pk := ""
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}

		targetHashKey = t.HashKey
		targetRangeKey = t.RangeKey
		targetHashType := t.HashType
		targetRangeType = t.RangeType
		if strings.TrimSpace(req.IndexName) != "" {
			gsi, ok := t.GetGSI(req.IndexName)
			if ok {
				if gsi.Status != "" && gsi.Status != model.IndexStatusActive {
					return awserr.ResourceInUse("Index " + req.IndexName + " is not ACTIVE")
				}
				if req.ConsistentRead {
					return awserr.Validation("ConsistentRead is not supported on global secondary indexes")
				}
				queryGSI = &gsi
				targetHashKey = gsi.HashKey
				targetRangeKey = gsi.RangeKey
				targetHashType = gsi.HashType
				targetRangeType = gsi.RangeType
			} else {
				lsi, ok := t.GetLSI(req.IndexName)
				if !ok {
					return awserr.Validation("unknown index " + req.IndexName)
				}
				queryLSI = &lsi
				targetHashKey = t.HashKey
				targetRangeKey = lsi.RangeKey
				targetHashType = t.HashType
				targetRangeType = lsi.RangeType
			}
		}

		selectMode, projectionExpression, err = normalizeQuerySelect(req, strings.TrimSpace(req.IndexName) != "", queryGSI != nil)
		if err != nil {
			return awserr.Validation(err.Error())
		}

		if strings.TrimSpace(req.KeyConditionExpression) != "" && len(req.KeyConditions) > 0 {
			return awserr.Validation("KeyConditionExpression and KeyConditions cannot both be set")
		}

		expressionValues = cloneExpressionValues(req.ExpressionAttributeValues)
		var pkToken keyExprToken
		if strings.TrimSpace(req.KeyConditionExpression) != "" {
			pkToken, skToken, err = parseKeyCondition(req.KeyConditionExpression)
			if err != nil {
				return awserr.Validation(err.Error())
			}
		} else {
			pkToken, skToken, expressionValues, err = parseLegacyQueryKeyConditions(req.KeyConditions, targetHashKey, targetRangeKey, expressionValues)
			if err != nil {
				return awserr.Validation(err.Error())
			}
		}

		pkAttr, err := resolveNameStrict(pkToken.attr, req.ExpressionAttributeNames)
		if err != nil {
			return awserr.Validation(err.Error())
		}
		if pkAttr != targetHashKey {
			return awserr.Validation("partition key condition must target HASH key")
		}
		pkValue, ok := expressionValues[pkToken.value]
		if !ok {
			return awserr.Validation("missing partition key expression value")
		}
		if err := model.ValidateKeyAttributeType(pkValue, targetHashType, targetHashKey); err != nil {
			return awserr.Validation("One or more parameter values were invalid: Condition parameter type does not match schema type")
		}
		pk, err = model.SerializeKeyValue(pkValue)
		if err != nil {
			return awserr.Validation(err.Error())
		}

		scanForward := true
		if req.ScanIndexForward != nil {
			scanForward = *req.ScanIndexForward
		}

		if queryGSI != nil {
			items, err = repos.Items().QueryByGSI(txCtx, t.Name, req.IndexName, pk, "", true, 0)
			if err != nil {
				return err
			}
			items, err = orderItemsForGSI(items, t, *queryGSI, req.ExclusiveStartKey, scanForward)
		} else if queryLSI != nil {
			items, err = repos.Items().QueryByGSI(txCtx, t.Name, req.IndexName, pk, "", true, 0)
			if err != nil {
				return err
			}
			items, err = orderItemsForLSI(items, t, *queryLSI, req.ExclusiveStartKey, scanForward)
		} else if skToken == nil {
			if targetRangeKey == "" {
				items, err = repos.Items().QueryByPKSK(txCtx, t.Name, pk, model.NoSortKey)
			} else {
				items, err = repos.Items().QueryByPK(txCtx, t.Name, pk, "", true, 0)
				if err == nil {
					items, err = orderItemsForTable(items, t, req.ExclusiveStartKey, scanForward)
				}
			}
		} else {
			skAttr, err := resolveNameStrict(skToken.attr, req.ExpressionAttributeNames)
			if err != nil {
				return awserr.Validation(err.Error())
			}
			if skAttr != targetRangeKey {
				return awserr.Validation("sort key condition must target RANGE key")
			}
			items, err = repos.Items().QueryByPK(txCtx, t.Name, pk, "", true, 0)
			if err == nil {
				items, err = orderItemsForTable(items, t, req.ExclusiveStartKey, scanForward)
			}
		}
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	limit := parseLimit(req.Limit)
	count := 0
	scanned := 0
	filtered := make([]map[string]any, 0)
	var lastScanned map[string]any
	totalRead := 0.0

	for _, item := range items {
		queryItem := item
		if queryGSI != nil {
			queryItem = projectItemForGSI(item, t, *queryGSI)
		} else if queryLSI != nil {
			queryItem = projectItemForLSI(item, t, *queryLSI)
		}

		if strings.TrimSpace(req.IndexName) != "" {
			raw, ok := queryItem[targetHashKey]
			if !ok {
				continue
			}
			itemPK, err := model.SerializeKeyValue(raw)
			if err != nil || itemPK != pk {
				continue
			}
		}

		if skToken != nil {
			ok, err := sortConditionMatches(queryItem, skToken, req.ExpressionAttributeNames, expressionValues, targetRangeType)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				continue
			}
		}

		scanned++
		lastScanned = keyFromItem(t, item)
		totalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(queryItem), req.ConsistentRead)

		matches, err := applyFilter(queryItem, req.FilterExpression, req.ExpressionAttributeNames, expressionValues)
		if err != nil {
			return nil, awserr.Validation(filterExpressionValidationMessage(err))
		}
		if matches {
			if selectMode != "COUNT" {
				emit := queryItem
				switch selectMode {
				case "ALL_ATTRIBUTES":
					emit = item
				case "SPECIFIC_ATTRIBUTES":
					projected, err := applyProjection(queryItem, projectionExpression, req.ExpressionAttributeNames)
					if err != nil {
						return nil, awserr.Validation(err.Error())
					}
					emit = projected
				}
				filtered = append(filtered, cloneItem(emit))
			}
			count++
		}

		if limit > 0 && scanned >= limit {
			break
		}
	}

	resp := map[string]any{"Count": count, "ScannedCount": scanned}
	if selectMode != "COUNT" {
		resp["Items"] = filtered
	}
	if err := s.ensureReadCapacity(t, totalRead); err != nil {
		return nil, err
	}
	indexType := ""
	if queryGSI != nil {
		indexType = "GSI"
	}
	if queryLSI != nil {
		indexType = "LSI"
	}
	setSingleQueryConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, req.IndexName, indexType, totalRead)
	if limit > 0 && scanned == limit && lastScanned != nil {
		resp["LastEvaluatedKey"] = lastScanned
	}
	return resp, nil
}

type scanRequest struct {
	TableName                 string            `json:"TableName"`
	FilterExpression          string            `json:"FilterExpression"`
	ProjectionExpression      string            `json:"ProjectionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	Limit                     int               `json:"Limit"`
	ExclusiveStartKey         map[string]any    `json:"ExclusiveStartKey"`
	Segment                   *int              `json:"Segment"`
	TotalSegments             *int              `json:"TotalSegments"`
	ConsistentRead            bool              `json:"ConsistentRead"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) scan(r *http.Request, body []byte) (map[string]any, error) {
	var req scanRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if (req.Segment == nil) != (req.TotalSegments == nil) {
		return nil, awserr.Validation("Segment and TotalSegments must be provided together")
	}
	segmentEnabled := req.Segment != nil && req.TotalSegments != nil
	segment := 0
	totalSegments := 0
	if segmentEnabled {
		segment = *req.Segment
		totalSegments = *req.TotalSegments
		if totalSegments <= 0 {
			return nil, awserr.Validation("TotalSegments must be greater than zero")
		}
		if totalSegments > 1000000 {
			return nil, awserr.Validation("TotalSegments must be less than or equal to 1000000")
		}
		if segment < 0 || segment >= totalSegments {
			return nil, awserr.Validation("Segment must be greater than or equal to 0 and less than TotalSegments")
		}
	}

	var t model.Table
	var items []map[string]any
	startPK := ""
	startSK := ""
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getActiveTableFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		if len(req.ExclusiveStartKey) > 0 {
			startPK, startSK, err = model.ExtractKey(t, req.ExclusiveStartKey)
			if err != nil {
				return awserr.Validation(err.Error())
			}
		}
		items, err = repos.Items().Scan(txCtx, t.Name, startPK, startSK, 0)
		return err
	}); err != nil {
		return nil, err
	}

	limit := parseLimit(req.Limit)
	filtered := make([]map[string]any, 0)
	scanned := 0
	var lastScanned map[string]any
	totalRead := 0.0
	for _, item := range items {
		if segmentEnabled {
			rawPK, ok := item[t.HashKey]
			if !ok {
				continue
			}
			serializedPK, err := model.SerializeKeyValue(rawPK)
			if err != nil {
				continue
			}
			if scanSegmentForPK(serializedPK, totalSegments) != segment {
				continue
			}
		}
		scanned++
		lastScanned = keyFromItem(t, item)
		totalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead)

		matches, err := applyFilter(item, req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if err != nil {
			return nil, awserr.Validation(filterExpressionValidationMessage(err))
		}
		if !matches {
			if limit > 0 && scanned >= limit {
				break
			}
			continue
		}
		projected, err := applyProjection(item, req.ProjectionExpression, req.ExpressionAttributeNames)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		filtered = append(filtered, projected)
		if limit > 0 && scanned >= limit {
			break
		}

	}

	resp := map[string]any{"Items": filtered, "Count": len(filtered), "ScannedCount": scanned}
	if err := s.ensureReadCapacity(t, totalRead); err != nil {
		return nil, err
	}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, totalRead, 0)
	if limit > 0 && scanned == limit && lastScanned != nil {
		resp["LastEvaluatedKey"] = lastScanned
	}
	return resp, nil
}

type batchGetItemRequest struct {
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
	RequestItems           map[string]struct {
		Keys                     []map[string]any  `json:"Keys"`
		ConsistentRead           bool              `json:"ConsistentRead"`
		AttributesToGet          []string          `json:"AttributesToGet"`
		ProjectionExpression     string            `json:"ProjectionExpression"`
		ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
		ReturnConsumedCapacity   string            `json:"ReturnConsumedCapacity"`
	} `json:"RequestItems"`
}

func (s *Server) batchGetItem(r *http.Request, body []byte) (map[string]any, error) {
	var req batchGetItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	totalKeys := 0
	for _, itemReq := range req.RequestItems {
		totalKeys += len(itemReq.Keys)
	}
	if len(req.RequestItems) == 0 {
		return nil, awserr.Validation("RequestItems is required")
	}
	if totalKeys > 100 {
		return nil, awserr.Validation("BatchGetItem supports at most 100 keys")
	}

	batchGetProcessLimit := processingLimitFromEnv("PINAX_BATCH_GET_PROCESS_LIMIT")
	processed := 0
	responses := map[string]any{}
	unprocessed := map[string]any{}
	readByTable := map[string]float64{}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for tableName, itemReq := range req.RequestItems {
			projectionExpression, err := normalizeLegacyAttributesProjection(itemReq.AttributesToGet, itemReq.ProjectionExpression)
			if err != nil {
				return awserr.Validation(err.Error())
			}
			if _, err := applyProjection(map[string]any{}, projectionExpression, itemReq.ExpressionAttributeNames); err != nil {
				return awserr.Validation(err.Error())
			}
			if len(itemReq.Keys) == 0 {
				return awserr.Validation("BatchGetItem request for table " + tableName + " must include at least one key")
			}
			t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			items := make([]map[string]any, 0, len(itemReq.Keys))
			unprocessedKeys := make([]map[string]any, 0)
			seenKeys := map[string]struct{}{}
			for _, key := range itemReq.Keys {
				pk, sk, err := model.ExtractKey(t, key)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				target := tableName + "|" + pk + "|" + sk
				if _, exists := seenKeys[target]; exists {
					return awserr.Validation("Provided list of item keys contains duplicates")
				}
				seenKeys[target] = struct{}{}
				if batchGetProcessLimit > 0 && processed >= batchGetProcessLimit {
					unprocessedKeys = append(unprocessedKeys, key)
					continue
				}
				processed++

				item, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						units := model.CalculateReadCapacityUnits(1, itemReq.ConsistentRead)
						if !s.reserveReadCapacity(t, units) {
							unprocessedKeys = append(unprocessedKeys, key)
							continue
						}
						readByTable[tableName] += units
						continue
					}
					return err
				}
				units := model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), itemReq.ConsistentRead)
				if !s.reserveReadCapacity(t, units) {
					unprocessedKeys = append(unprocessedKeys, key)
					continue
				}
				projected, err := applyProjection(item, projectionExpression, itemReq.ExpressionAttributeNames)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				items = append(items, projected)
				readByTable[tableName] += units
			}
			responses[tableName] = items
			if len(unprocessedKeys) > 0 {
				entry := map[string]any{
					"Keys":                     unprocessedKeys,
					"ConsistentRead":           itemReq.ConsistentRead,
					"ProjectionExpression":     itemReq.ProjectionExpression,
					"ExpressionAttributeNames": itemReq.ExpressionAttributeNames,
				}
				if len(itemReq.AttributesToGet) > 0 {
					entry["AttributesToGet"] = itemReq.AttributesToGet
				}
				unprocessed[tableName] = entry
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"Responses": responses, "UnprocessedKeys": unprocessed}
	for tableName, units := range readByTable {
		mode := req.ReturnConsumedCapacity
		if mode == "" {
			mode = req.RequestItems[tableName].ReturnConsumedCapacity
		}
		addConsumedCapacity(resp, mode, tableName, units, 0)
	}
	return resp, nil
}

type batchWriteItemRequest struct {
	RequestItems map[string][]struct {
		PutRequest struct {
			Item map[string]any `json:"Item"`
		} `json:"PutRequest"`
		DeleteRequest struct {
			Key map[string]any `json:"Key"`
		} `json:"DeleteRequest"`
	} `json:"RequestItems"`
	ReturnConsumedCapacity      string `json:"ReturnConsumedCapacity"`
	ReturnItemCollectionMetrics string `json:"ReturnItemCollectionMetrics"`
}

func (s *Server) batchWriteItem(r *http.Request, body []byte) (map[string]any, error) {
	var req batchWriteItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	itemCollectionMode := strings.ToUpper(strings.TrimSpace(req.ReturnItemCollectionMetrics))
	if itemCollectionMode == "" {
		itemCollectionMode = "NONE"
	}
	if itemCollectionMode != "NONE" && itemCollectionMode != "SIZE" {
		return nil, awserr.Validation("BatchWriteItem ReturnItemCollectionMetrics must be NONE or SIZE")
	}

	totalOps := 0
	for _, ops := range req.RequestItems {
		totalOps += len(ops)
	}
	if len(req.RequestItems) == 0 {
		return nil, awserr.Validation("RequestItems is required")
	}
	if totalOps > 25 {
		return nil, awserr.Validation("BatchWriteItem supports at most 25 operations")
	}

	tableNames := make([]string, 0, len(req.RequestItems))
	for tableName := range req.RequestItems {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	batchWriteProcessLimit := processingLimitFromEnv("PINAX_BATCH_WRITE_PROCESS_LIMIT")
	processed := 0
	unprocessed := map[string]any{}
	writeByTable := map[string]float64{}
	itemCollectionMetrics := map[string][]map[string]any{}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for _, tableName := range tableNames {
			ops := req.RequestItems[tableName]
			if len(ops) == 0 {
				return awserr.Validation("BatchWriteItem request for table " + tableName + " must include at least one write request")
			}
			t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}

			seenKeys := map[string]struct{}{}
			seenItemCollections := map[string]struct{}{}
			for _, op := range ops {
				if len(op.PutRequest.Item) == 0 && len(op.DeleteRequest.Key) == 0 {
					return awserr.Validation("write request must contain either PutRequest or DeleteRequest")
				}
				if len(op.PutRequest.Item) > 0 && len(op.DeleteRequest.Key) > 0 {
					return awserr.Validation("write request cannot contain both PutRequest and DeleteRequest")
				}
				if batchWriteProcessLimit > 0 && processed >= batchWriteProcessLimit {
					if unprocessed[tableName] == nil {
						unprocessed[tableName] = make([]any, 0)
					}
					unprocessed[tableName] = append(unprocessed[tableName].([]any), op)
					continue
				}

				if len(op.PutRequest.Item) > 0 {
					if model.ItemTooLarge(op.PutRequest.Item) {
						return awserr.Validation("Item size has exceeded the maximum allowed size")
					}
					pk, sk, err := model.ExtractItemKeys(t, op.PutRequest.Item)
					if err != nil {
						return awserr.Validation(err.Error())
					}
					if err := model.ValidateSecondaryIndexKeyTypes(t, op.PutRequest.Item); err != nil {
						return awserr.Validation(err.Error())
					}
					k := tableName + "|" + pk + "|" + sk
					if _, exists := seenKeys[k]; exists {
						return awserr.Validation("Provided list of item keys contains duplicates")
					}
					seenKeys[k] = struct{}{}
					current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
					existed := true
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
					if errors.Is(err, sql.ErrNoRows) {
						existed = false
						current = map[string]any{}
					}
					writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(op.PutRequest.Item))
					if !s.reserveWriteCapacity(t, writeUnits) {
						if unprocessed[tableName] == nil {
							unprocessed[tableName] = make([]any, 0)
						}
						unprocessed[tableName] = append(unprocessed[tableName].([]any), op)
						continue
					}
					if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, op.PutRequest.Item); err != nil {
						return err
					}
					eventName := "INSERT"
					streamOld := current
					if existed {
						eventName = "MODIFY"
					} else {
						streamOld = nil
					}
					if err := s.emitMutationEventForWrite(txCtx, repos, t, eventName, keyAttributesFromItem(t, op.PutRequest.Item), streamOld, op.PutRequest.Item, time.Now().UnixMilli()); err != nil {
						return err
					}
					writeByTable[tableName] += writeUnits
					if itemCollectionMode == "SIZE" && len(t.LSIs) > 0 {
						hashValue := op.PutRequest.Item[t.HashKey]
						if keyToken, err := model.SerializeKeyValue(hashValue); err == nil {
							if _, exists := seenItemCollections[keyToken]; !exists {
								seenItemCollections[keyToken] = struct{}{}
								itemCollectionMetrics[tableName] = append(itemCollectionMetrics[tableName], map[string]any{
									"ItemCollectionKey":   map[string]any{t.HashKey: hashValue},
									"SizeEstimateRangeGB": []float64{0, 1},
								})
							}
						}
					}
					processed++
				}
				if len(op.DeleteRequest.Key) > 0 {
					pk, sk, err := model.ExtractKey(t, op.DeleteRequest.Key)
					if err != nil {
						return awserr.Validation(err.Error())
					}
					k := tableName + "|" + pk + "|" + sk
					if _, exists := seenKeys[k]; exists {
						return awserr.Validation("Provided list of item keys contains duplicates")
					}
					seenKeys[k] = struct{}{}
					current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
					existed := true
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return err
					}
					if errors.Is(err, sql.ErrNoRows) {
						existed = false
						current = op.DeleteRequest.Key
					}
					writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
					if !s.reserveWriteCapacity(t, writeUnits) {
						if unprocessed[tableName] == nil {
							unprocessed[tableName] = make([]any, 0)
						}
						unprocessed[tableName] = append(unprocessed[tableName].([]any), op)
						continue
					}
					if err := repos.Items().DeleteItem(txCtx, t.Name, pk, sk); err != nil {
						return err
					}
					if existed {
						if len(current) > 0 {
							if err := s.emitMutationEventForWrite(txCtx, repos, t, "REMOVE", keyAttributesFromKey(t, op.DeleteRequest.Key), current, nil, time.Now().UnixMilli()); err != nil {
								return err
							}
						}
					}
					writeByTable[tableName] += writeUnits
					if itemCollectionMode == "SIZE" && len(t.LSIs) > 0 {
						hashValue := op.DeleteRequest.Key[t.HashKey]
						if keyToken, err := model.SerializeKeyValue(hashValue); err == nil {
							if _, exists := seenItemCollections[keyToken]; !exists {
								seenItemCollections[keyToken] = struct{}{}
								itemCollectionMetrics[tableName] = append(itemCollectionMetrics[tableName], map[string]any{
									"ItemCollectionKey":   map[string]any{t.HashKey: hashValue},
									"SizeEstimateRangeGB": []float64{0, 1},
								})
							}
						}
					}
					processed++
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"UnprocessedItems": unprocessed}
	for tableName, units := range writeByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	if itemCollectionMode == "SIZE" {
		resp["ItemCollectionMetrics"] = itemCollectionMetrics
	}
	return resp, nil
}

type transactGetRequest struct {
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
	TransactItems          []struct {
		Get struct {
			TableName                string            `json:"TableName"`
			Key                      map[string]any    `json:"Key"`
			ProjectionExpression     string            `json:"ProjectionExpression"`
			ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
		} `json:"Get"`
	} `json:"TransactItems"`
}

func (s *Server) transactGetItems(r *http.Request, body []byte) (map[string]any, error) {
	var req transactGetRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if len(req.TransactItems) == 0 {
		return nil, awserr.Validation("TransactItems is required")
	}
	if len(req.TransactItems) > 25 {
		return nil, awserr.Validation("TransactGetItems supports at most 25 items")
	}

	responses := make([]map[string]any, 0, len(req.TransactItems))
	readByTable := map[string]float64{}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for _, txItem := range req.TransactItems {
			g := txItem.Get
			t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), g.TableName)
			if err != nil {
				return err
			}
			pk, sk, err := model.ExtractKey(t, g.Key)
			if err != nil {
				return awserr.Validation(err.Error())
			}
			item, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					if _, err := applyProjection(map[string]any{}, g.ProjectionExpression, g.ExpressionAttributeNames); err != nil {
						return awserr.Validation(err.Error())
					}
					readByTable[g.TableName] += model.CalculateReadCapacityUnits(1, true)
					responses = append(responses, map[string]any{})
					continue
				}
				return err
			}
			projected, err := applyProjection(item, g.ProjectionExpression, g.ExpressionAttributeNames)
			if err != nil {
				return awserr.Validation(err.Error())
			}
			readByTable[g.TableName] += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), true)
			responses = append(responses, map[string]any{"Item": projected})
		}
		for tableName, units := range readByTable {
			t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			if err := s.ensureReadCapacity(t, units); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"Responses": responses}
	for tableName, units := range readByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, units, 0)
	}
	return resp, nil
}

type transactWriteRequest struct {
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
	ClientRequestToken     string `json:"ClientRequestToken"`
	TransactItems          []struct {
		Put struct {
			TableName                           string            `json:"TableName"`
			Item                                map[string]any    `json:"Item"`
			ConditionExpression                 string            `json:"ConditionExpression"`
			ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
			ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Put"`
		Delete struct {
			TableName                           string            `json:"TableName"`
			Key                                 map[string]any    `json:"Key"`
			ConditionExpression                 string            `json:"ConditionExpression"`
			ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
			ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Delete"`
		Update struct {
			TableName                           string            `json:"TableName"`
			Key                                 map[string]any    `json:"Key"`
			UpdateExpression                    string            `json:"UpdateExpression"`
			ConditionExpression                 string            `json:"ConditionExpression"`
			ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
			ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Update"`
		ConditionCheck struct {
			TableName                           string            `json:"TableName"`
			Key                                 map[string]any    `json:"Key"`
			ConditionExpression                 string            `json:"ConditionExpression"`
			ReturnValuesOnConditionCheckFailure string            `json:"ReturnValuesOnConditionCheckFailure"`
			ExpressionAttributeNames            map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues           map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"ConditionCheck"`
	} `json:"TransactItems"`
}

func (s *Server) transactWriteItems(r *http.Request, body []byte) (map[string]any, error) {
	var req transactWriteRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ClientRequestToken) != "" && len(req.ClientRequestToken) > 36 {
		return nil, awserr.Validation("ClientRequestToken must be between 1 and 36 characters")
	}
	if len(req.TransactItems) == 0 {
		return nil, awserr.Validation("TransactItems is required")
	}
	if len(req.TransactItems) > 25 {
		return nil, awserr.Validation("TransactWriteItems supports at most 25 actions")
	}
	var response map[string]any
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		nowMillis := time.Now().UnixMilli()
		if strings.TrimSpace(req.ClientRequestToken) != "" {
			if err := repos.Items().DeleteExpiredTransactWriteIdempotency(txCtx, nowMillis); err != nil {
				return err
			}
			rec, err := repos.Items().GetTransactWriteIdempotency(txCtx, req.ClientRequestToken, nowMillis)
			if err == nil {
				hash, err := transactWriteRequestHash(req)
				if err != nil {
					return err
				}
				if rec.RequestHash != hash {
					return awserr.IdempotentParameterMismatch("The provided client token is already in use with different parameters")
				}
				response = rec.Response
				return nil
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}

		seenTargets := map[string]struct{}{}
		writeByTable := map[string]float64{}

		for i, txItem := range req.TransactItems {
			actions := 0
			if len(txItem.Put.Item) > 0 {
				actions++
			}
			if len(txItem.Delete.Key) > 0 {
				actions++
			}
			if len(txItem.Update.Key) > 0 {
				actions++
			}
			if len(txItem.ConditionCheck.Key) > 0 {
				actions++
			}
			if actions != 1 {
				return awserr.Validation("TransactItems can only contain one of Check, Put, Update or Delete")
			}

			if len(txItem.Put.Item) > 0 {
				if err := validateReturnValuesOnConditionCheckFailure(txItem.Put.ReturnValuesOnConditionCheckFailure); err != nil {
					return awserr.Validation(err.Error())
				}
				t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), txItem.Put.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractItemKeys(t, txItem.Put.Item)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return awserr.Validation("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				itemExisted := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					itemExisted = false
					current = map[string]any{}
				}
				ok, err := expr.Evaluate(txItem.Put.ConditionExpression, current, txItem.Put.ExpressionAttributeNames, txItem.Put.ExpressionAttributeValues)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, conditionExpressionValidationMessage(err))
				}
				if !ok {
					reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
						Code:    "ConditionalCheckFailed",
						Message: "The conditional request failed",
						Item:    itemForConditionFailure(txItem.Put.ReturnValuesOnConditionCheckFailure, current, itemExisted),
					})
					return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
				}
				if model.ItemTooLarge(txItem.Put.Item) {
					return awserr.Validation("Item size has exceeded the maximum allowed size")
				}
				if err := model.ValidateSecondaryIndexKeyTypes(t, txItem.Put.Item); err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, err.Error())
				}
				if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, txItem.Put.Item); err != nil {
					return err
				}
				eventName := "INSERT"
				streamOld := current
				if itemExisted {
					eventName = "MODIFY"
				} else {
					streamOld = nil
				}
				if err := s.emitMutationEventForWrite(txCtx, repos, t, eventName, keyAttributesFromItem(t, txItem.Put.Item), streamOld, txItem.Put.Item, time.Now().UnixMilli()); err != nil {
					return err
				}
				writeByTable[txItem.Put.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(txItem.Put.Item))
				continue
			}

			if len(txItem.Delete.Key) > 0 {
				if err := validateReturnValuesOnConditionCheckFailure(txItem.Delete.ReturnValuesOnConditionCheckFailure); err != nil {
					return awserr.Validation(err.Error())
				}
				t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), txItem.Delete.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, txItem.Delete.Key)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return awserr.Validation("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				itemExisted := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					itemExisted = false
					current = map[string]any{}
				}
				ok, err := expr.Evaluate(txItem.Delete.ConditionExpression, current, txItem.Delete.ExpressionAttributeNames, txItem.Delete.ExpressionAttributeValues)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, conditionExpressionValidationMessage(err))
				}
				if !ok {
					reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
						Code:    "ConditionalCheckFailed",
						Message: "The conditional request failed",
						Item:    itemForConditionFailure(txItem.Delete.ReturnValuesOnConditionCheckFailure, current, itemExisted),
					})
					return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
				}
				if err := repos.Items().DeleteItem(txCtx, t.Name, pk, sk); err != nil {
					return err
				}
				if itemExisted {
					if err := s.emitMutationEventForWrite(txCtx, repos, t, "REMOVE", keyAttributesFromKey(t, txItem.Delete.Key), current, nil, time.Now().UnixMilli()); err != nil {
						return err
					}
				}
				writeByTable[txItem.Delete.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
				continue
			}

			if len(txItem.Update.Key) > 0 {
				if err := validateReturnValuesOnConditionCheckFailure(txItem.Update.ReturnValuesOnConditionCheckFailure); err != nil {
					return awserr.Validation(err.Error())
				}
				t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), txItem.Update.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, txItem.Update.Key)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return awserr.Validation("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				existing, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				itemExisted := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					itemExisted = false
					existing = map[string]any{}
				}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						current = map[string]any{t.HashKey: txItem.Update.Key[t.HashKey]}
						if t.RangeKey != "" {
							current[t.RangeKey] = txItem.Update.Key[t.RangeKey]
						}
					} else {
						return err
					}
				}
				ok, err := expr.Evaluate(txItem.Update.ConditionExpression, current, txItem.Update.ExpressionAttributeNames, txItem.Update.ExpressionAttributeValues)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, conditionExpressionValidationMessage(err))
				}
				if !ok {
					reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
						Code:    "ConditionalCheckFailed",
						Message: "The conditional request failed",
						Item:    itemForConditionFailure(txItem.Update.ReturnValuesOnConditionCheckFailure, existing, itemExisted),
					})
					return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
				}
				plan, err := parseUpdateExpression(txItem.Update.UpdateExpression, txItem.Update.ExpressionAttributeNames, txItem.Update.ExpressionAttributeValues)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, err.Error())
				}
				updated, _, err := applyUpdatePlan(current, plan)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, err.Error())
				}
				if err := model.ValidateSecondaryIndexKeyTypes(t, updated); err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, err.Error())
				}
				if model.ItemTooLarge(updated) {
					return awserr.Validation("Item size has exceeded the maximum allowed size")
				}
				if err := repos.Items().PutItem(txCtx, t.Name, pk, sk, updated); err != nil {
					return err
				}
				eventName := "INSERT"
				streamOld := existing
				if itemExisted {
					eventName = "MODIFY"
				} else {
					streamOld = nil
				}
				if err := s.emitMutationEventForWrite(txCtx, repos, t, eventName, keyAttributesFromKey(t, txItem.Update.Key), streamOld, updated, time.Now().UnixMilli()); err != nil {
					return err
				}
				writeByTable[txItem.Update.TableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated))
				continue
			}

			if len(txItem.ConditionCheck.Key) > 0 {
				if err := validateReturnValuesOnConditionCheckFailure(txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure); err != nil {
					return awserr.Validation(err.Error())
				}
				t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), txItem.ConditionCheck.TableName)
				if err != nil {
					return err
				}
				pk, sk, err := model.ExtractKey(t, txItem.ConditionCheck.Key)
				if err != nil {
					return awserr.Validation(err.Error())
				}
				target := t.Name + "|" + pk + "|" + sk
				if _, exists := seenTargets[target]; exists {
					return awserr.Validation("Transaction request cannot include multiple operations on one item")
				}
				seenTargets[target] = struct{}{}

				current, err := repos.Items().GetItem(txCtx, t.Name, pk, sk)
				itemExisted := true
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if errors.Is(err, sql.ErrNoRows) {
					itemExisted = false
					current = map[string]any{}
				}
				ok, err := expr.Evaluate(txItem.ConditionCheck.ConditionExpression, current, txItem.ConditionCheck.ExpressionAttributeNames, txItem.ConditionCheck.ExpressionAttributeValues)
				if err != nil {
					return transactionValidationCanceled(len(req.TransactItems), i, conditionExpressionValidationMessage(err))
				}
				if !ok {
					reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
						Code:    "ConditionalCheckFailed",
						Message: "The conditional request failed",
						Item:    itemForConditionFailure(txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure, current, itemExisted),
					})
					return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
				}
				continue
			}

			return awserr.Validation("each transact item must contain one operation")
		}
		for tableName, units := range writeByTable {
			t, err := s.getActiveTableFromRepo(txCtx, repos.Tables(), tableName)
			if err != nil {
				return err
			}
			if err := s.ensureWriteCapacity(t, units); err != nil {
				return err
			}
		}

		response = map[string]any{}
		for tableName, units := range writeByTable {
			addConsumedCapacity(response, req.ReturnConsumedCapacity, tableName, 0, units)
		}
		if strings.TrimSpace(req.ClientRequestToken) != "" {
			hash, err := transactWriteRequestHash(req)
			if err != nil {
				return err
			}
			record := model.TransactWriteIdempotencyRecord{
				Token:       req.ClientRequestToken,
				RequestHash: hash,
				Response:    response,
				CreatedAt:   nowMillis,
				ExpiresAt:   nowMillis + int64((10*time.Minute)/time.Millisecond),
			}
			if err := repos.Items().PutTransactWriteIdempotency(txCtx, record); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if response == nil {
		response = map[string]any{}
	}
	return response, nil
}

func transactWriteRequestHash(req transactWriteRequest) (string, error) {
	req.ClientRequestToken = ""
	raw, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func buildTransactionCancellationReasons(count int, failedIndex int, failed awserr.CancellationReason) []awserr.CancellationReason {
	reasons := make([]awserr.CancellationReason, count)
	for i := 0; i < count; i++ {
		reasons[i] = awserr.CancellationReason{Code: "None"}
	}
	reasons[failedIndex] = failed
	return reasons
}

func transactionValidationCanceled(count int, failedIndex int, msg string) error {
	reasons := buildTransactionCancellationReasons(count, failedIndex, awserr.CancellationReason{
		Code:    "ValidationError",
		Message: msg,
	})
	return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]", reasons)
}

func validateReturnValuesOnConditionCheckFailure(v string) error {
	v = strings.TrimSpace(v)
	if v == "" || v == "NONE" || v == "ALL_OLD" {
		return nil
	}
	return fmt.Errorf("unsupported ReturnValuesOnConditionCheckFailure %q", v)
}

func itemForConditionFailure(returnValues string, item map[string]any, itemExisted bool) map[string]any {
	if strings.TrimSpace(returnValues) != "ALL_OLD" || !itemExisted {
		return nil
	}
	return item
}

func processingLimitFromEnv(name string) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0
	}
	return v
}
