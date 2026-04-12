package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	itemopsapp "github.com/jdillenkofer/pinax/internal/app/itemops"
	queryapp "github.com/jdillenkofer/pinax/internal/app/query"
	transactionapp "github.com/jdillenkofer/pinax/internal/app/transaction"
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

type queryHTTPAdapter struct {
	skToken              *sortKeyCondition
	expressionNames      map[string]string
	expressionValues     map[string]any
	targetRangeType      string
	filterExpression     string
	projectionExpression string
}

func (a queryHTTPAdapter) OrderItemsForGSI(items []map[string]any, t model.Table, gsi model.GlobalSecondaryIndex, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	return orderItemsForGSI(items, t, gsi, exclusiveStartKey, scanForward)
}

func (a queryHTTPAdapter) OrderItemsForLSI(items []map[string]any, t model.Table, lsi model.LocalSecondaryIndex, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	return orderItemsForLSI(items, t, lsi, exclusiveStartKey, scanForward)
}

func (a queryHTTPAdapter) OrderItemsForTable(items []map[string]any, t model.Table, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	return orderItemsForTable(items, t, exclusiveStartKey, scanForward)
}

func (a queryHTTPAdapter) ProjectItemForGSI(item map[string]any, t model.Table, gsi model.GlobalSecondaryIndex) map[string]any {
	return projectItemForGSI(item, t, gsi)
}

func (a queryHTTPAdapter) ProjectItemForLSI(item map[string]any, t model.Table, lsi model.LocalSecondaryIndex) map[string]any {
	return projectItemForLSI(item, t, lsi)
}

func (a queryHTTPAdapter) SortConditionMatches(item map[string]any) (bool, error) {
	if a.skToken == nil {
		return true, nil
	}
	return sortConditionMatches(item, a.skToken, a.expressionNames, a.expressionValues, a.targetRangeType)
}

func (a queryHTTPAdapter) ApplyFilter(item map[string]any) (bool, error) {
	return applyFilter(item, a.filterExpression, a.expressionNames, a.expressionValues)
}

func (a queryHTTPAdapter) ApplyProjection(item map[string]any) (map[string]any, error) {
	return applyProjection(item, a.projectionExpression, a.expressionNames)
}

func (a queryHTTPAdapter) CloneItem(item map[string]any) map[string]any {
	return cloneItem(item)
}

func (a queryHTTPAdapter) KeyFromItem(t model.Table, item map[string]any) map[string]any {
	return keyFromItem(t, item)
}

func (a queryHTTPAdapter) SegmentForPK(serializedPK string, totalSegments int) int {
	return scanSegmentForPK(serializedPK, totalSegments)
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

	target, err := s.queryService.ResolveQueryTarget(r.Context(), queryapp.ResolveQueryTargetInput{
		TableName:      req.TableName,
		IndexName:      strings.TrimSpace(req.IndexName),
		ConsistentRead: req.ConsistentRead,
	})
	if err != nil {
		mapped := mapAppError(err)
		if mapped != err {
			return nil, mapped
		}
		return nil, err
	}

	t := target.Table
	targetHashKey := target.TargetHashKey
	targetRangeKey := target.TargetRangeKey
	targetRangeType := target.TargetRangeType
	queryGSI := target.QueryGSI
	queryLSI := target.QueryLSI
	selectMode := ""
	projectionExpression := ""
	var expressionValues map[string]any
	var skToken *sortKeyCondition
	pk := ""
	targetHashType := target.TargetHashType

	selectMode, projectionExpression, err = normalizeQuerySelect(req, strings.TrimSpace(req.IndexName) != "", queryGSI != nil)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	if strings.TrimSpace(req.KeyConditionExpression) != "" && len(req.KeyConditions) > 0 {
		return nil, awserr.Validation("KeyConditionExpression and KeyConditions cannot both be set")
	}

	expressionValues = cloneExpressionValues(req.ExpressionAttributeValues)
	var pkToken keyExprToken
	if strings.TrimSpace(req.KeyConditionExpression) != "" {
		pkToken, skToken, err = parseKeyCondition(req.KeyConditionExpression)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
	} else {
		pkToken, skToken, expressionValues, err = parseLegacyQueryKeyConditions(req.KeyConditions, targetHashKey, targetRangeKey, expressionValues)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
	}

	pkAttr, err := resolveNameStrict(pkToken.attr, req.ExpressionAttributeNames)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if pkAttr != targetHashKey {
		return nil, awserr.Validation("partition key condition must target HASH key")
	}
	pkValue, ok := expressionValues[pkToken.value]
	if !ok {
		return nil, awserr.Validation("missing partition key expression value")
	}
	if err := model.ValidateKeyAttributeType(pkValue, targetHashType, targetHashKey); err != nil {
		return nil, awserr.Validation("One or more parameter values were invalid: Condition parameter type does not match schema type")
	}
	pk, err = model.SerializeKeyValue(pkValue)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	scanForward := true
	if req.ScanIndexForward != nil {
		scanForward = *req.ScanIndexForward
	}
	if skToken != nil {
		skAttr, err := resolveNameStrict(skToken.attr, req.ExpressionAttributeNames)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if skAttr != targetRangeKey {
			return nil, awserr.Validation("sort key condition must target RANGE key")
		}
	}
	queryAdapter := queryHTTPAdapter{
		skToken:              skToken,
		expressionNames:      req.ExpressionAttributeNames,
		expressionValues:     expressionValues,
		targetRangeType:      targetRangeType,
		filterExpression:     req.FilterExpression,
		projectionExpression: projectionExpression,
	}

	items, err := s.queryService.QueryItems(r.Context(), queryapp.QueryItemsInput{
		Target:              target,
		IndexName:           strings.TrimSpace(req.IndexName),
		PK:                  pk,
		ExclusiveStartKey:   req.ExclusiveStartKey,
		ScanForward:         scanForward,
		HasSortKeyCondition: skToken != nil,
		Orderer:             queryAdapter,
	})
	if err != nil {
		return nil, err
	}

	limit := parseLimit(req.Limit)
	processed, err := s.queryService.ProcessQueryItems(queryapp.QueryProcessInput{
		Table:          t,
		Items:          items,
		Limit:          limit,
		SelectMode:     selectMode,
		ConsistentRead: req.ConsistentRead,
		IndexName:      strings.TrimSpace(req.IndexName),
		TargetHashKey:  targetHashKey,
		PK:             pk,
		QueryGSI:       queryGSI,
		QueryLSI:       queryLSI,
		Processor:      queryAdapter,
	})
	if err != nil {
		return nil, awserr.Validation(filterExpressionValidationMessage(err))
	}

	resp := map[string]any{"Count": processed.Count, "ScannedCount": processed.Scanned}
	if selectMode != "COUNT" {
		resp["Items"] = processed.Items
	}
	if err := s.ensureReadCapacity(t, processed.TotalRead); err != nil {
		return nil, err
	}
	indexType := ""
	if queryGSI != nil {
		indexType = "GSI"
	}
	if queryLSI != nil {
		indexType = "LSI"
	}
	setSingleQueryConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, req.IndexName, indexType, processed.TotalRead)
	if limit > 0 && processed.Scanned == limit && processed.LastScanned != nil {
		resp["LastEvaluatedKey"] = processed.LastScanned
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

	t, items, err := s.queryService.ScanItems(r.Context(), queryapp.ScanItemsInput{
		TableName: req.TableName,
		ResolveStartKey: func(t model.Table) (string, string, error) {
			if len(req.ExclusiveStartKey) == 0 {
				return "", "", nil
			}
			pk, sk, err := model.ExtractKey(t, req.ExclusiveStartKey)
			if err != nil {
				return "", "", awserr.Validation(err.Error())
			}
			return pk, sk, nil
		},
	})
	if err != nil {
		return nil, err
	}
	scanAdapter := queryHTTPAdapter{
		expressionNames:      req.ExpressionAttributeNames,
		expressionValues:     req.ExpressionAttributeValues,
		filterExpression:     req.FilterExpression,
		projectionExpression: req.ProjectionExpression,
	}

	limit := parseLimit(req.Limit)
	processed, err := s.queryService.ProcessScanItems(queryapp.ScanProcessInput{
		Table:          t,
		Items:          items,
		Limit:          limit,
		SegmentEnabled: segmentEnabled,
		Segment:        segment,
		TotalSegments:  totalSegments,
		ConsistentRead: req.ConsistentRead,
		Processor:      scanAdapter,
	})
	if err != nil {
		return nil, awserr.Validation(filterExpressionValidationMessage(err))
	}

	resp := map[string]any{"Items": processed.Items, "Count": len(processed.Items), "ScannedCount": processed.Scanned}
	if err := s.ensureReadCapacity(t, processed.TotalRead); err != nil {
		return nil, err
	}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, processed.TotalRead, 0)
	if limit > 0 && processed.Scanned == limit && processed.LastScanned != nil {
		resp["LastEvaluatedKey"] = processed.LastScanned
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

	input := itemopsapp.BatchGetInput{RequestItems: map[string]itemopsapp.BatchGetTableRequest{}, ProcessLimit: processingLimitFromEnv("PINAX_BATCH_GET_PROCESS_LIMIT"), ReserveRead: s.reserveReadCapacity}
	for tableName, itemReq := range req.RequestItems {
		projectionExpression, err := normalizeLegacyAttributesProjection(itemReq.AttributesToGet, itemReq.ProjectionExpression)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if _, err := applyProjection(map[string]any{}, projectionExpression, itemReq.ExpressionAttributeNames); err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if len(itemReq.Keys) == 0 {
			return nil, awserr.Validation("BatchGetItem request for table " + tableName + " must include at least one key")
		}
		names := itemReq.ExpressionAttributeNames
		input.RequestItems[tableName] = itemopsapp.BatchGetTableRequest{
			Keys:           itemReq.Keys,
			ConsistentRead: itemReq.ConsistentRead,
			Project: func(item map[string]any) (map[string]any, error) {
				return applyProjection(item, projectionExpression, names)
			},
		}
	}

	result, err := s.itemOpsService.BatchGet(r.Context(), input)
	if err != nil {
		mapped := mapAppError(err)
		if mapped != err {
			return nil, mapped
		}
		return nil, err
	}

	responses := map[string]any{}
	for tableName, items := range result.Responses {
		responses[tableName] = items
	}
	unprocessed := map[string]any{}
	for tableName, keys := range result.Unprocessed {
		itemReq := req.RequestItems[tableName]
		entry := map[string]any{
			"Keys":                     keys,
			"ConsistentRead":           itemReq.ConsistentRead,
			"ProjectionExpression":     itemReq.ProjectionExpression,
			"ExpressionAttributeNames": itemReq.ExpressionAttributeNames,
		}
		if len(itemReq.AttributesToGet) > 0 {
			entry["AttributesToGet"] = itemReq.AttributesToGet
		}
		unprocessed[tableName] = entry
	}

	resp := map[string]any{"Responses": responses, "UnprocessedKeys": unprocessed}
	for tableName, units := range result.ReadByTable {
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

	input := itemopsapp.BatchWriteInput{
		RequestItems:       map[string][]itemopsapp.BatchWriteOperation{},
		ProcessLimit:       processingLimitFromEnv("PINAX_BATCH_WRITE_PROCESS_LIMIT"),
		IncludeItemMetrics: itemCollectionMode == "SIZE",
		ReserveWrite:       s.reserveWriteCapacity,
		EmitMutation: func(ctx context.Context, repos uow.Repos, t model.Table, eventName string, key map[string]any, oldImage map[string]any, newImage map[string]any, changedAt int64) error {
			return s.emitMutationEventForWrite(ctx, repos, t, eventName, keyAttributesFromKey(t, key), oldImage, newImage, changedAt)
		},
	}
	for tableName, ops := range req.RequestItems {
		if len(ops) == 0 {
			return nil, awserr.Validation("BatchWriteItem request for table " + tableName + " must include at least one write request")
		}
		converted := make([]itemopsapp.BatchWriteOperation, 0, len(ops))
		for _, op := range ops {
			converted = append(converted, itemopsapp.BatchWriteOperation{
				PutItem:    op.PutRequest.Item,
				DeleteKey:  op.DeleteRequest.Key,
				RawRequest: op,
			})
		}
		input.RequestItems[tableName] = converted
	}

	result, err := s.itemOpsService.BatchWrite(r.Context(), input)
	if err != nil {
		mapped := mapAppError(err)
		if mapped != err {
			return nil, mapped
		}
		return nil, err
	}

	unprocessed := map[string]any{}
	for tableName, ops := range result.Unprocessed {
		unprocessed[tableName] = ops
	}
	resp := map[string]any{"UnprocessedItems": unprocessed}
	for tableName, units := range result.WriteByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	if itemCollectionMode == "SIZE" {
		resp["ItemCollectionMetrics"] = result.ItemCollectionMetrics
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

type transactGetHTTPAdapter struct {
	server *Server
}

func (a transactGetHTTPAdapter) ApplyProjection(item map[string]any, projection string, names map[string]string) (map[string]any, error) {
	return applyProjection(item, projection, names)
}

func (a transactGetHTTPAdapter) EnsureRead(t model.Table, units float64) error {
	return a.server.ensureReadCapacity(t, units)
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

	input := transactionapp.TransactGetInput{
		Items:   make([]transactionapp.TransactGetItem, 0, len(req.TransactItems)),
		Adapter: transactGetHTTPAdapter{server: s},
	}
	for _, txItem := range req.TransactItems {
		input.Items = append(input.Items, transactionapp.TransactGetItem{
			TableName:            txItem.Get.TableName,
			Key:                  txItem.Get.Key,
			ProjectionExpression: txItem.Get.ProjectionExpression,
			ExpressionNames:      txItem.Get.ExpressionAttributeNames,
		})
	}
	result, err := s.transactionService.TransactGet(r.Context(), input)
	if err != nil {
		mapped := mapAppError(err)
		if mapped != err {
			return nil, mapped
		}
		return nil, err
	}

	resp := map[string]any{"Responses": result.Responses}
	for tableName, units := range result.ReadByTable {
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

type transactWriteHTTPAdapter struct {
	server *Server
	req    transactWriteRequest
}

func (a transactWriteHTTPAdapter) ValidateReturnOnFail(v string) error {
	return validateReturnValuesOnConditionCheckFailure(v)
}

func (a transactWriteHTTPAdapter) EvaluateCondition(expression string, item map[string]any, names map[string]string, values map[string]any) (bool, error) {
	return expr.Evaluate(expression, item, names, values)
}

func (a transactWriteHTTPAdapter) ApplyUpdate(current map[string]any, updateExpression string, names map[string]string, values map[string]any) (map[string]any, error) {
	plan, err := parseUpdateExpression(updateExpression, names, values)
	if err != nil {
		return nil, err
	}
	updated, _, err := applyUpdatePlan(current, plan)
	return updated, err
}

func (a transactWriteHTTPAdapter) EmitMutation(ctx context.Context, repos uow.Repos, t model.Table, eventName string, key map[string]any, oldImage map[string]any, newImage map[string]any, changedAt int64) error {
	return a.server.emitMutationEventForWrite(ctx, repos, t, eventName, keyAttributesFromKey(t, key), oldImage, newImage, changedAt)
}

func (a transactWriteHTTPAdapter) EnsureWrite(t model.Table, units float64) error {
	return a.server.ensureWriteCapacity(t, units)
}

func (a transactWriteHTTPAdapter) OnConditionEvalError(total int, failedIndex int, message string) error {
	return transactionValidationCanceled(total, failedIndex, message)
}

func (a transactWriteHTTPAdapter) OnConditionFailed(total int, failedIndex int, returnValues string, current map[string]any, existed bool) error {
	reasons := buildTransactionCancellationReasons(total, failedIndex, awserr.CancellationReason{
		Code:    "ConditionalCheckFailed",
		Message: "The conditional request failed",
		Item:    itemForConditionFailure(returnValues, current, existed),
	})
	return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
}

func (a transactWriteHTTPAdapter) BuildResponse(writeByTable map[string]float64) map[string]any {
	resp := map[string]any{}
	for tableName, units := range writeByTable {
		addConsumedCapacity(resp, a.req.ReturnConsumedCapacity, tableName, 0, units)
	}
	return resp
}

func (a transactWriteHTTPAdapter) RequestHash() (string, error) {
	return transactWriteRequestHash(a.req)
}

func (a transactWriteHTTPAdapter) IdempotentMismatchErr() error {
	return awserr.IdempotentParameterMismatch("The provided client token is already in use with different parameters")
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
	input := transactionapp.TransactWriteInput{
		ClientRequestToken:   strings.TrimSpace(req.ClientRequestToken),
		NowMillis:            time.Now().UnixMilli(),
		IdempotencyTTLMillis: int64((10 * time.Minute) / time.Millisecond),
		Adapter:              transactWriteHTTPAdapter{server: s, req: req},
	}
	input.Items = make([]transactionapp.TransactWriteItem, 0, len(req.TransactItems))
	for _, txItem := range req.TransactItems {
		item := transactionapp.TransactWriteItem{}
		if len(txItem.Put.Item) > 0 {
			item.Put = &transactionapp.PutAction{TableName: txItem.Put.TableName, Item: txItem.Put.Item, Condition: txItem.Put.ConditionExpression, ReturnOnFail: txItem.Put.ReturnValuesOnConditionCheckFailure, ExprNames: txItem.Put.ExpressionAttributeNames, ExprValues: txItem.Put.ExpressionAttributeValues}
		}
		if len(txItem.Delete.Key) > 0 {
			item.Delete = &transactionapp.DeleteAction{TableName: txItem.Delete.TableName, Key: txItem.Delete.Key, Condition: txItem.Delete.ConditionExpression, ReturnOnFail: txItem.Delete.ReturnValuesOnConditionCheckFailure, ExprNames: txItem.Delete.ExpressionAttributeNames, ExprValues: txItem.Delete.ExpressionAttributeValues}
		}
		if len(txItem.Update.Key) > 0 {
			item.Update = &transactionapp.UpdateAction{TableName: txItem.Update.TableName, Key: txItem.Update.Key, UpdateExpression: txItem.Update.UpdateExpression, Condition: txItem.Update.ConditionExpression, ReturnOnFail: txItem.Update.ReturnValuesOnConditionCheckFailure, ExprNames: txItem.Update.ExpressionAttributeNames, ExprValues: txItem.Update.ExpressionAttributeValues}
		}
		if len(txItem.ConditionCheck.Key) > 0 {
			item.ConditionCheck = &transactionapp.ConditionCheckAction{TableName: txItem.ConditionCheck.TableName, Key: txItem.ConditionCheck.Key, Condition: txItem.ConditionCheck.ConditionExpression, ReturnOnFail: txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure, ExprNames: txItem.ConditionCheck.ExpressionAttributeNames, ExprValues: txItem.ConditionCheck.ExpressionAttributeValues}
		}
		input.Items = append(input.Items, item)
	}

	result, err := s.transactionService.TransactWrite(r.Context(), input)
	if err != nil {
		mapped := mapAppError(err)
		if mapped != err {
			return nil, mapped
		}
		return nil, err
	}
	if result.Response == nil {
		return map[string]any{}, nil
	}
	return result.Response, nil
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
