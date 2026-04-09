package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/expr"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/store"
)

const targetPrefix = "DynamoDB_20120810."

type Server struct {
	store             store.Store
	requestAuthorizer authorization.RequestAuthorizer
}

func NewServer(store store.Store, requestAuthorizer authorization.RequestAuthorizer) *Server {
	return &Server{store: store, requestAuthorizer: requestAuthorizer}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	target := strings.TrimSpace(r.Header.Get("X-Amz-Target"))
	if !strings.HasPrefix(target, targetPrefix) {
		awserr.Write(w, awserr.Validation("X-Amz-Target header must look like DynamoDB_20120810.<Operation>"))
		return
	}
	op := strings.TrimPrefix(target, targetPrefix)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		awserr.Write(w, err)
		return
	}

	if err := s.authorizeRequest(r, op, body); err != nil {
		awserr.Write(w, err)
		return
	}

	resp, err := s.dispatch(r, op, body)
	if err != nil {
		slog.Debug("request failed", "operation", op, "err", err)
		awserr.Write(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("encode response", "operation", op, "err", err)
	}
}

func (s *Server) authorizeRequest(r *http.Request, operation string, body []byte) error {
	if s.requestAuthorizer == nil {
		return nil
	}

	var payload struct {
		TableName string `json:"TableName"`
	}
	if len(strings.TrimSpace(string(body))) > 0 {
		_ = json.Unmarshal(body, &payload)
	}

	var tableName *string
	if payload.TableName != "" {
		tableName = &payload.TableName
	}

	var accessKeyID *string
	if v, ok := r.Context().Value(authentication.AccessKeyIDContextKey{}).(string); ok && v != "" {
		accessKeyID = &v
	}

	clientIP := getClientIP(r)
	remoteIP := getRemoteIP(r.RemoteAddr)

	authorized, err := s.requestAuthorizer.AuthorizeRequest(r.Context(), &authorization.Request{
		Operation: operation,
		Authorization: authorization.Authorization{
			AccessKeyID: accessKeyID,
		},
		TableName: tableName,
		HTTPRequest: authorization.HTTPRequest{
			Method:      r.Method,
			Path:        r.URL.Path,
			Query:       r.URL.RawQuery,
			QueryParams: r.URL.Query(),
			Headers:     r.Header,
			Host:        r.Host,
			Proto:       r.Proto,
			RemoteAddr:  r.RemoteAddr,
			RemoteIP:    remoteIP,
			ClientIP:    clientIP,
			Scheme:      getScheme(r),
		},
	})
	if err != nil {
		return awserr.Internal("authorization error")
	}
	if !authorized {
		return &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	return nil
}

func getRemoteIP(remoteAddr string) *string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}
	ipString := ip.String()
	return &ipString
}

func getClientIP(r *http.Request) *string {
	if v, ok := r.Context().Value(authentication.ClientIPContextKey{}).(string); ok && v != "" {
		return &v
	}
	return getRemoteIP(r.RemoteAddr)
}

func getScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func (s *Server) dispatch(r *http.Request, operation string, body []byte) (map[string]any, error) {
	switch operation {
	case "CreateTable":
		return s.createTable(r, body)
	case "DescribeTable":
		return s.describeTable(r, body)
	case "ListTables":
		return s.listTables(r, body)
	case "DeleteTable":
		return s.deleteTable(r, body)
	case "UpdateTable":
		return s.updateTable(r, body)
	case "PutItem":
		return s.putItem(r, body)
	case "GetItem":
		return s.getItem(r, body)
	case "DeleteItem":
		return s.deleteItem(r, body)
	case "UpdateItem":
		return s.updateItem(r, body)
	case "Query":
		return s.query(r, body)
	case "Scan":
		return s.scan(r, body)
	case "BatchGetItem":
		return s.batchGetItem(r, body)
	case "BatchWriteItem":
		return s.batchWriteItem(r, body)
	case "TransactGetItems":
		return s.transactGetItems(r, body)
	case "TransactWriteItems":
		return s.transactWriteItems(r, body)
	case "UpdateTimeToLive":
		return s.updateTimeToLive(r, body)
	case "DescribeTimeToLive":
		return s.describeTimeToLive(r, body)
	default:
		return nil, awserr.Validation("unsupported operation " + operation)
	}
}

type createTableRequest struct {
	TableName             string `json:"TableName"`
	BillingMode           string `json:"BillingMode"`
	ProvisionedThroughput *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughput"`
	AttributeDefinitions []struct {
		AttributeName string `json:"AttributeName"`
		AttributeType string `json:"AttributeType"`
	} `json:"AttributeDefinitions"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType       string `json:"KeyType"`
	} `json:"KeySchema"`
	GlobalSecondaryIndexes []struct {
		IndexName string `json:"IndexName"`
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

	now := time.Now().Unix()
	t := model.Table{
		Name:               req.TableName,
		HashKey:            hashKey,
		HashType:           hashType,
		RangeKey:           rangeKey,
		RangeType:          attrType[rangeKey],
		BillingMode:        billingMode,
		ReadCapacityUnits:  readCapacityUnits,
		WriteCapacityUnits: writeCapacityUnits,
		Status:             model.TableStatusCreating,
		StatusAt:           now,
		CreatedAt:          now,
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
		t.GSIs = append(t.GSIs, model.GlobalSecondaryIndex{
			IndexName:      g.IndexName,
			HashKey:        gHash,
			HashType:       attrType[gHash],
			RangeKey:       gRange,
			RangeType:      attrType[gRange],
			Status:         model.IndexStatusActive,
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
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if err := s.store.CreateTable(r.Context(), tx, t); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, awserr.ResourceInUse("Table already exists: " + req.TableName)
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
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
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTableWithLifecycle(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	count, err := s.store.CountItems(r.Context(), tx, t.Name)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"Table": t.Description(count)}, nil
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
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	names, err := s.store.ListTables(r.Context(), tx, req.ExclusiveStartTableName, req.Limit)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		t, err := s.getTableWithLifecycle(r.Context(), tx, name)
		if err != nil {
			var apiErr *awserr.APIError
			if errors.As(err, &apiErr) && apiErr.Code == "ResourceNotFoundException" {
				continue
			}
			return nil, err
		}
		filtered = append(filtered, t.Name)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{"TableNames": filtered}
	if req.Limit > 0 && len(filtered) == req.Limit {
		resp["LastEvaluatedTableName"] = filtered[len(filtered)-1]
	}
	return resp, nil
}

func (s *Server) deleteTable(r *http.Request, body []byte) (map[string]any, error) {
	var req tableNameRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTableWithLifecycle(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	if t.Status == model.TableStatusDeleting {
		return nil, awserr.ResourceInUse("Table is currently " + t.Status)
	}
	count, err := s.store.CountItems(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	t.Status = model.TableStatusDeleting
	t.StatusAt = time.Now().Unix() + 1
	if err := s.store.UpdateTableIndexes(r.Context(), tx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"TableDescription": t.Description(count)}, nil
}

type putItemRequest struct {
	TableName                 string            `json:"TableName"`
	Item                      map[string]any    `json:"Item"`
	ReturnValues              string            `json:"ReturnValues"`
	ConditionExpression       string            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) putItem(r *http.Request, body []byte) (map[string]any, error) {
	var req putItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if model.ItemTooLarge(req.Item) {
		return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	pk, sk, err := model.ExtractItemKeys(t, req.Item)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := model.ValidateSecondaryIndexKeyTypes(t, req.Item); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
	existed := true
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		existed = false
		current = map[string]any{}
	}
	ok, err := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if !ok {
		return nil, awserr.ConditionalCheckFailed("The conditional request failed")
	}

	if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, req.Item); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(req.Item)))

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
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	pk, sk, err := model.ExtractKey(t, req.Key)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	item, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			resp := map[string]any{}
			setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, model.CalculateReadCapacityUnits(1, req.ConsistentRead), 0)
			return resp, nil
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	projected, err := applyProjection(item, req.ProjectionExpression, req.ExpressionAttributeNames)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resp := map[string]any{"Item": projected}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead), 0)
	return resp, nil
}

type deleteItemRequest struct {
	TableName                 string            `json:"TableName"`
	Key                       map[string]any    `json:"Key"`
	ReturnValues              string            `json:"ReturnValues"`
	ConditionExpression       string            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) deleteItem(r *http.Request, body []byte) (map[string]any, error) {
	var req deleteItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	pk, sk, err := model.ExtractKey(t, req.Key)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
	existed := true
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		existed = false
		current = map[string]any{}
	}
	ok, err := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if !ok {
		return nil, awserr.ConditionalCheckFailed("The conditional request failed")
	}
	if err := s.store.DeleteItem(r.Context(), tx, t.Name, pk, sk); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current)))

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

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	pk, sk, err := model.ExtractKey(t, req.Key)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
	oldItem := map[string]any{}
	itemExisted := true
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			itemExisted = false
			current = map[string]any{t.HashKey: req.Key[t.HashKey]}
			if t.RangeKey != "" {
				current[t.RangeKey] = req.Key[t.RangeKey]
			}
		} else {
			return nil, err
		}
	}
	if itemExisted {
		oldItem = cloneItem(current)
	}

	ok, err := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if !ok {
		return nil, awserr.ConditionalCheckFailed("The conditional request failed")
	}

	plan, err := parseUpdateExpression(req.UpdateExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	updated, _, err := applyUpdatePlan(current, plan)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := model.ValidateSecondaryIndexKeyTypes(t, updated); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if model.ItemTooLarge(updated) {
		return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
	}
	if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, updated); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated)))

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
	ProvisionedThroughput *struct {
		ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
		WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
	} `json:"ProvisionedThroughput"`
	AttributeDefinitions []struct {
		AttributeName string `json:"AttributeName"`
		AttributeType string `json:"AttributeType"`
	} `json:"AttributeDefinitions"`
	GlobalSecondaryIndexUpdates []struct {
		Create struct {
			IndexName string `json:"IndexName"`
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
	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTableWithLifecycle(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	if t.Status != model.TableStatusActive {
		return nil, awserr.ResourceInUse("Table is currently " + t.Status)
	}
	if strings.TrimSpace(req.BillingMode) != "" || req.ProvisionedThroughput != nil {
		billingMode, readCapacityUnits, writeCapacityUnits, err := normalizeBillingConfig(req.BillingMode, req.ProvisionedThroughput)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		t.BillingMode = billingMode
		t.ReadCapacityUnits = readCapacityUnits
		t.WriteCapacityUnits = writeCapacityUnits
		if err := s.store.UpdateTableBilling(r.Context(), tx, t.Name, billingMode, readCapacityUnits, writeCapacityUnits); err != nil {
			return nil, err
		}
	}

	if len(req.GlobalSecondaryIndexUpdates) > 0 {
		attrTypes := map[string]string{}
		for _, d := range req.AttributeDefinitions {
			if strings.TrimSpace(d.AttributeName) != "" {
				attrTypes[d.AttributeName] = d.AttributeType
			}
		}
		now := time.Now().Unix()
		updatedGSIs, err := applyGSIUpdates(t, req.GlobalSecondaryIndexUpdates, attrTypes, now)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		t.GSIs = updatedGSIs
		t.Status = model.TableStatusUpdating
		t.StatusAt = now + 1
		if err := s.store.UpdateTableIndexes(r.Context(), tx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs); err != nil {
			return nil, err
		}
		items, err := s.store.Scan(r.Context(), tx, t.Name, "", "", 0)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			pk, sk, err := model.ExtractItemKeys(t, item)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, item); err != nil {
				return nil, err
			}
		}
	}

	count, err := s.store.CountItems(r.Context(), tx, t.Name)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"TableDescription": t.Description(count)}, nil
}

func (s *Server) refreshTableLifecycle(ctx context.Context, tx *sql.Tx, t *model.Table) error {
	now := time.Now().Unix()
	if t.Status == model.TableStatusDeleting && t.StatusAt > 0 && now >= t.StatusAt {
		if err := s.store.DeleteTable(ctx, tx, t.Name); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if t.Status == model.TableStatusCreating && t.StatusAt > 0 && now >= t.StatusAt {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
	}
	if !advanceTableLifecycle(t, now) {
		return nil
	}
	return s.store.UpdateTableIndexes(ctx, tx, t.Name, t.Status, t.StatusAt, t.GSIs, t.LSIs)
}

func (s *Server) getTableWithLifecycle(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error) {
	t, err := s.store.GetTable(ctx, tx, tableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Table{}, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return model.Table{}, err
	}
	if err := s.refreshTableLifecycle(ctx, tx, &t); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.Table{}, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return model.Table{}, err
	}
	return t, nil
}

func (s *Server) getActiveTable(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error) {
	t, err := s.getTableWithLifecycle(ctx, tx, tableName)
	if err != nil {
		return model.Table{}, err
	}
	if t.Status != model.TableStatusActive {
		return model.Table{}, awserr.ResourceInUse("Table is currently " + t.Status)
	}
	return t, nil
}

func advanceTableLifecycle(t *model.Table, now int64) bool {
	changed := false
	updatedGSIs := make([]model.GlobalSecondaryIndex, 0, len(t.GSIs))
	pending := false

	for _, g := range t.GSIs {
		status := strings.TrimSpace(g.Status)
		if status == "" {
			status = model.IndexStatusActive
		}
		if (status == model.IndexStatusCreating || status == model.IndexStatusDeleting) && g.StatusAt > 0 && now >= g.StatusAt {
			if status == model.IndexStatusDeleting {
				changed = true
				continue
			}
			g.Status = model.IndexStatusActive
			g.StatusAt = 0
			changed = true
		}
		if g.Status == model.IndexStatusCreating || g.Status == model.IndexStatusDeleting {
			pending = true
		}
		updatedGSIs = append(updatedGSIs, g)
	}

	if len(updatedGSIs) != len(t.GSIs) {
		changed = true
	}
	t.GSIs = updatedGSIs

	if pending {
		if t.Status != model.TableStatusUpdating {
			t.Status = model.TableStatusUpdating
			changed = true
		}
	} else if t.Status != model.TableStatusActive {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
		changed = true
	}

	if t.Status == model.TableStatusUpdating && t.StatusAt > 0 && now >= t.StatusAt && !pending {
		t.Status = model.TableStatusActive
		t.StatusAt = 0
		changed = true
	}

	return changed
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
		IndexName string
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
}

func applyGSIUpdates(table model.Table, updates []struct {
	Create struct {
		IndexName string `json:"IndexName"`
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
}, attrTypes map[string]string, now int64) ([]model.GlobalSecondaryIndex, error) {
	gsis := append([]model.GlobalSecondaryIndex{}, table.GSIs...)
	touched := map[string]struct{}{}
	for _, u := range updates {
		hasCreate := strings.TrimSpace(u.Create.IndexName) != ""
		hasDelete := strings.TrimSpace(u.Delete.IndexName) != ""
		if hasCreate == hasDelete {
			return nil, fmt.Errorf("each GlobalSecondaryIndexUpdates entry must include exactly one of Create or Delete")
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
					g.StatusAt = now + 1
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
		gsis = append(gsis, model.GlobalSecondaryIndex{
			IndexName:      name,
			HashKey:        hashKey,
			HashType:       hashType,
			RangeKey:       rangeKey,
			RangeType:      rangeType,
			Status:         model.IndexStatusCreating,
			StatusAt:       now + 1,
			ProjectionType: projectionType,
			NonKeyAttrs:    nonKeyAttrs,
		})
	}
	sort.Slice(gsis, func(i, j int) bool { return gsis[i].IndexName < gsis[j].IndexName })
	return gsis, nil
}

type queryRequest struct {
	TableName                 string            `json:"TableName"`
	IndexName                 string            `json:"IndexName"`
	KeyConditionExpression    string            `json:"KeyConditionExpression"`
	FilterExpression          string            `json:"FilterExpression"`
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

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	if err := s.refreshTableLifecycle(r.Context(), tx, &t); err != nil {
		return nil, err
	}
	pkToken, skToken, err := parseKeyCondition(req.KeyConditionExpression)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	targetHashKey := t.HashKey
	targetRangeKey := t.RangeKey
	targetHashType := t.HashType
	targetRangeType := t.RangeType
	var queryGSI *model.GlobalSecondaryIndex
	var queryLSI *model.LocalSecondaryIndex
	if strings.TrimSpace(req.IndexName) != "" {
		gsi, ok := t.GetGSI(req.IndexName)
		if ok {
			if gsi.Status != "" && gsi.Status != model.IndexStatusActive {
				return nil, awserr.ResourceInUse("Index " + req.IndexName + " is not ACTIVE")
			}
			if req.ConsistentRead {
				return nil, awserr.Validation("ConsistentRead is not supported on global secondary indexes")
			}
			queryGSI = &gsi
			targetHashKey = gsi.HashKey
			targetRangeKey = gsi.RangeKey
			targetHashType = gsi.HashType
			targetRangeType = gsi.RangeType
		} else {
			lsi, ok := t.GetLSI(req.IndexName)
			if !ok {
				return nil, awserr.Validation("unknown index " + req.IndexName)
			}
			queryLSI = &lsi
			targetHashKey = t.HashKey
			targetRangeKey = lsi.RangeKey
			targetHashType = t.HashType
			targetRangeType = lsi.RangeType
		}
	}

	pkAttr, err := resolveNameStrict(pkToken.attr, req.ExpressionAttributeNames)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if pkAttr != targetHashKey {
		return nil, awserr.Validation("partition key condition must target HASH key")
	}
	pkValue, ok := req.ExpressionAttributeValues[pkToken.value]
	if !ok {
		return nil, awserr.Validation("missing partition key expression value")
	}
	if err := model.ValidateKeyAttributeType(pkValue, targetHashType, targetHashKey); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	pk, err := model.SerializeKeyValue(pkValue)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	scanForward := true
	if req.ScanIndexForward != nil {
		scanForward = *req.ScanIndexForward
	}

	var items []map[string]any
	if queryGSI != nil {
		items, err = s.store.QueryByGSI(r.Context(), tx, t.Name, req.IndexName, pk, "", true, 0)
		if err != nil {
			return nil, err
		}
		items, err = orderItemsForGSI(items, t, *queryGSI, req.ExclusiveStartKey, scanForward)
	} else if queryLSI != nil {
		items, err = s.store.QueryByPK(r.Context(), tx, t.Name, pk, "", true, 0)
		if err != nil {
			return nil, err
		}
		items, err = orderItemsForLSI(items, t, *queryLSI, req.ExclusiveStartKey, scanForward)
	} else if skToken == nil {
		if targetRangeKey == "" {
			items, err = s.store.QueryByPKSK(r.Context(), tx, t.Name, pk, model.NoSortKey)
		} else {
			items, err = s.store.QueryByPK(r.Context(), tx, t.Name, pk, "", true, 0)
			if err == nil {
				items, err = orderItemsForTable(items, t, req.ExclusiveStartKey, scanForward)
			}
		}
	} else {
		skAttr, err := resolveNameStrict(skToken.attr, req.ExpressionAttributeNames)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if skAttr != targetRangeKey {
			return nil, awserr.Validation("sort key condition must target RANGE key")
		}
		items, err = s.store.QueryByPK(r.Context(), tx, t.Name, pk, "", true, 0)
		if err == nil {
			items, err = orderItemsForTable(items, t, req.ExclusiveStartKey, scanForward)
		}
	}
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
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
			ok, err := sortConditionMatches(queryItem, skToken, req.ExpressionAttributeNames, req.ExpressionAttributeValues, targetRangeType)
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

		matches, err := applyFilter(queryItem, req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if matches {
			projected, err := applyProjection(queryItem, req.ProjectionExpression, req.ExpressionAttributeNames)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			filtered = append(filtered, projected)
			count++
		}

		if limit > 0 && scanned >= limit {
			break
		}
	}

	resp := map[string]any{"Items": filtered, "Count": count, "ScannedCount": scanned}
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
	ConsistentRead            bool              `json:"ConsistentRead"`
	ReturnConsumedCapacity    string            `json:"ReturnConsumedCapacity"`
}

func (s *Server) scan(r *http.Request, body []byte) (map[string]any, error) {
	var req scanRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getActiveTable(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}

	startPK := ""
	startSK := ""
	if len(req.ExclusiveStartKey) > 0 {
		startPK, startSK, err = model.ExtractKey(t, req.ExclusiveStartKey)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
	}
	items, err := s.store.Scan(r.Context(), tx, t.Name, startPK, startSK, 0)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	limit := parseLimit(req.Limit)
	filtered := make([]map[string]any, 0)
	scanned := 0
	var lastScanned map[string]any
	totalRead := 0.0
	for _, item := range items {
		scanned++
		lastScanned = keyFromItem(t, item)
		totalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead)

		matches, err := applyFilter(item, req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if err != nil {
			return nil, awserr.Validation(err.Error())
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

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	batchGetProcessLimit := processingLimitFromEnv("PINAX_BATCH_GET_PROCESS_LIMIT")
	processed := 0
	responses := map[string]any{}
	unprocessed := map[string]any{}
	readByTable := map[string]float64{}
	for tableName, itemReq := range req.RequestItems {
		if len(itemReq.Keys) == 0 {
			return nil, awserr.Validation("BatchGetItem request for table " + tableName + " must include at least one key")
		}
		t, err := s.getActiveTable(r.Context(), tx, tableName)
		if err != nil {
			return nil, err
		}
		seenKeys := map[string]struct{}{}
		items := make([]map[string]any, 0, len(itemReq.Keys))
		unprocessedKeys := make([]map[string]any, 0)
		for _, key := range itemReq.Keys {
			pk, sk, err := model.ExtractKey(t, key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := tableName + "|" + pk + "|" + sk
			if _, exists := seenKeys[target]; exists {
				return nil, awserr.Validation("BatchGetItem contains duplicate keys")
			}
			seenKeys[target] = struct{}{}

			if batchGetProcessLimit > 0 && processed >= batchGetProcessLimit {
				unprocessedKeys = append(unprocessedKeys, key)
				continue
			}
			processed++

			item, err := s.store.GetItem(r.Context(), tx, tableName, pk, sk)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					readByTable[tableName] += model.CalculateReadCapacityUnits(1, itemReq.ConsistentRead)
					continue
				}
				return nil, err
			}
			projected, err := applyProjection(item, itemReq.ProjectionExpression, itemReq.ExpressionAttributeNames)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			items = append(items, projected)
			readByTable[tableName] += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), itemReq.ConsistentRead)
		}
		responses[tableName] = items
		if len(unprocessedKeys) > 0 {
			unprocessed[tableName] = map[string]any{
				"Keys":                     unprocessedKeys,
				"ConsistentRead":           itemReq.ConsistentRead,
				"ProjectionExpression":     itemReq.ProjectionExpression,
				"ExpressionAttributeNames": itemReq.ExpressionAttributeNames,
			}
		}
	}

	if err := tx.Commit(); err != nil {
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
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
}

func (s *Server) batchWriteItem(r *http.Request, body []byte) (map[string]any, error) {
	var req batchWriteItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
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

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tableNames := make([]string, 0, len(req.RequestItems))
	for tableName := range req.RequestItems {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	batchWriteProcessLimit := processingLimitFromEnv("PINAX_BATCH_WRITE_PROCESS_LIMIT")
	processed := 0
	unprocessed := map[string]any{}
	writeByTable := map[string]float64{}

	for _, tableName := range tableNames {
		ops := req.RequestItems[tableName]
		if len(ops) == 0 {
			return nil, awserr.Validation("BatchWriteItem request for table " + tableName + " must include at least one write request")
		}
		t, err := s.getActiveTable(r.Context(), tx, tableName)
		if err != nil {
			return nil, err
		}

		seenKeys := map[string]struct{}{}
		for _, op := range ops {
			if len(op.PutRequest.Item) == 0 && len(op.DeleteRequest.Key) == 0 {
				return nil, awserr.Validation("write request must contain either PutRequest or DeleteRequest")
			}
			if len(op.PutRequest.Item) > 0 && len(op.DeleteRequest.Key) > 0 {
				return nil, awserr.Validation("write request cannot contain both PutRequest and DeleteRequest")
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
					return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
				}
				pk, sk, err := model.ExtractItemKeys(t, op.PutRequest.Item)
				if err != nil {
					return nil, awserr.Validation(err.Error())
				}
				if err := model.ValidateSecondaryIndexKeyTypes(t, op.PutRequest.Item); err != nil {
					return nil, awserr.Validation(err.Error())
				}
				k := tableName + "|" + pk + "|" + sk
				if _, exists := seenKeys[k]; exists {
					return nil, awserr.Validation("BatchWriteItem contains duplicate keys")
				}
				seenKeys[k] = struct{}{}
				if err := s.store.PutItem(r.Context(), tx, tableName, pk, sk, op.PutRequest.Item); err != nil {
					return nil, err
				}
				writeByTable[tableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(op.PutRequest.Item))
				processed++
			}
			if len(op.DeleteRequest.Key) > 0 {
				pk, sk, err := model.ExtractKey(t, op.DeleteRequest.Key)
				if err != nil {
					return nil, awserr.Validation(err.Error())
				}
				k := tableName + "|" + pk + "|" + sk
				if _, exists := seenKeys[k]; exists {
					return nil, awserr.Validation("BatchWriteItem contains duplicate keys")
				}
				seenKeys[k] = struct{}{}
				current, err := s.store.GetItem(r.Context(), tx, tableName, pk, sk)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return nil, err
				}
				if errors.Is(err, sql.ErrNoRows) {
					current = op.DeleteRequest.Key
				}
				if err := s.store.DeleteItem(r.Context(), tx, tableName, pk, sk); err != nil {
					return nil, err
				}
				writeByTable[tableName] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
				processed++
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{"UnprocessedItems": unprocessed}
	for tableName, units := range writeByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	return resp, nil
}

type transactGetRequest struct {
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
	TransactItems          []struct {
		Get struct {
			TableName string         `json:"TableName"`
			Key       map[string]any `json:"Key"`
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

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	responses := make([]map[string]any, 0, len(req.TransactItems))
	readByTable := map[string]float64{}
	for _, txItem := range req.TransactItems {
		g := txItem.Get
		t, err := s.getActiveTable(r.Context(), tx, g.TableName)
		if err != nil {
			return nil, err
		}
		pk, sk, err := model.ExtractKey(t, g.Key)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		item, err := s.store.GetItem(r.Context(), tx, g.TableName, pk, sk)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				readByTable[g.TableName] += model.CalculateReadCapacityUnits(1, true)
				responses = append(responses, map[string]any{})
				continue
			}
			return nil, err
		}
		readByTable[g.TableName] += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), true)
		responses = append(responses, map[string]any{"Item": item})
	}

	if err := tx.Commit(); err != nil {
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
	if len(req.TransactItems) == 0 {
		return nil, awserr.Validation("TransactItems is required")
	}
	if len(req.TransactItems) > 25 {
		return nil, awserr.Validation("TransactWriteItems supports at most 25 actions")
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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
			return nil, awserr.Validation("each TransactWriteItem must include exactly one operation")
		}

		if len(txItem.Put.Item) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Put.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.getActiveTable(r.Context(), tx, txItem.Put.TableName)
			if err != nil {
				return nil, err
			}
			pk, sk, err := model.ExtractItemKeys(t, txItem.Put.Item)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "TransactionConflict",
					Message: "Transaction request cannot include multiple operations on one item",
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [TransactionConflict]", reasons)
			}
			seenTargets[target] = struct{}{}

			current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
			itemExisted := true
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				itemExisted = false
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.Put.ConditionExpression, current, txItem.Put.ExpressionAttributeNames, txItem.Put.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "ConditionalCheckFailed",
					Message: "The conditional request failed",
					Item:    itemForConditionFailure(txItem.Put.ReturnValuesOnConditionCheckFailure, current, itemExisted),
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
			}
			if model.ItemTooLarge(txItem.Put.Item) {
				return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
			}
			if err := model.ValidateSecondaryIndexKeyTypes(t, txItem.Put.Item); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, txItem.Put.Item); err != nil {
				return nil, err
			}
			writeByTable[t.Name] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(txItem.Put.Item))
			continue
		}

		if len(txItem.Delete.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Delete.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.getActiveTable(r.Context(), tx, txItem.Delete.TableName)
			if err != nil {
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.Delete.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "TransactionConflict",
					Message: "Transaction request cannot include multiple operations on one item",
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [TransactionConflict]", reasons)
			}
			seenTargets[target] = struct{}{}

			current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
			itemExisted := true
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				itemExisted = false
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.Delete.ConditionExpression, current, txItem.Delete.ExpressionAttributeNames, txItem.Delete.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "ConditionalCheckFailed",
					Message: "The conditional request failed",
					Item:    itemForConditionFailure(txItem.Delete.ReturnValuesOnConditionCheckFailure, current, itemExisted),
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
			}
			if err := s.store.DeleteItem(r.Context(), tx, t.Name, pk, sk); err != nil {
				return nil, err
			}
			writeByTable[t.Name] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
			continue
		}

		if len(txItem.Update.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Update.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.getActiveTable(r.Context(), tx, txItem.Update.TableName)
			if err != nil {
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.Update.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "TransactionConflict",
					Message: "Transaction request cannot include multiple operations on one item",
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [TransactionConflict]", reasons)
			}
			seenTargets[target] = struct{}{}

			existing, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
			itemExisted := true
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				itemExisted = false
				existing = map[string]any{}
			}

			current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					current = map[string]any{t.HashKey: txItem.Update.Key[t.HashKey]}
					if t.RangeKey != "" {
						current[t.RangeKey] = txItem.Update.Key[t.RangeKey]
					}
				} else {
					return nil, err
				}
			}
			ok, err := expr.Evaluate(txItem.Update.ConditionExpression, current, txItem.Update.ExpressionAttributeNames, txItem.Update.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "ConditionalCheckFailed",
					Message: "The conditional request failed",
					Item:    itemForConditionFailure(txItem.Update.ReturnValuesOnConditionCheckFailure, existing, itemExisted),
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
			}
			plan, err := parseUpdateExpression(txItem.Update.UpdateExpression, txItem.Update.ExpressionAttributeNames, txItem.Update.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			updated, _, err := applyUpdatePlan(current, plan)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if err := model.ValidateSecondaryIndexKeyTypes(t, updated); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if model.ItemTooLarge(updated) {
				return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
			}
			if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, updated); err != nil {
				return nil, err
			}
			writeByTable[t.Name] += model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated))
			continue
		}

		if len(txItem.ConditionCheck.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.getActiveTable(r.Context(), tx, txItem.ConditionCheck.TableName)
			if err != nil {
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.ConditionCheck.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "TransactionConflict",
					Message: "Transaction request cannot include multiple operations on one item",
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [TransactionConflict]", reasons)
			}
			seenTargets[target] = struct{}{}

			current, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
			itemExisted := true
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				itemExisted = false
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.ConditionCheck.ConditionExpression, current, txItem.ConditionCheck.ExpressionAttributeNames, txItem.ConditionCheck.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				reasons := buildTransactionCancellationReasons(len(req.TransactItems), i, awserr.CancellationReason{
					Code:    "ConditionalCheckFailed",
					Message: "The conditional request failed",
					Item:    itemForConditionFailure(txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure, current, itemExisted),
				})
				return nil, awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]", reasons)
			}
			continue
		}

		return nil, awserr.Validation("each transact item must contain one operation")
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	for tableName, units := range writeByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	return resp, nil
}

func buildTransactionCancellationReasons(count int, failedIndex int, failed awserr.CancellationReason) []awserr.CancellationReason {
	reasons := make([]awserr.CancellationReason, count)
	for i := 0; i < count; i++ {
		reasons[i] = awserr.CancellationReason{Code: "None"}
	}
	reasons[failedIndex] = failed
	return reasons
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

func decode(body []byte, out any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	var trailing struct{}
	if err := dec.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("invalid request body: unexpected trailing content")
	}
	return nil
}

func resolveName(v string, names map[string]string) string {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "#") {
		if out, ok := names[v]; ok {
			return out
		}
	}
	return v
}

func resolveNameStrict(v string, names map[string]string) (string, error) {
	v = strings.TrimSpace(v)
	if strings.HasPrefix(v, "#") {
		if out, ok := names[v]; ok {
			return out, nil
		}
		return "", fmt.Errorf("missing expression name %q", v)
	}
	return v, nil
}

func normalizeIndexProjection(projectionType string, nonKeyAttrs []string) (string, []string, error) {
	projectionType = strings.TrimSpace(projectionType)
	if projectionType == "" {
		projectionType = "ALL"
	}

	cleaned := make([]string, 0, len(nonKeyAttrs))
	seen := map[string]struct{}{}
	for _, attr := range nonKeyAttrs {
		name := strings.TrimSpace(attr)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cleaned = append(cleaned, name)
	}

	switch projectionType {
	case "ALL":
		if len(cleaned) > 0 {
			return "", nil, fmt.Errorf("NonKeyAttributes is only allowed when ProjectionType is INCLUDE")
		}
		return projectionType, nil, nil
	case "KEYS_ONLY":
		if len(cleaned) > 0 {
			return "", nil, fmt.Errorf("NonKeyAttributes is only allowed when ProjectionType is INCLUDE")
		}
		return projectionType, nil, nil
	case "INCLUDE":
		if len(cleaned) == 0 {
			return "", nil, fmt.Errorf("NonKeyAttributes must be provided when ProjectionType is INCLUDE")
		}
		return projectionType, cleaned, nil
	default:
		return "", nil, fmt.Errorf("unsupported ProjectionType %q", projectionType)
	}
}

func normalizeBillingConfig(mode string, throughput *struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
}) (string, int64, int64, error) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		if throughput != nil {
			mode = "PROVISIONED"
		} else {
			mode = "PAY_PER_REQUEST"
		}
	}
	switch mode {
	case "PAY_PER_REQUEST":
		if throughput != nil && (throughput.ReadCapacityUnits > 0 || throughput.WriteCapacityUnits > 0) {
			return "", 0, 0, fmt.Errorf("ProvisionedThroughput must not be set when BillingMode is PAY_PER_REQUEST")
		}
		return mode, 0, 0, nil
	case "PROVISIONED":
		if throughput == nil {
			return "", 0, 0, fmt.Errorf("ProvisionedThroughput is required when BillingMode is PROVISIONED")
		}
		if throughput.ReadCapacityUnits <= 0 || throughput.WriteCapacityUnits <= 0 {
			return "", 0, 0, fmt.Errorf("ProvisionedThroughput ReadCapacityUnits and WriteCapacityUnits must be greater than 0")
		}
		return mode, throughput.ReadCapacityUnits, throughput.WriteCapacityUnits, nil
	default:
		return "", 0, 0, fmt.Errorf("unsupported BillingMode %q", mode)
	}
}

func projectItemForGSI(item map[string]any, t model.Table, g model.GlobalSecondaryIndex) map[string]any {
	if g.ProjectionType == "ALL" {
		return item
	}

	projected := map[string]any{}
	keys := []string{t.HashKey, t.RangeKey, g.HashKey, g.RangeKey}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if value, ok := item[key]; ok {
			projected[key] = value
		}
	}

	if g.ProjectionType == "INCLUDE" {
		for _, attr := range g.NonKeyAttrs {
			if value, ok := item[attr]; ok {
				projected[attr] = value
			}
		}
	}

	return projected
}

func projectItemForLSI(item map[string]any, t model.Table, l model.LocalSecondaryIndex) map[string]any {
	if l.ProjectionType == "ALL" {
		return item
	}

	projected := map[string]any{}
	keys := []string{t.HashKey, t.RangeKey, l.RangeKey}
	for _, key := range keys {
		if key == "" {
			continue
		}
		if value, ok := item[key]; ok {
			projected[key] = value
		}
	}

	if l.ProjectionType == "INCLUDE" {
		for _, attr := range l.NonKeyAttrs {
			if value, ok := item[attr]; ok {
				projected[attr] = value
			}
		}
	}

	return projected
}

func orderItemsForTable(items []map[string]any, t model.Table, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	type entry struct {
		item map[string]any
		raw  any
		pk   string
		sk   string
	}

	entries := make([]entry, 0, len(items))
	for _, item := range items {
		raw := any(nil)
		if t.RangeKey != "" {
			v, ok := item[t.RangeKey]
			if !ok {
				continue
			}
			raw = v
		}
		pkVal, ok := item[t.HashKey]
		if !ok {
			continue
		}
		pk, err := model.SerializeKeyValue(pkVal)
		if err != nil {
			continue
		}
		sk := model.NoSortKey
		if t.RangeKey != "" {
			skVal, ok := item[t.RangeKey]
			if !ok {
				continue
			}
			sk, err = model.SerializeKeyValue(skVal)
			if err != nil {
				continue
			}
		}
		entries = append(entries, entry{item: item, raw: raw, pk: pk, sk: sk})
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := compareAttributeValues(entries[i].raw, entries[j].raw)
		if cmp == 0 {
			if scanForward {
				return entries[i].sk < entries[j].sk
			}
			return entries[i].sk > entries[j].sk
		}
		if scanForward {
			return cmp < 0
		}
		return cmp > 0
	})

	start := 0
	if len(exclusiveStartKey) > 0 {
		startPK, startSK, err := model.ExtractKey(t, exclusiveStartKey)
		if err != nil {
			return nil, err
		}
		for i, e := range entries {
			if e.pk == startPK && e.sk == startSK {
				start = i + 1
				break
			}
		}
	}

	out := make([]map[string]any, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		out = append(out, entries[i].item)
	}
	return out, nil
}

func orderItemsForGSI(items []map[string]any, t model.Table, g model.GlobalSecondaryIndex, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	type entry struct {
		item map[string]any
		idx  any
		pk   string
		sk   string
	}

	entries := make([]entry, 0, len(items))
	for _, item := range items {
		idx := any(nil)
		if g.RangeKey != "" {
			raw, ok := item[g.RangeKey]
			if !ok {
				continue
			}
			idx = raw
		}

		pkRaw, ok := item[t.HashKey]
		if !ok {
			continue
		}
		pk, err := model.SerializeKeyValue(pkRaw)
		if err != nil {
			continue
		}
		sk := model.NoSortKey
		if t.RangeKey != "" {
			skRaw, ok := item[t.RangeKey]
			if !ok {
				continue
			}
			sk, err = model.SerializeKeyValue(skRaw)
			if err != nil {
				continue
			}
		}
		entries = append(entries, entry{item: item, idx: idx, pk: pk, sk: sk})
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := compareAttributeValues(entries[i].idx, entries[j].idx)
		if cmp == 0 {
			if entries[i].pk == entries[j].pk {
				if scanForward {
					return entries[i].sk < entries[j].sk
				}
				return entries[i].sk > entries[j].sk
			}
			if scanForward {
				return entries[i].pk < entries[j].pk
			}
			return entries[i].pk > entries[j].pk
		}
		if scanForward {
			return cmp < 0
		}
		return cmp > 0
	})

	start := 0
	if len(exclusiveStartKey) > 0 {
		startPK, startSK, err := model.ExtractKey(t, exclusiveStartKey)
		if err != nil {
			return nil, err
		}
		for i, e := range entries {
			if e.pk == startPK && e.sk == startSK {
				start = i + 1
				break
			}
		}
	}

	out := make([]map[string]any, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		out = append(out, entries[i].item)
	}
	return out, nil
}

func orderItemsForLSI(items []map[string]any, t model.Table, l model.LocalSecondaryIndex, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	type entry struct {
		item   map[string]any
		lsiRaw any
		tblRaw any
		tblSK  string
	}

	entries := make([]entry, 0, len(items))
	for _, item := range items {
		raw, ok := item[l.RangeKey]
		if !ok {
			continue
		}
		lsiRaw := raw
		tblSK := model.NoSortKey
		tblRaw := any(nil)
		if t.RangeKey != "" {
			rawTableSK, ok := item[t.RangeKey]
			if !ok {
				continue
			}
			tblRaw = rawTableSK
			var err error
			tblSK, err = model.SerializeKeyValue(rawTableSK)
			if err != nil {
				continue
			}
		}
		entries = append(entries, entry{item: item, lsiRaw: lsiRaw, tblRaw: tblRaw, tblSK: tblSK})
	}

	sort.Slice(entries, func(i, j int) bool {
		cmp := compareAttributeValues(entries[i].lsiRaw, entries[j].lsiRaw)
		if cmp == 0 {
			if scanForward {
				return entries[i].tblSK < entries[j].tblSK
			}
			return entries[i].tblSK > entries[j].tblSK
		}
		if scanForward {
			return cmp < 0
		}
		return cmp > 0
	})

	if len(exclusiveStartKey) == 0 {
		out := make([]map[string]any, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.item)
		}
		return out, nil
	}

	startPK, startSK, err := model.ExtractKey(t, exclusiveStartKey)
	if err != nil {
		return nil, err
	}
	start := 0
	for i, e := range entries {
		pkVal, ok := e.item[t.HashKey]
		if !ok {
			continue
		}
		pk, err := model.SerializeKeyValue(pkVal)
		if err != nil {
			continue
		}
		tblSK := model.NoSortKey
		if t.RangeKey != "" {
			tblSKVal, ok := e.item[t.RangeKey]
			if !ok {
				continue
			}
			tblSK, err = model.SerializeKeyValue(tblSKVal)
			if err != nil {
				continue
			}
		}
		if pk == startPK && tblSK == startSK {
			start = i + 1
			break
		}
	}

	out := make([]map[string]any, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		out = append(out, entries[i].item)
	}
	return out, nil
}

func compareAttributeValues(a any, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	if an, ok := numberFromAttr(a); ok {
		if bn, ok := numberFromAttr(b); ok {
			if an < bn {
				return -1
			}
			if an > bn {
				return 1
			}
			return 0
		}
	}

	as, errA := model.SerializeKeyValue(a)
	bs, errB := model.SerializeKeyValue(b)
	if errA != nil || errB != nil {
		return 0
	}
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func numberFromAttr(v any) (float64, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return 0, false
	}
	n, ok := m["N"].(string)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(n, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func firstNonEmpty(v string, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}

type updateTimeToLiveRequest struct {
	TableName               string `json:"TableName"`
	TimeToLiveSpecification struct {
		Enabled       bool   `json:"Enabled"`
		AttributeName string `json:"AttributeName"`
	} `json:"TimeToLiveSpecification"`
}

func (s *Server) updateTimeToLive(r *http.Request, body []byte) (map[string]any, error) {
	var req updateTimeToLiveRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if req.TableName == "" {
		return nil, awserr.Validation("TableName is required")
	}
	if req.TimeToLiveSpecification.AttributeName == "" {
		return nil, awserr.Validation("AttributeName is required")
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTableWithLifecycle(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}

	ttl := model.TimeToLive{
		Enabled:  req.TimeToLiveSpecification.Enabled,
		AttrName: req.TimeToLiveSpecification.AttributeName,
		StatusAt: time.Now().Unix() + 1,
	}
	if ttl.Enabled {
		ttl.Status = model.TTLStatusEnabling
	} else {
		ttl.Status = model.TTLStatusDisabling
	}
	if err := s.store.UpdateTimeToLive(r.Context(), tx, req.TableName, ttl); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	t.TimeToLive = ttl
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": ttl.Status,
			"AttributeName":    ttl.AttrName,
		},
	}, nil
}

type describeTimeToLiveRequest struct {
	TableName string `json:"TableName"`
}

func (s *Server) describeTimeToLive(r *http.Request, body []byte) (map[string]any, error) {
	var req describeTimeToLiveRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if req.TableName == "" {
		return nil, awserr.Validation("TableName is required")
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	t, err := s.getTableWithLifecycle(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	if t.TimeToLive.Status == model.TTLStatusEnabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
		t.TimeToLive.Status = model.TTLStatusEnabled
		t.TimeToLive.StatusAt = 0
		t.TimeToLive.Enabled = true
		if err := s.store.UpdateTimeToLive(r.Context(), tx, req.TableName, t.TimeToLive); err != nil {
			return nil, err
		}
	}
	if t.TimeToLive.Status == model.TTLStatusDisabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
		t.TimeToLive.Status = model.TTLStatusDisabled
		t.TimeToLive.StatusAt = 0
		t.TimeToLive.Enabled = false
		if err := s.store.UpdateTimeToLive(r.Context(), tx, req.TableName, t.TimeToLive); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	status := t.TimeToLive.Status
	if strings.TrimSpace(status) == "" {
		if t.TimeToLive.Enabled {
			status = model.TTLStatusEnabled
		} else {
			status = model.TTLStatusDisabled
		}
	}
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": status,
			"AttributeName":    t.TimeToLive.AttrName,
		},
	}, nil
}

func addConsumedCapacity(resp map[string]any, capacity string, tableName string, readUnits, writeUnits float64) {
	if capacity == "" || capacity == "NONE" {
		return
	}
	if resp["ConsumedCapacity"] == nil {
		resp["ConsumedCapacity"] = []map[string]any{}
	}
	list := resp["ConsumedCapacity"].([]map[string]any)
	entry := map[string]any{
		"TableName": tableName,
	}
	if readUnits > 0 {
		entry["ReadCapacityUnits"] = readUnits
		entry["CapacityUnits"] = readUnits
	}
	if writeUnits > 0 {
		entry["WriteCapacityUnits"] = writeUnits
		entry["CapacityUnits"] = writeUnits
	}
	if readUnits > 0 && writeUnits > 0 {
		entry["CapacityUnits"] = readUnits + writeUnits
	}
	resp["ConsumedCapacity"] = append(list, entry)
}

func setSingleConsumedCapacity(resp map[string]any, capacity string, tableName string, readUnits, writeUnits float64) {
	if capacity == "" || capacity == "NONE" {
		return
	}
	entry := map[string]any{"TableName": tableName}
	if readUnits > 0 {
		entry["ReadCapacityUnits"] = readUnits
		entry["CapacityUnits"] = readUnits
	}
	if writeUnits > 0 {
		entry["WriteCapacityUnits"] = writeUnits
		entry["CapacityUnits"] = writeUnits
	}
	if readUnits > 0 && writeUnits > 0 {
		entry["CapacityUnits"] = readUnits + writeUnits
	}
	resp["ConsumedCapacity"] = entry
}

func setSingleQueryConsumedCapacity(resp map[string]any, mode string, tableName, indexName, indexType string, readUnits float64) {
	if mode == "" || mode == "NONE" {
		return
	}
	entry := map[string]any{
		"TableName":         tableName,
		"ReadCapacityUnits": readUnits,
		"CapacityUnits":     readUnits,
	}
	if mode == "INDEXES" && strings.TrimSpace(indexName) != "" {
		if indexType == "GSI" {
			entry["GlobalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		} else if indexType == "LSI" {
			entry["LocalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		}
	}
	resp["ConsumedCapacity"] = entry
}

func addQueryConsumedCapacity(resp map[string]any, mode string, tableName, indexName, indexType string, readUnits float64) {
	if mode == "" || mode == "NONE" {
		return
	}
	if resp["ConsumedCapacity"] == nil {
		resp["ConsumedCapacity"] = []map[string]any{}
	}
	entry := map[string]any{
		"TableName":         tableName,
		"ReadCapacityUnits": readUnits,
		"CapacityUnits":     readUnits,
	}
	if mode == "INDEXES" && strings.TrimSpace(indexName) != "" {
		if indexType == "GSI" {
			entry["GlobalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		} else if indexType == "LSI" {
			entry["LocalSecondaryIndexes"] = map[string]any{
				indexName: map[string]any{
					"ReadCapacityUnits": readUnits,
					"CapacityUnits":     readUnits,
				},
			}
		}
	}
	list := resp["ConsumedCapacity"].([]map[string]any)
	resp["ConsumedCapacity"] = append(list, entry)
}
