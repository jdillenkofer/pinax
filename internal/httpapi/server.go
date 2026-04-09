package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
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
	TableName            string `json:"TableName"`
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

	t := model.Table{
		Name:      req.TableName,
		HashKey:   hashKey,
		HashType:  hashType,
		RangeKey:  rangeKey,
		RangeType: attrType[rangeKey],
		CreatedAt: time.Now().Unix(),
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
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

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"TableNames": names}, nil
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	count, err := s.store.CountItems(r.Context(), tx, req.TableName)
	if err != nil {
		return nil, err
	}
	if err := s.store.DeleteTable(r.Context(), tx, req.TableName); err != nil {
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	pk, sk, err := model.ExtractItemKeys(t, req.Item)
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

	if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, req.Item); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{}
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(req.Item)))

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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	pk, sk, err := model.ExtractKey(t, req.Key)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	item, err := s.store.GetItem(r.Context(), tx, t.Name, pk, sk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
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
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead), 0)
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
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
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current)))

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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
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
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, 0, model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated)))

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
	TableName            string `json:"TableName"`
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}

	if len(req.GlobalSecondaryIndexUpdates) > 0 {
		attrTypes := map[string]string{}
		for _, d := range req.AttributeDefinitions {
			if strings.TrimSpace(d.AttributeName) != "" {
				attrTypes[d.AttributeName] = d.AttributeType
			}
		}
		updatedGSIs, err := applyGSIUpdates(t, req.GlobalSecondaryIndexUpdates, attrTypes)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		t.GSIs = updatedGSIs
		if err := s.store.UpdateTableIndexes(r.Context(), tx, t.Name, t.GSIs, t.LSIs); err != nil {
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
}, attrTypes map[string]string) ([]model.GlobalSecondaryIndex, error) {
	gsis := append([]model.GlobalSecondaryIndex{}, table.GSIs...)
	for _, u := range updates {
		hasCreate := strings.TrimSpace(u.Create.IndexName) != ""
		hasDelete := strings.TrimSpace(u.Delete.IndexName) != ""
		if hasCreate == hasDelete {
			return nil, fmt.Errorf("each GlobalSecondaryIndexUpdates entry must include exactly one of Create or Delete")
		}
		if hasDelete {
			name := strings.TrimSpace(u.Delete.IndexName)
			found := false
			next := make([]model.GlobalSecondaryIndex, 0, len(gsis))
			for _, g := range gsis {
				if g.IndexName == name {
					found = true
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	pkToken, skToken, err := parseKeyCondition(req.KeyConditionExpression)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	targetHashKey := t.HashKey
	targetRangeKey := t.RangeKey
	var queryGSI *model.GlobalSecondaryIndex
	var queryLSI *model.LocalSecondaryIndex
	if strings.TrimSpace(req.IndexName) != "" {
		gsi, ok := t.GetGSI(req.IndexName)
		if ok {
			if req.ConsistentRead {
				return nil, awserr.Validation("ConsistentRead is not supported on global secondary indexes")
			}
			queryGSI = &gsi
			targetHashKey = gsi.HashKey
			targetRangeKey = gsi.RangeKey
		} else {
			lsi, ok := t.GetLSI(req.IndexName)
			if !ok {
				return nil, awserr.Validation("unknown index " + req.IndexName)
			}
			queryLSI = &lsi
			targetHashKey = t.HashKey
			targetRangeKey = lsi.RangeKey
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
	pk, err := model.SerializeKeyValue(pkValue)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	scanForward := true
	if req.ScanIndexForward != nil {
		scanForward = *req.ScanIndexForward
	}

	startSK := ""
	if queryGSI == nil && queryLSI == nil {
		if len(req.ExclusiveStartKey) > 0 {
			_, startSK, err = model.ExtractKey(t, req.ExclusiveStartKey)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
		} else if !scanForward {
			startSK = "~"
		}
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
			items, err = s.store.QueryByPK(r.Context(), tx, t.Name, pk, startSK, scanForward, 0)
		}
	} else {
		skAttr, err := resolveNameStrict(skToken.attr, req.ExpressionAttributeNames)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if skAttr != targetRangeKey {
			return nil, awserr.Validation("sort key condition must target RANGE key")
		}
		items, err = s.store.QueryByPK(r.Context(), tx, t.Name, pk, startSK, scanForward, 0)
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
			ok, err := sortConditionMatches(queryItem, skToken, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				continue
			}
		}

		scanned++
		lastScanned = keyFromItem(t, item)

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
	var totalRead float64
	for _, item := range filtered {
		totalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead)
	}
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, totalRead, 0)
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
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
	for _, item := range items {
		scanned++
		lastScanned = keyFromItem(t, item)

		matches, err := applyFilter(item, req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if !matches {
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
	var totalRead float64
	for _, item := range filtered {
		totalRead += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), req.ConsistentRead)
	}
	addConsumedCapacity(resp, req.ReturnConsumedCapacity, t.Name, totalRead, 0)
	if limit > 0 && scanned == limit && lastScanned != nil {
		resp["LastEvaluatedKey"] = lastScanned
	}
	return resp, nil
}

type batchGetItemRequest struct {
	RequestItems map[string]struct {
		Keys                   []map[string]any `json:"Keys"`
		ConsistentRead         bool             `json:"ConsistentRead"`
		ReturnConsumedCapacity string           `json:"ReturnConsumedCapacity"`
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
	if totalKeys > 100 {
		return nil, awserr.Validation("BatchGetItem supports at most 100 keys")
	}

	tx, err := s.store.DB().BeginTx(r.Context(), nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	responses := map[string]any{}
	for tableName, itemReq := range req.RequestItems {
		t, err := s.store.GetTable(r.Context(), tx, tableName)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
			}
			return nil, err
		}
		items := make([]map[string]any, 0, len(itemReq.Keys))
		for _, key := range itemReq.Keys {
			pk, sk, err := model.ExtractKey(t, key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			item, err := s.store.GetItem(r.Context(), tx, tableName, pk, sk)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				return nil, err
			}
			items = append(items, item)
		}
		responses[tableName] = items
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	resp := map[string]any{"Responses": responses, "UnprocessedKeys": map[string]any{}}
	for tableName, itemReq := range req.RequestItems {
		if items, ok := responses[tableName]; ok {
			for _, item := range items.([]map[string]any) {
				addConsumedCapacity(resp, itemReq.ReturnConsumedCapacity, tableName, model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), itemReq.ConsistentRead), 0)
			}
		}
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

	for _, tableName := range tableNames {
		ops := req.RequestItems[tableName]
		t, err := s.store.GetTable(r.Context(), tx, tableName)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
			}
			return nil, err
		}

		seenKeys := map[string]struct{}{}
		for _, op := range ops {
			if len(op.PutRequest.Item) > 0 && len(op.DeleteRequest.Key) > 0 {
				return nil, awserr.Validation("write request cannot contain both PutRequest and DeleteRequest")
			}
			if len(op.PutRequest.Item) > 0 {
				if model.ItemTooLarge(op.PutRequest.Item) {
					return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
				}
				pk, sk, err := model.ExtractItemKeys(t, op.PutRequest.Item)
				if err != nil {
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
				if err := s.store.DeleteItem(r.Context(), tx, tableName, pk, sk); err != nil {
					return nil, err
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"UnprocessedItems": map[string]any{}}, nil
}

type transactGetRequest struct {
	TransactItems []struct {
		Get struct {
			TableName              string         `json:"TableName"`
			Key                    map[string]any `json:"Key"`
			ReturnConsumedCapacity string         `json:"ReturnConsumedCapacity"`
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
	for _, txItem := range req.TransactItems {
		g := txItem.Get
		t, err := s.store.GetTable(r.Context(), tx, g.TableName)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
			}
			return nil, err
		}
		pk, sk, err := model.ExtractKey(t, g.Key)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		item, err := s.store.GetItem(r.Context(), tx, g.TableName, pk, sk)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				responses = append(responses, map[string]any{})
				continue
			}
			return nil, err
		}
		responses = append(responses, map[string]any{"Item": item})
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return map[string]any{"Responses": responses}, nil
}

type transactWriteRequest struct {
	TransactItems []struct {
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

	for i, txItem := range req.TransactItems {
		if len(txItem.Put.Item) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Put.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.store.GetTable(r.Context(), tx, txItem.Put.TableName)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
				}
				return nil, err
			}
			pk, sk, err := model.ExtractItemKeys(t, txItem.Put.Item)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
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
			if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, txItem.Put.Item); err != nil {
				return nil, err
			}
			continue
		}

		if len(txItem.Delete.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Delete.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.store.GetTable(r.Context(), tx, txItem.Delete.TableName)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
				}
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.Delete.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
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
			continue
		}

		if len(txItem.Update.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.Update.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.store.GetTable(r.Context(), tx, txItem.Update.TableName)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
				}
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.Update.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
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
			if model.ItemTooLarge(updated) {
				return nil, awserr.Validation("Item size has exceeded the maximum allowed size (400KB)")
			}
			if err := s.store.PutItem(r.Context(), tx, t.Name, pk, sk, updated); err != nil {
				return nil, err
			}
			continue
		}

		if len(txItem.ConditionCheck.Key) > 0 {
			if err := validateReturnValuesOnConditionCheckFailure(txItem.ConditionCheck.ReturnValuesOnConditionCheckFailure); err != nil {
				return nil, awserr.Validation(err.Error())
			}
			t, err := s.store.GetTable(r.Context(), tx, txItem.ConditionCheck.TableName)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
				}
				return nil, err
			}
			pk, sk, err := model.ExtractKey(t, txItem.ConditionCheck.Key)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
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

	return map[string]any{}, nil
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

func decode(body []byte, out any) error {
	if len(strings.TrimSpace(string(body))) == 0 {
		body = []byte("{}")
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
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

func orderItemsForGSI(items []map[string]any, t model.Table, g model.GlobalSecondaryIndex, exclusiveStartKey map[string]any, scanForward bool) ([]map[string]any, error) {
	type entry struct {
		item map[string]any
		idx  string
		pk   string
		sk   string
	}

	entries := make([]entry, 0, len(items))
	for _, item := range items {
		idx := ""
		if g.RangeKey != "" {
			raw, ok := item[g.RangeKey]
			if !ok {
				continue
			}
			v, err := model.SerializeKeyValue(raw)
			if err != nil {
				continue
			}
			idx = v
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
		if entries[i].idx == entries[j].idx {
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
			return entries[i].idx < entries[j].idx
		}
		return entries[i].idx > entries[j].idx
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
		item  map[string]any
		lsiSK string
		tblSK string
	}

	entries := make([]entry, 0, len(items))
	for _, item := range items {
		raw, ok := item[l.RangeKey]
		if !ok {
			continue
		}
		lsiSK, err := model.SerializeKeyValue(raw)
		if err != nil {
			continue
		}
		tblSK := model.NoSortKey
		if t.RangeKey != "" {
			rawTableSK, ok := item[t.RangeKey]
			if !ok {
				continue
			}
			tblSK, err = model.SerializeKeyValue(rawTableSK)
			if err != nil {
				continue
			}
		}
		entries = append(entries, entry{item: item, lsiSK: lsiSK, tblSK: tblSK})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].lsiSK == entries[j].lsiSK {
			if scanForward {
				return entries[i].tblSK < entries[j].tblSK
			}
			return entries[i].tblSK > entries[j].tblSK
		}
		if scanForward {
			return entries[i].lsiSK < entries[j].lsiSK
		}
		return entries[i].lsiSK > entries[j].lsiSK
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}

	ttl := model.TimeToLive{
		Enabled:  req.TimeToLiveSpecification.Enabled,
		AttrName: req.TimeToLiveSpecification.AttributeName,
	}
	if err := s.store.UpdateTimeToLive(r.Context(), tx, req.TableName, ttl); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	t.TimeToLive = ttl
	status := "DISABLED"
	if ttl.Enabled {
		status = "ENABLED"
	}
	return map[string]any{
		"TimeToLiveDescription": map[string]any{
			"TimeToLiveStatus": status,
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

	t, err := s.store.GetTable(r.Context(), tx, req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	status := "DISABLED"
	if t.TimeToLive.Enabled {
		status = "ENABLED"
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
