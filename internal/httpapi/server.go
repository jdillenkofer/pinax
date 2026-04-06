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
			ProjectionType string `json:"ProjectionType"`
		} `json:"Projection"`
	} `json:"GlobalSecondaryIndexes"`
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
		t.GSIs = append(t.GSIs, model.GlobalSecondaryIndex{
			IndexName:      g.IndexName,
			HashKey:        gHash,
			HashType:       attrType[gHash],
			RangeKey:       gRange,
			RangeType:      attrType[gRange],
			ProjectionType: firstNonEmpty(g.Projection.ProjectionType, "ALL"),
		})
	}
	if err := s.store.CreateTable(r.Context(), t); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return nil, awserr.ResourceInUse("Table already exists: " + req.TableName)
		}
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
	t, err := s.store.GetTable(r.Context(), req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	count, err := s.store.CountItems(r.Context(), t.Name)
	if err != nil {
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
	names, err := s.store.ListTables(r.Context(), req.ExclusiveStartTableName, req.Limit)
	if err != nil {
		return nil, err
	}
	return map[string]any{"TableNames": names}, nil
}

func (s *Server) deleteTable(r *http.Request, body []byte) (map[string]any, error) {
	var req tableNameRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		return nil, err
	}
	count, err := s.store.CountItems(r.Context(), req.TableName)
	if err != nil {
		return nil, err
	}
	if err := s.store.DeleteTable(r.Context(), req.TableName); err != nil {
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
}

func (s *Server) putItem(r *http.Request, body []byte) (map[string]any, error) {
	var req putItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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

	current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
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

	if err := s.store.PutItem(r.Context(), t.Name, pk, sk, req.Item); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return map[string]any{}, nil
	}
	if req.ReturnValues != "ALL_OLD" {
		return nil, awserr.Validation("PutItem ReturnValues must be NONE or ALL_OLD")
	}
	if existed {
		return map[string]any{"Attributes": current}, nil
	}
	return map[string]any{}, nil
}

type getItemRequest struct {
	TableName                string            `json:"TableName"`
	Key                      map[string]any    `json:"Key"`
	ProjectionExpression     string            `json:"ProjectionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames"`
}

func (s *Server) getItem(r *http.Request, body []byte) (map[string]any, error) {
	var req getItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	item, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	projected, err := applyProjection(item, req.ProjectionExpression, req.ExpressionAttributeNames)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	return map[string]any{"Item": projected}, nil
}

type deleteItemRequest struct {
	TableName                 string            `json:"TableName"`
	Key                       map[string]any    `json:"Key"`
	ReturnValues              string            `json:"ReturnValues"`
	ConditionExpression       string            `json:"ConditionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
}

func (s *Server) deleteItem(r *http.Request, body []byte) (map[string]any, error) {
	var req deleteItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
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
	if err := s.store.DeleteItem(r.Context(), t.Name, pk, sk); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return map[string]any{}, nil
	}
	if req.ReturnValues != "ALL_OLD" {
		return nil, awserr.Validation("DeleteItem ReturnValues must be NONE or ALL_OLD")
	}
	if existed {
		return map[string]any{"Attributes": current}, nil
	}
	return map[string]any{}, nil
}

type updateItemRequest struct {
	TableName                 string            `json:"TableName"`
	Key                       map[string]any    `json:"Key"`
	UpdateExpression          string            `json:"UpdateExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
	ConditionExpression       string            `json:"ConditionExpression"`
	ReturnValues              string            `json:"ReturnValues"`
}

func (s *Server) updateItem(r *http.Request, body []byte) (map[string]any, error) {
	var req updateItemRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
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
	if err := s.store.PutItem(r.Context(), t.Name, pk, sk, updated); err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.ReturnValues) == "" || req.ReturnValues == "NONE" {
		return map[string]any{}, nil
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
		return map[string]any{}, nil
	}
	return map[string]any{"Attributes": attributes}, nil
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
}

func (s *Server) query(r *http.Request, body []byte) (map[string]any, error) {
	var req queryRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	if strings.TrimSpace(req.IndexName) != "" {
		gsi, ok := t.GetGSI(req.IndexName)
		if !ok {
			return nil, awserr.Validation("unknown index " + req.IndexName)
		}
		targetHashKey = gsi.HashKey
		targetRangeKey = gsi.RangeKey
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
	if len(req.ExclusiveStartKey) > 0 {
		_, startSK, err = model.ExtractKey(t, req.ExclusiveStartKey)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
	} else if !scanForward {
		startSK = "~"
	}

	var items []map[string]any
	if skToken == nil {
		if targetRangeKey == "" {
			items, err = s.store.QueryByPKSK(r.Context(), t.Name, pk, model.NoSortKey)
		} else {
			items, err = s.store.QueryByPK(r.Context(), t.Name, pk, startSK, scanForward, 0)
		}
	} else {
		skAttr, err := resolveNameStrict(skToken.attr, req.ExpressionAttributeNames)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if skAttr != targetRangeKey {
			return nil, awserr.Validation("sort key condition must target RANGE key")
		}
		items, err = s.store.QueryByPK(r.Context(), t.Name, pk, startSK, scanForward, 0)
	}
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(req.IndexName) != "" {
		candidate, err := s.store.Scan(r.Context(), t.Name, "", "", 0)
		if err != nil {
			return nil, err
		}
		items = candidate
	}

	limit := parseLimit(req.Limit)
	count := 0
	scanned := 0
	filtered := make([]map[string]any, 0)
	var lastScanned map[string]any

	for _, item := range items {
		if strings.TrimSpace(req.IndexName) != "" {
			raw, ok := item[targetHashKey]
			if !ok {
				continue
			}
			itemPK, err := model.SerializeKeyValue(raw)
			if err != nil || itemPK != pk {
				continue
			}
		}

		if skToken != nil {
			ok, err := sortConditionMatches(item, skToken, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				continue
			}
		}

		scanned++
		lastScanned = keyFromItem(t, item)

		matches, err := applyFilter(item, req.FilterExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		if matches {
			projected, err := applyProjection(item, req.ProjectionExpression, req.ExpressionAttributeNames)
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
}

func (s *Server) scan(r *http.Request, body []byte) (map[string]any, error) {
	var req scanRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	items, err := s.store.Scan(r.Context(), t.Name, startPK, startSK, 0)
	if err != nil {
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
	if limit > 0 && scanned == limit && lastScanned != nil {
		resp["LastEvaluatedKey"] = lastScanned
	}
	return resp, nil
}

type batchGetItemRequest struct {
	RequestItems map[string]struct {
		Keys []map[string]any `json:"Keys"`
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

	responses := map[string]any{}
	for tableName, itemReq := range req.RequestItems {
		t, err := s.store.GetTable(r.Context(), tableName)
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
			item, err := s.store.GetItem(r.Context(), tableName, pk, sk)
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
	return map[string]any{"Responses": responses, "UnprocessedKeys": map[string]any{}}, nil
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

	tableNames := make([]string, 0, len(req.RequestItems))
	for tableName := range req.RequestItems {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	for _, tableName := range tableNames {
		ops := req.RequestItems[tableName]
		t, err := s.store.GetTable(r.Context(), tableName)
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
				pk, sk, err := model.ExtractItemKeys(t, op.PutRequest.Item)
				if err != nil {
					return nil, awserr.Validation(err.Error())
				}
				k := tableName + "|" + pk + "|" + sk
				if _, exists := seenKeys[k]; exists {
					return nil, awserr.Validation("BatchWriteItem contains duplicate keys")
				}
				seenKeys[k] = struct{}{}
				if err := s.store.PutItem(r.Context(), tableName, pk, sk, op.PutRequest.Item); err != nil {
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
				if err := s.store.DeleteItem(r.Context(), tableName, pk, sk); err != nil {
					return nil, err
				}
			}
		}
	}

	return map[string]any{"UnprocessedItems": map[string]any{}}, nil
}

type transactGetRequest struct {
	TransactItems []struct {
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

	responses := make([]map[string]any, 0, len(req.TransactItems))
	for _, txItem := range req.TransactItems {
		g := txItem.Get
		t, err := s.store.GetTable(r.Context(), g.TableName)
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
		item, err := s.store.GetItem(r.Context(), g.TableName, pk, sk)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				responses = append(responses, map[string]any{})
				continue
			}
			return nil, err
		}
		responses = append(responses, map[string]any{"Item": item})
	}
	return map[string]any{"Responses": responses}, nil
}

type transactWriteRequest struct {
	TransactItems []struct {
		Put struct {
			TableName                 string            `json:"TableName"`
			Item                      map[string]any    `json:"Item"`
			ConditionExpression       string            `json:"ConditionExpression"`
			ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Put"`
		Delete struct {
			TableName                 string            `json:"TableName"`
			Key                       map[string]any    `json:"Key"`
			ConditionExpression       string            `json:"ConditionExpression"`
			ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Delete"`
		Update struct {
			TableName                 string            `json:"TableName"`
			Key                       map[string]any    `json:"Key"`
			UpdateExpression          string            `json:"UpdateExpression"`
			ConditionExpression       string            `json:"ConditionExpression"`
			ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
		} `json:"Update"`
		ConditionCheck struct {
			TableName                 string            `json:"TableName"`
			Key                       map[string]any    `json:"Key"`
			ConditionExpression       string            `json:"ConditionExpression"`
			ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
			ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
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

	type stagedMutation struct {
		table string
		pk    string
		sk    string
		item  map[string]any
		del   bool
	}
	staged := make([]stagedMutation, 0, len(req.TransactItems))
	seenTargets := map[string]struct{}{}

	for _, txItem := range req.TransactItems {
		if len(txItem.Put.Item) > 0 {
			t, err := s.store.GetTable(r.Context(), txItem.Put.TableName)
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
			current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.Put.ConditionExpression, current, txItem.Put.ExpressionAttributeNames, txItem.Put.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				return nil, awserr.ConditionalCheckFailed("The conditional request failed")
			}
			staged = append(staged, stagedMutation{table: t.Name, pk: pk, sk: sk, item: txItem.Put.Item})
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
			}
			seenTargets[target] = struct{}{}
			continue
		}

		if len(txItem.Delete.Key) > 0 {
			t, err := s.store.GetTable(r.Context(), txItem.Delete.TableName)
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
			current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.Delete.ConditionExpression, current, txItem.Delete.ExpressionAttributeNames, txItem.Delete.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				return nil, awserr.ConditionalCheckFailed("The conditional request failed")
			}
			staged = append(staged, stagedMutation{table: t.Name, pk: pk, sk: sk, del: true})
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
			}
			seenTargets[target] = struct{}{}
			continue
		}

		if len(txItem.Update.Key) > 0 {
			t, err := s.store.GetTable(r.Context(), txItem.Update.TableName)
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
			current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
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
				return nil, awserr.ConditionalCheckFailed("The conditional request failed")
			}
			plan, err := parseUpdateExpression(txItem.Update.UpdateExpression, txItem.Update.ExpressionAttributeNames, txItem.Update.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			updated, _, err := applyUpdatePlan(current, plan)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			staged = append(staged, stagedMutation{table: t.Name, pk: pk, sk: sk, item: updated})
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
			}
			seenTargets[target] = struct{}{}
			continue
		}

		if len(txItem.ConditionCheck.Key) > 0 {
			t, err := s.store.GetTable(r.Context(), txItem.ConditionCheck.TableName)
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
			current, err := s.store.GetItem(r.Context(), t.Name, pk, sk)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
			if errors.Is(err, sql.ErrNoRows) {
				current = map[string]any{}
			}
			ok, err := expr.Evaluate(txItem.ConditionCheck.ConditionExpression, current, txItem.ConditionCheck.ExpressionAttributeNames, txItem.ConditionCheck.ExpressionAttributeValues)
			if err != nil {
				return nil, awserr.Validation(err.Error())
			}
			if !ok {
				return nil, awserr.ConditionalCheckFailed("The conditional request failed")
			}
			target := t.Name + "|" + pk + "|" + sk
			if _, exists := seenTargets[target]; exists {
				return nil, awserr.Validation("TransactWriteItems cannot target the same item more than once")
			}
			seenTargets[target] = struct{}{}
			continue
		}

		return nil, awserr.Validation("each transact item must contain one operation")
	}

	for _, m := range staged {
		if m.del {
			if err := s.store.DeleteItem(r.Context(), m.table, m.pk, m.sk); err != nil {
				return nil, err
			}
			continue
		}
		if err := s.store.PutItem(r.Context(), m.table, m.pk, m.sk, m.item); err != nil {
			return nil, err
		}
	}

	return map[string]any{}, nil
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

	t, err := s.store.GetTable(r.Context(), req.TableName)
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
	if err := s.store.UpdateTimeToLive(r.Context(), req.TableName, ttl); err != nil {
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

	t, err := s.store.GetTable(r.Context(), req.TableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
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
