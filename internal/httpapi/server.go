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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
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
	return map[string]any{}, nil
}

type getItemRequest struct {
	TableName string         `json:"TableName"`
	Key       map[string]any `json:"Key"`
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
	return map[string]any{"Item": item}, nil
}

type deleteItemRequest struct {
	TableName                 string            `json:"TableName"`
	Key                       map[string]any    `json:"Key"`
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
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
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			current = map[string]any{t.HashKey: req.Key[t.HashKey]}
			if t.RangeKey != "" {
				current[t.RangeKey] = req.Key[t.RangeKey]
			}
		} else {
			return nil, err
		}
	}

	ok, err := expr.Evaluate(req.ConditionExpression, current, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if !ok {
		return nil, awserr.ConditionalCheckFailed("The conditional request failed")
	}

	sets, removes, err := parseUpdateExpression(req.UpdateExpression, req.ExpressionAttributeNames, req.ExpressionAttributeValues)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	for k, v := range sets {
		current[k] = v
	}
	for _, k := range removes {
		delete(current, k)
	}
	if err := s.store.PutItem(r.Context(), t.Name, pk, sk, current); err != nil {
		return nil, err
	}

	if req.ReturnValues == "ALL_NEW" {
		return map[string]any{"Attributes": current}, nil
	}
	return map[string]any{}, nil
}

type queryRequest struct {
	TableName                 string            `json:"TableName"`
	KeyConditionExpression    string            `json:"KeyConditionExpression"`
	ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
	ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
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
	pkAttr := resolveName(pkToken.attr, req.ExpressionAttributeNames)
	if pkAttr != t.HashKey {
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

	startSK := ""
	if len(req.ExclusiveStartKey) > 0 {
		_, startSK, err = model.ExtractKey(t, req.ExclusiveStartKey)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
	}

	var items []map[string]any
	if skToken == nil {
		if t.RangeKey == "" {
			items, err = s.store.QueryByPKSK(r.Context(), t.Name, pk, model.NoSortKey)
		} else {
			items, err = s.store.QueryByPK(r.Context(), t.Name, pk, startSK, req.Limit)
		}
	} else {
		if resolveName(skToken.attr, req.ExpressionAttributeNames) != t.RangeKey {
			return nil, awserr.Validation("sort key condition must target RANGE key")
		}
		sv, ok := req.ExpressionAttributeValues[skToken.value]
		if !ok {
			return nil, awserr.Validation("missing sort key expression value")
		}
		sk, err := model.SerializeKeyValue(sv)
		if err != nil {
			return nil, awserr.Validation(err.Error())
		}
		items, err = s.store.QueryByPKSK(r.Context(), t.Name, pk, sk)
	}
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"Items": items, "Count": len(items), "ScannedCount": len(items)}
	if t.RangeKey != "" && req.Limit > 0 && len(items) == req.Limit {
		last := items[len(items)-1]
		resp["LastEvaluatedKey"] = map[string]any{t.HashKey: last[t.HashKey], t.RangeKey: last[t.RangeKey]}
	}
	return resp, nil
}

type scanRequest struct {
	TableName         string         `json:"TableName"`
	Limit             int            `json:"Limit"`
	ExclusiveStartKey map[string]any `json:"ExclusiveStartKey"`
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
	items, err := s.store.Scan(r.Context(), t.Name, startPK, startSK, req.Limit)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"Items": items, "Count": len(items), "ScannedCount": len(items)}
	if req.Limit > 0 && len(items) == req.Limit {
		last := items[len(items)-1]
		lek := map[string]any{t.HashKey: last[t.HashKey]}
		if t.RangeKey != "" {
			lek[t.RangeKey] = last[t.RangeKey]
		}
		resp["LastEvaluatedKey"] = lek
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

		for _, op := range ops {
			if len(op.PutRequest.Item) > 0 {
				pk, sk, err := model.ExtractItemKeys(t, op.PutRequest.Item)
				if err != nil {
					return nil, awserr.Validation(err.Error())
				}
				if err := s.store.PutItem(r.Context(), tableName, pk, sk, op.PutRequest.Item); err != nil {
					return nil, err
				}
			}
			if len(op.DeleteRequest.Key) > 0 {
				pk, sk, err := model.ExtractKey(t, op.DeleteRequest.Key)
				if err != nil {
					return nil, awserr.Validation(err.Error())
				}
				if err := s.store.DeleteItem(r.Context(), tableName, pk, sk); err != nil {
					return nil, err
				}
			}
		}
	}

	return map[string]any{"UnprocessedItems": map[string]any{}}, nil
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

type keyExprToken struct {
	attr  string
	value string
}

func parseKeyCondition(s string) (pk keyExprToken, sk *keyExprToken, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return pk, nil, fmt.Errorf("KeyConditionExpression is required")
	}
	parts := strings.Split(s, "AND")
	pk, err = parseSingleEq(parts[0])
	if err != nil {
		return pk, nil, err
	}
	if len(parts) > 1 {
		tok, err := parseSingleEq(parts[1])
		if err != nil {
			return pk, nil, err
		}
		sk = &tok
	}
	if len(parts) > 2 {
		return pk, nil, fmt.Errorf("only one optional sort key condition is supported")
	}
	return pk, sk, nil
}

func parseSingleEq(s string) (keyExprToken, error) {
	parts := strings.Split(s, "=")
	if len(parts) != 2 {
		return keyExprToken{}, fmt.Errorf("only '=' key conditions are supported")
	}
	a := strings.TrimSpace(parts[0])
	v := strings.TrimSpace(parts[1])
	if a == "" || v == "" {
		return keyExprToken{}, fmt.Errorf("invalid key condition segment")
	}
	return keyExprToken{attr: a, value: v}, nil
}

func parseUpdateExpression(raw string, names map[string]string, values map[string]any) (map[string]any, []string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, fmt.Errorf("UpdateExpression is required")
	}

	upper := strings.ToUpper(raw)
	setIdx := strings.Index(upper, "SET ")
	removeIdx := strings.Index(upper, "REMOVE ")

	sets := map[string]any{}
	removes := []string{}

	if setIdx >= 0 {
		setStart := setIdx + len("SET ")
		setEnd := len(raw)
		if removeIdx > setIdx {
			setEnd = removeIdx
		}
		for _, assign := range strings.Split(raw[setStart:setEnd], ",") {
			parts := strings.Split(assign, "=")
			if len(parts) != 2 {
				return nil, nil, fmt.Errorf("invalid SET clause")
			}
			left := resolveName(strings.TrimSpace(parts[0]), names)
			right := strings.TrimSpace(parts[1])
			v, ok := values[right]
			if !ok {
				return nil, nil, fmt.Errorf("missing expression attribute value %q", right)
			}
			sets[left] = v
		}
	}

	if removeIdx >= 0 {
		removeStart := removeIdx + len("REMOVE ")
		for _, attr := range strings.Split(raw[removeStart:], ",") {
			resolved := resolveName(strings.TrimSpace(attr), names)
			if resolved != "" {
				removes = append(removes, resolved)
			}
		}
	}

	if len(sets) == 0 && len(removes) == 0 {
		return nil, nil, fmt.Errorf("only SET/REMOVE update expressions are supported")
	}
	return sets, removes, nil
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
