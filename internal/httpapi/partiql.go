package httpapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/model"
)

var (
	selectStatementPattern = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+([^\s]+)\s+WHERE\s+(.+?)\s*$`)
	insertStatementPattern = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+([^\s]+)\s+VALUE\s+(.+?)\s*$`)
	updateStatementPattern = regexp.MustCompile(`(?is)^\s*UPDATE\s+([^\s]+)\s+SET\s+(.+?)\s+WHERE\s+(.+?)\s*$`)
	deleteStatementPattern = regexp.MustCompile(`(?is)^\s*DELETE\s+FROM\s+([^\s]+)\s+WHERE\s+(.+?)\s*$`)
	keyConditionPattern    = regexp.MustCompile(`(?is)^\s*([A-Za-z0-9_]+)\s*=\s*\?\s*$`)
	comparisonPattern      = regexp.MustCompile(`(?is)^\s*([A-Za-z0-9_]+)\s*(=|<>|<=|<|>=|>)\s*\?\s*$`)
	betweenPattern         = regexp.MustCompile(`(?is)^\s*([A-Za-z0-9_]+)\s+BETWEEN\s+\?\s+AND\s+\?\s*$`)
	inPattern              = regexp.MustCompile(`(?is)^\s*([A-Za-z0-9_]+)\s+IN\s*\((.+)\)\s*$`)
	beginsWithPattern      = regexp.MustCompile(`(?is)^\s*begins_with\s*\(\s*([A-Za-z0-9_]+)\s*,\s*\?\s*\)\s*$`)
	setClausePattern       = regexp.MustCompile(`(?is)^\s*([A-Za-z0-9_]+)\s*=\s*\?\s*$`)
	valuePairPattern       = regexp.MustCompile(`(?is)^\s*['\"]?([A-Za-z0-9_]+)['\"]?\s*:\s*\?\s*$`)
)

type executeStatementRequest struct {
	Statement                           string `json:"Statement"`
	ConsistentRead                      bool   `json:"ConsistentRead"`
	Limit                               int    `json:"Limit"`
	NextToken                           string `json:"NextToken"`
	Parameters                          []any  `json:"Parameters"`
	ReturnConsumedCapacity              string `json:"ReturnConsumedCapacity"`
	ReturnValuesOnConditionCheckFailure string `json:"ReturnValuesOnConditionCheckFailure"`
}

type batchExecuteStatementRequest struct {
	Statements []struct {
		Statement                           string `json:"Statement"`
		ConsistentRead                      bool   `json:"ConsistentRead"`
		Parameters                          []any  `json:"Parameters"`
		ReturnValuesOnConditionCheckFailure string `json:"ReturnValuesOnConditionCheckFailure"`
	} `json:"Statements"`
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
}

type executeTransactionRequest struct {
	TransactStatements []struct {
		Statement                           string `json:"Statement"`
		Parameters                          []any  `json:"Parameters"`
		ReturnValuesOnConditionCheckFailure string `json:"ReturnValuesOnConditionCheckFailure"`
	} `json:"TransactStatements"`
	ClientRequestToken     string `json:"ClientRequestToken"`
	ReturnConsumedCapacity string `json:"ReturnConsumedCapacity"`
}

type partiqlResult struct {
	Items            []map[string]any
	LastEvaluatedKey map[string]any
	NextToken        string
	ReadUnits        float64
	WriteUnits       float64
	TableName        string
}

type partiqlPredicate struct {
	Attr string
	Op   string
	A    any
	B    any
	List []any
}

type partiqlNextToken struct {
	Key           map[string]any `json:"key"`
	StatementHash string         `json:"statementHash"`
	TableName     string         `json:"tableName"`
}

func (s *Server) executeStatement(r *http.Request, body []byte) (map[string]any, error) {
	var req executeStatementRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.Statement) == "" {
		return nil, awserr.Validation("Statement is required")
	}
	operation := partiQLOperation(req.Statement)
	if operation == "UNKNOWN" {
		return nil, awserr.Validation("Unsupported PartiQL statement")
	}
	if req.Limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if req.Limit > 1000 {
		return nil, awserr.Validation("Limit must be less than or equal to 1000")
	}
	if strings.TrimSpace(req.NextToken) != "" && operation != "SELECT" {
		return nil, awserr.Validation("NextToken is only supported for SELECT statements")
	}
	if req.ConsistentRead && operation != "SELECT" {
		return nil, awserr.Validation("ConsistentRead is only supported for SELECT statements")
	}
	if err := validateReturnValuesOnConditionCheckFailure(req.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ReturnValuesOnConditionCheckFailure) != "" && operation == "SELECT" {
		return nil, awserr.Validation("ReturnValuesOnConditionCheckFailure is not supported for SELECT statements")
	}

	var res partiqlResult
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		res, err = s.runPartiQLStatement(txCtx, repos, req.Statement, req.Parameters, req.ConsistentRead, req.Limit, req.NextToken, req.ReturnValuesOnConditionCheckFailure)
		return err
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"Items": res.Items}
	if len(res.LastEvaluatedKey) > 0 {
		resp["LastEvaluatedKey"] = res.LastEvaluatedKey
	}
	if strings.TrimSpace(res.NextToken) != "" {
		resp["NextToken"] = res.NextToken
	}
	setSingleConsumedCapacity(resp, req.ReturnConsumedCapacity, res.TableName, res.ReadUnits, res.WriteUnits)
	return resp, nil
}

func (s *Server) batchExecuteStatement(r *http.Request, body []byte) (map[string]any, error) {
	var req batchExecuteStatementRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if len(req.Statements) == 0 {
		return nil, awserr.Validation("Statements is required")
	}
	if len(req.Statements) > 25 {
		return nil, awserr.Validation("BatchExecuteStatement supports at most 25 statements")
	}

	responses := make([]map[string]any, 0, len(req.Statements))
	readByTable := map[string]float64{}
	writeByTable := map[string]float64{}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for _, stmt := range req.Statements {
			if strings.TrimSpace(stmt.Statement) == "" {
				responses = append(responses, map[string]any{"Error": map[string]any{"Code": "ValidationError", "Message": "Statement is required"}})
				continue
			}
			if err := validateReturnValuesOnConditionCheckFailure(stmt.ReturnValuesOnConditionCheckFailure); err != nil {
				responses = append(responses, map[string]any{"Error": map[string]any{"Code": "ValidationError", "Message": err.Error()}, "TableName": parseTableNameFromStatement(stmt.Statement)})
				continue
			}
			op := partiQLOperation(stmt.Statement)
			if op == "UNKNOWN" {
				responses = append(responses, map[string]any{"Error": map[string]any{"Code": "ValidationError", "Message": "Unsupported PartiQL statement"}, "TableName": parseTableNameFromStatement(stmt.Statement)})
				continue
			}
			if stmt.ConsistentRead && op != "SELECT" {
				responses = append(responses, map[string]any{"Error": map[string]any{"Code": "ValidationError", "Message": "ConsistentRead is only supported for SELECT statements"}, "TableName": parseTableNameFromStatement(stmt.Statement)})
				continue
			}
			res, err := s.runPartiQLStatement(txCtx, repos, stmt.Statement, stmt.Parameters, stmt.ConsistentRead, 0, "", stmt.ReturnValuesOnConditionCheckFailure)
			if err != nil {
				responses = append(responses, batchStatementErrorResponse(stmt.Statement, err))
				continue
			}
			entry := map[string]any{}
			if len(res.Items) > 0 {
				entry["Item"] = res.Items[0]
			}
			responses = append(responses, entry)
			if res.TableName != "" {
				readByTable[res.TableName] += res.ReadUnits
				writeByTable[res.TableName] += res.WriteUnits
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	resp := map[string]any{"Responses": responses}
	for tableName, units := range readByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, units, writeByTable[tableName])
	}
	for tableName, units := range writeByTable {
		if _, ok := readByTable[tableName]; ok {
			continue
		}
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	return resp, nil
}

func (s *Server) executeTransaction(r *http.Request, body []byte) (map[string]any, error) {
	var req executeTransactionRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if len(req.TransactStatements) == 0 {
		return nil, awserr.Validation("TransactStatements is required")
	}
	if len(req.TransactStatements) > 100 {
		return nil, awserr.Validation("ExecuteTransaction supports at most 100 statements")
	}
	if strings.TrimSpace(req.ClientRequestToken) != "" {
		if len(strings.TrimSpace(req.ClientRequestToken)) > 36 {
			return nil, awserr.Validation("ClientRequestToken must be less than or equal to 36")
		}
		replay, mismatch, err := s.lookupExecuteTransactionIdempotency(r.Context(), req)
		if err != nil {
			return nil, err
		}
		if mismatch {
			return nil, awserr.IdempotentParameterMismatch("Idempotent parameter mismatch")
		}
		if replay != nil {
			return replay, nil
		}
	}

	responses := make([]map[string]any, 0, len(req.TransactStatements))
	readByTable := map[string]float64{}
	writeByTable := map[string]float64{}
	nowMillis := time.Now().UnixMilli()
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for idx, stmt := range req.TransactStatements {
			if err := validateReturnValuesOnConditionCheckFailure(stmt.ReturnValuesOnConditionCheckFailure); err != nil {
				reasons := transactionCancellationReasons(len(req.TransactStatements), idx, awserr.Validation(err.Error()))
				return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]", reasons)
			}
			op := partiQLOperation(stmt.Statement)
			if op == "UNKNOWN" {
				reasons := transactionCancellationReasons(len(req.TransactStatements), idx, awserr.Validation("Unsupported PartiQL statement"))
				return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [None]", reasons)
			}
			res, err := s.runPartiQLStatement(txCtx, repos, stmt.Statement, stmt.Parameters, false, 0, "", stmt.ReturnValuesOnConditionCheckFailure)
			if err != nil {
				reasons := transactionCancellationReasons(len(req.TransactStatements), idx, err)
				return awserr.TransactionCanceled("Transaction cancelled, please refer cancellation reasons for specific reasons [None]", reasons)
			}
			entry := map[string]any{}
			if len(res.Items) > 0 {
				entry["Item"] = res.Items[0]
			}
			responses = append(responses, entry)
			if res.TableName != "" {
				readByTable[res.TableName] += res.ReadUnits
				writeByTable[res.TableName] += res.WriteUnits
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	resp := map[string]any{"Responses": responses}
	for tableName, units := range readByTable {
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, units, writeByTable[tableName])
	}
	for tableName, units := range writeByTable {
		if _, ok := readByTable[tableName]; ok {
			continue
		}
		addConsumedCapacity(resp, req.ReturnConsumedCapacity, tableName, 0, units)
	}
	if strings.TrimSpace(req.ClientRequestToken) != "" {
		if err := s.storeExecuteTransactionIdempotency(r.Context(), req, resp, nowMillis); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (s *Server) runPartiQLStatement(ctx context.Context, repos uow.Repos, statement string, parameters []any, consistentRead bool, limit int, nextToken string, returnValuesOnConditionCheckFailure string) (partiqlResult, error) {
	statement = strings.TrimSpace(statement)
	if statement == "" {
		return partiqlResult{}, awserr.Validation("Statement is required")
	}

	if m := selectStatementPattern.FindStringSubmatch(statement); len(m) == 4 {
		return s.runPartiQLSelect(ctx, repos, strings.TrimSpace(m[1]), parseTableIdentifier(m[2]), strings.TrimSpace(m[3]), parameters, consistentRead, limit, nextToken)
	}
	if m := insertStatementPattern.FindStringSubmatch(statement); len(m) == 3 {
		return s.runPartiQLInsert(ctx, repos, parseTableIdentifier(m[1]), strings.TrimSpace(m[2]), parameters)
	}
	if m := updateStatementPattern.FindStringSubmatch(statement); len(m) == 4 {
		return s.runPartiQLUpdate(ctx, repos, parseTableIdentifier(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3]), parameters, returnValuesOnConditionCheckFailure)
	}
	if m := deleteStatementPattern.FindStringSubmatch(statement); len(m) == 3 {
		return s.runPartiQLDelete(ctx, repos, parseTableIdentifier(m[1]), strings.TrimSpace(m[2]), parameters, returnValuesOnConditionCheckFailure)
	}
	return partiqlResult{}, awserr.Validation("Unsupported PartiQL statement")
}

func (s *Server) runPartiQLSelect(ctx context.Context, repos uow.Repos, projection string, tableName string, where string, parameters []any, consistentRead bool, limit int, nextToken string) (partiqlResult, error) {
	t, err := s.getActiveTableFromRepo(ctx, repos.Tables(), tableName)
	if err != nil {
		return partiqlResult{}, err
	}
	preds, consumed, err := parsePredicates(where, parameters)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	if consumed != len(parameters) {
		return partiqlResult{}, awserr.Validation("Incorrect number of parameters for statement")
	}
	hashPred, ok := findPredicate(preds, t.HashKey)
	if !ok {
		return partiqlResult{}, awserr.Validation("PartiQL WHERE clause must include partition key equality")
	}
	if hashPred.Op != "=" {
		return partiqlResult{}, awserr.Validation("Partition key predicate must be equality")
	}
	hashValue := hashPred.A
	if err := model.ValidateKeyAttributeType(hashValue, t.HashType, t.HashKey); err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	hashKey, err := model.SerializeKeyValue(hashValue)
	if err != nil {
		return partiqlResult{}, awserr.Validation("Invalid partition key value")
	}

	projectionAttrs, err := parseProjection(projection)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}

	skPred, hasSKPred := findPredicate(preds, t.RangeKey)
	if t.RangeKey != "" && hasSKPred && skPred.Op == "=" {
		if strings.TrimSpace(nextToken) != "" {
			return partiqlResult{}, awserr.Validation("NextToken is not supported for exact key lookups")
		}
		if err := model.ValidateKeyAttributeType(skPred.A, t.RangeType, t.RangeKey); err != nil {
			return partiqlResult{}, awserr.Validation(err.Error())
		}
		rangeKey, err := model.SerializeKeyValue(skPred.A)
		if err != nil {
			return partiqlResult{}, awserr.Validation("Invalid sort key value")
		}
		item, err := repos.Items().GetItem(ctx, t.Name, hashKey, rangeKey)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return partiqlResult{Items: []map[string]any{}, TableName: t.Name}, nil
			}
			return partiqlResult{}, err
		}
		if !matchAllPredicates(item, preds) {
			return partiqlResult{Items: []map[string]any{}, TableName: t.Name}, nil
		}
		units := model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), consistentRead)
		if err := s.ensureReadCapacity(t, units); err != nil {
			return partiqlResult{}, err
		}
		return partiqlResult{Items: []map[string]any{projectItem(item, projectionAttrs)}, ReadUnits: units, TableName: t.Name}, nil
	}

	startSK := ""
	if strings.TrimSpace(nextToken) != "" {
		key, tokenTable, tokenHash, err := decodePartiQLNextToken(nextToken)
		if err != nil {
			return partiqlResult{}, awserr.Validation("Invalid NextToken")
		}
		if tokenTable != t.Name || tokenHash != statementHash(projection+"|"+tableName+"|"+where) {
			return partiqlResult{}, awserr.Validation("Invalid NextToken")
		}
		startPK, startRange, err := model.ExtractKey(t, key)
		if err != nil {
			return partiqlResult{}, awserr.Validation("Invalid NextToken")
		}
		if startPK != hashKey {
			return partiqlResult{}, awserr.Validation("Invalid NextToken")
		}
		startSK = startRange
	}

	items, err := repos.Items().QueryByPK(ctx, t.Name, hashKey, startSK, true, 0)
	if err != nil {
		return partiqlResult{}, err
	}

	maxItems := parseLimit(limit)
	if maxItems == 0 {
		maxItems = len(items)
	}
	out := make([]map[string]any, 0, minInt(maxItems, len(items)))
	readUnits := 0.0
	var last map[string]any
	for i, item := range items {
		readUnits += model.CalculateReadCapacityUnits(model.CalculateItemSizeBytes(item), consistentRead)
		if !matchAllPredicates(item, preds) {
			continue
		}
		if len(out) < maxItems {
			out = append(out, projectItem(item, projectionAttrs))
		}
		if len(out) == maxItems && i < len(items)-1 {
			last = keyFromItem(t, item)
			break
		}
	}
	if err := s.ensureReadCapacity(t, readUnits); err != nil {
		return partiqlResult{}, err
	}
	res := partiqlResult{Items: out, ReadUnits: readUnits, TableName: t.Name}
	if last != nil {
		res.LastEvaluatedKey = last
		res.NextToken = encodePartiQLNextToken(last, t.Name, statementHash(projection+"|"+tableName+"|"+where))
	}
	return res, nil
}

func (s *Server) runPartiQLInsert(ctx context.Context, repos uow.Repos, tableName string, valueExpr string, parameters []any) (partiqlResult, error) {
	t, err := s.getActiveTableFromRepo(ctx, repos.Tables(), tableName)
	if err != nil {
		return partiqlResult{}, err
	}
	item, consumed, err := parsePartiQLValueObject(valueExpr, parameters)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	if consumed != len(parameters) {
		return partiqlResult{}, awserr.Validation("Incorrect number of parameters for statement")
	}
	if model.ItemTooLarge(item) {
		return partiqlResult{}, awserr.Validation("Item size has exceeded the maximum allowed size")
	}
	pk, sk, err := model.ExtractItemKeys(t, item)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	if err := model.ValidateSecondaryIndexKeyTypes(t, item); err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	current, err := repos.Items().GetItem(ctx, t.Name, pk, sk)
	existed := true
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return partiqlResult{}, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		existed = false
		current = nil
	}
	writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(item))
	if err := s.ensureWriteCapacity(t, writeUnits); err != nil {
		return partiqlResult{}, err
	}
	if err := repos.Items().PutItem(ctx, t, pk, sk, item); err != nil {
		return partiqlResult{}, err
	}
	eventName := "INSERT"
	if existed {
		eventName = "MODIFY"
	}
	if err := s.emitMutationEventForWrite(ctx, repos, t, eventName, keyAttributesFromItem(t, item), current, item, time.Now().UnixMilli()); err != nil {
		return partiqlResult{}, err
	}
	return partiqlResult{Items: []map[string]any{}, WriteUnits: writeUnits, TableName: t.Name}, nil
}

func (s *Server) runPartiQLUpdate(ctx context.Context, repos uow.Repos, tableName string, setExpr string, where string, parameters []any, returnValuesOnConditionCheckFailure string) (partiqlResult, error) {
	t, err := s.getActiveTableFromRepo(ctx, repos.Tables(), tableName)
	if err != nil {
		return partiqlResult{}, err
	}
	setters, usedBySet, err := parseSetClause(setExpr, parameters)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	preds, usedByWhere, err := parsePredicates(where, parameters[usedBySet:])
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	if usedBySet+usedByWhere != len(parameters) {
		return partiqlResult{}, awserr.Validation("Incorrect number of parameters for statement")
	}
	hashPred, ok := findPredicate(preds, t.HashKey)
	if !ok {
		return partiqlResult{}, awserr.Validation("PartiQL WHERE clause must include partition key equality")
	}
	if hashPred.Op != "=" {
		return partiqlResult{}, awserr.Validation("Partition key predicate must be equality")
	}
	hashValue := hashPred.A
	hashKey, err := model.SerializeKeyValue(hashValue)
	if err != nil {
		return partiqlResult{}, awserr.Validation("Invalid partition key value")
	}
	rangeKey := model.NoSortKey
	var rangeValue any
	if t.RangeKey != "" {
		rangePred, ok := findPredicate(preds, t.RangeKey)
		if !ok {
			return partiqlResult{}, awserr.Validation("PartiQL WHERE clause must include sort key equality")
		}
		if rangePred.Op != "=" {
			return partiqlResult{}, awserr.Validation("Sort key predicate must be equality")
		}
		rangeValue = rangePred.A
		rangeKey, err = model.SerializeKeyValue(rangePred.A)
		if err != nil {
			return partiqlResult{}, awserr.Validation("Invalid sort key value")
		}
	}
	current, err := repos.Items().GetItem(ctx, t.Name, hashKey, rangeKey)
	itemExisted := true
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return partiqlResult{}, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		itemExisted = false
		current = map[string]any{t.HashKey: hashValue}
		if t.RangeKey != "" {
			current[t.RangeKey] = rangeValue
		}
	}
	if itemExisted && !matchAllPredicates(current, preds) {
		return partiqlResult{}, awserr.ConditionalCheckFailedWithItem("The conditional request failed", itemForConditionFailure(returnValuesOnConditionCheckFailure, current, itemExisted))
	}
	updated := cloneItem(current)
	for attr, val := range setters {
		updated[attr] = val
	}
	if model.ItemTooLarge(updated) {
		return partiqlResult{}, awserr.Validation("Item size has exceeded the maximum allowed size")
	}
	if err := model.ValidateSecondaryIndexKeyTypes(t, updated); err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(updated))
	if err := s.ensureWriteCapacity(t, writeUnits); err != nil {
		return partiqlResult{}, err
	}
	if err := repos.Items().PutItem(ctx, t, hashKey, rangeKey, updated); err != nil {
		return partiqlResult{}, err
	}
	eventName := "INSERT"
	oldForStream := map[string]any(nil)
	if itemExisted {
		eventName = "MODIFY"
		oldForStream = current
	}
	streamKey := map[string]any{t.HashKey: hashValue}
	if t.RangeKey != "" {
		streamKey[t.RangeKey] = rangeValue
	}
	if err := s.emitMutationEventForWrite(ctx, repos, t, eventName, keyAttributesFromKey(t, streamKey), oldForStream, updated, time.Now().UnixMilli()); err != nil {
		return partiqlResult{}, err
	}
	return partiqlResult{Items: []map[string]any{}, WriteUnits: writeUnits, TableName: t.Name}, nil
}

func (s *Server) runPartiQLDelete(ctx context.Context, repos uow.Repos, tableName string, where string, parameters []any, returnValuesOnConditionCheckFailure string) (partiqlResult, error) {
	t, err := s.getActiveTableFromRepo(ctx, repos.Tables(), tableName)
	if err != nil {
		return partiqlResult{}, err
	}
	preds, consumed, err := parsePredicates(where, parameters)
	if err != nil {
		return partiqlResult{}, awserr.Validation(err.Error())
	}
	if consumed != len(parameters) {
		return partiqlResult{}, awserr.Validation("Incorrect number of parameters for statement")
	}
	hashPred, ok := findPredicate(preds, t.HashKey)
	if !ok {
		return partiqlResult{}, awserr.Validation("PartiQL WHERE clause must include partition key equality")
	}
	if hashPred.Op != "=" {
		return partiqlResult{}, awserr.Validation("Partition key predicate must be equality")
	}
	hashValue := hashPred.A
	hashKey, err := model.SerializeKeyValue(hashValue)
	if err != nil {
		return partiqlResult{}, awserr.Validation("Invalid partition key value")
	}
	rangeKey := model.NoSortKey
	var rangeValue any
	if t.RangeKey != "" {
		rangePred, ok := findPredicate(preds, t.RangeKey)
		if !ok {
			return partiqlResult{}, awserr.Validation("PartiQL WHERE clause must include sort key equality")
		}
		if rangePred.Op != "=" {
			return partiqlResult{}, awserr.Validation("Sort key predicate must be equality")
		}
		rangeValue = rangePred.A
		rangeKey, err = model.SerializeKeyValue(rangePred.A)
		if err != nil {
			return partiqlResult{}, awserr.Validation("Invalid sort key value")
		}
	}
	current, err := repos.Items().GetItem(ctx, t.Name, hashKey, rangeKey)
	existed := true
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return partiqlResult{}, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		existed = false
		current = map[string]any{t.HashKey: hashValue}
		if t.RangeKey != "" {
			current[t.RangeKey] = rangeValue
		}
	}
	if existed && !matchAllPredicates(current, preds) {
		return partiqlResult{}, awserr.ConditionalCheckFailedWithItem("The conditional request failed", itemForConditionFailure(returnValuesOnConditionCheckFailure, current, existed))
	}
	writeUnits := model.CalculateWriteCapacityUnits(model.CalculateItemSizeBytes(current))
	if err := s.ensureWriteCapacity(t, writeUnits); err != nil {
		return partiqlResult{}, err
	}
	if err := repos.Items().DeleteItem(ctx, t.Name, hashKey, rangeKey); err != nil {
		return partiqlResult{}, err
	}
	if existed {
		streamKey := map[string]any{t.HashKey: hashValue}
		if t.RangeKey != "" {
			streamKey[t.RangeKey] = rangeValue
		}
		if err := s.emitMutationEventForWrite(ctx, repos, t, "REMOVE", keyAttributesFromKey(t, streamKey), current, nil, time.Now().UnixMilli()); err != nil {
			return partiqlResult{}, err
		}
	}
	return partiqlResult{Items: []map[string]any{}, WriteUnits: writeUnits, TableName: t.Name}, nil
}

func parseProjection(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "*" {
		return nil, nil
	}
	parts := splitTopLevel(raw, ',')
	if len(parts) == 0 {
		return nil, fmt.Errorf("Invalid SELECT projection")
	}
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		attr := strings.TrimSpace(part)
		if attr == "" {
			continue
		}
		if idx := strings.Index(strings.ToUpper(attr), " AS "); idx > 0 {
			attr = strings.TrimSpace(attr[:idx])
		}
		out = append(out, strings.Trim(attr, "\"`"))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("Invalid SELECT projection")
	}
	return out, nil
}

func parsePredicates(where string, parameters []any) ([]partiqlPredicate, int, error) {
	parts := splitByAND(where)
	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("Invalid WHERE clause")
	}
	preds := make([]partiqlPredicate, 0, len(parts))
	used := 0
	for _, part := range parts {
		if m := betweenPattern.FindStringSubmatch(part); len(m) == 2 {
			if used+1 >= len(parameters) {
				return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
			}
			preds = append(preds, partiqlPredicate{Attr: strings.TrimSpace(m[1]), Op: "BETWEEN", A: parameters[used], B: parameters[used+1]})
			used += 2
			continue
		}
		if m := inPattern.FindStringSubmatch(part); len(m) == 3 {
			placeholders := splitTopLevel(strings.TrimSpace(m[2]), ',')
			if len(placeholders) == 0 {
				return nil, 0, fmt.Errorf("IN predicate must include at least one placeholder")
			}
			values := make([]any, 0, len(placeholders))
			for _, ph := range placeholders {
				if strings.TrimSpace(ph) != "?" {
					return nil, 0, fmt.Errorf("IN predicate only supports placeholders")
				}
				if used >= len(parameters) {
					return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
				}
				values = append(values, parameters[used])
				used++
			}
			preds = append(preds, partiqlPredicate{Attr: strings.TrimSpace(m[1]), Op: "IN", List: values})
			continue
		}
		if m := beginsWithPattern.FindStringSubmatch(part); len(m) == 2 {
			if used >= len(parameters) {
				return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
			}
			preds = append(preds, partiqlPredicate{Attr: strings.TrimSpace(m[1]), Op: "BEGINS_WITH", A: parameters[used]})
			used++
			continue
		}
		if m := comparisonPattern.FindStringSubmatch(part); len(m) == 3 {
			if used >= len(parameters) {
				return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
			}
			preds = append(preds, partiqlPredicate{Attr: strings.TrimSpace(m[1]), Op: strings.ToUpper(strings.TrimSpace(m[2])), A: parameters[used]})
			used++
			continue
		}
		return nil, 0, fmt.Errorf("Unsupported predicate in WHERE clause")
	}
	return preds, used, nil
}

func findPredicate(preds []partiqlPredicate, attr string) (partiqlPredicate, bool) {
	for _, p := range preds {
		if p.Attr == attr {
			return p, true
		}
	}
	return partiqlPredicate{}, false
}

func matchAllPredicates(item map[string]any, preds []partiqlPredicate) bool {
	for _, p := range preds {
		actual, ok := item[p.Attr]
		if !ok {
			return false
		}
		if !matchPredicate(actual, p) {
			return false
		}
	}
	return true
}

func matchPredicate(actual any, pred partiqlPredicate) bool {
	switch pred.Op {
	case "=":
		return compareAttributeValue(actual, pred.A) == 0
	case "<>":
		return compareAttributeValue(actual, pred.A) != 0
	case "<":
		return compareAttributeValue(actual, pred.A) < 0
	case "<=":
		return compareAttributeValue(actual, pred.A) <= 0
	case ">":
		return compareAttributeValue(actual, pred.A) > 0
	case ">=":
		return compareAttributeValue(actual, pred.A) >= 0
	case "BETWEEN":
		return compareAttributeValue(actual, pred.A) >= 0 && compareAttributeValue(actual, pred.B) <= 0
	case "BEGINS_WITH":
		as, aok := attributeString(actual)
		ps, pok := attributeString(pred.A)
		return aok && pok && strings.HasPrefix(as, ps)
	case "IN":
		for _, candidate := range pred.List {
			if compareAttributeValue(actual, candidate) == 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func compareAttributeValue(a, b any) int {
	aNum, aNumOK := attributeNumber(a)
	bNum, bNumOK := attributeNumber(b)
	if aNumOK && bNumOK {
		if aNum < bNum {
			return -1
		}
		if aNum > bNum {
			return 1
		}
		return 0
	}
	as, aStrOK := attributeString(a)
	bs, bStrOK := attributeString(b)
	if aStrOK && bStrOK {
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
	if reflect.DeepEqual(a, b) {
		return 0
	}
	return -1
}

func attributeString(v any) (string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return "", false
	}
	s, ok := m["S"].(string)
	return s, ok
}

func attributeNumber(v any) (float64, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return 0, false
	}
	ns, ok := m["N"].(string)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseFloat(ns, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func parseSetClause(setExpr string, parameters []any) (map[string]any, int, error) {
	parts := splitTopLevel(setExpr, ',')
	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("Invalid SET clause")
	}
	used := 0
	setters := map[string]any{}
	for _, part := range parts {
		m := setClausePattern.FindStringSubmatch(part)
		if len(m) != 2 {
			return nil, 0, fmt.Errorf("SET clause only supports assignments with placeholders")
		}
		if used >= len(parameters) {
			return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
		}
		setters[strings.TrimSpace(m[1])] = parameters[used]
		used++
	}
	return setters, used, nil
}

func parsePartiQLValueObject(valueExpr string, parameters []any) (map[string]any, int, error) {
	valueExpr = strings.TrimSpace(valueExpr)
	if !strings.HasPrefix(valueExpr, "{") || !strings.HasSuffix(valueExpr, "}") {
		return nil, 0, fmt.Errorf("VALUE clause must be an object literal")
	}
	body := strings.TrimSpace(valueExpr[1 : len(valueExpr)-1])
	if body == "" {
		return map[string]any{}, 0, nil
	}
	parts := splitTopLevel(body, ',')
	item := map[string]any{}
	used := 0
	for _, part := range parts {
		m := valuePairPattern.FindStringSubmatch(part)
		if len(m) != 2 {
			return nil, 0, fmt.Errorf("VALUE object only supports key: ? pairs")
		}
		if used >= len(parameters) {
			return nil, 0, fmt.Errorf("Incorrect number of parameters for statement")
		}
		item[strings.TrimSpace(m[1])] = parameters[used]
		used++
	}
	return item, used, nil
}

func projectItem(item map[string]any, projection []string) map[string]any {
	if len(projection) == 0 {
		return cloneItem(item)
	}
	out := map[string]any{}
	for _, attr := range projection {
		if v, ok := item[attr]; ok {
			out[attr] = v
		}
	}
	return out
}

func parseTableIdentifier(raw string) string {
	return strings.Trim(strings.TrimSpace(raw), "\"`")
}

func splitByAND(where string) []string {
	normalized := strings.ReplaceAll(where, "\n", " ")
	pattern := regexp.MustCompile(`(?i)\s+AND\s+`)
	rawParts := pattern.Split(normalized, -1)
	parts := make([]string, 0, len(rawParts))
	for i := 0; i < len(rawParts); i++ {
		part := strings.TrimSpace(rawParts[i])
		if part == "" {
			continue
		}
		upper := strings.ToUpper(part)
		if strings.Contains(upper, " BETWEEN ") && strings.Count(part, "?") == 1 && i+1 < len(rawParts) {
			next := strings.TrimSpace(rawParts[i+1])
			part = part + " AND " + next
			i++
		}
		parts = append(parts, part)
	}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func encodePartiQLNextToken(key map[string]any, tableName, statementHash string) string {
	raw, err := json.Marshal(partiqlNextToken{Key: key, TableName: tableName, StatementHash: statementHash})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePartiQLNextToken(token string) (map[string]any, string, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return nil, "", "", err
	}
	var decoded partiqlNextToken
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, "", "", err
	}
	if len(decoded.Key) == 0 {
		return nil, "", "", fmt.Errorf("invalid next token")
	}
	return decoded.Key, decoded.TableName, decoded.StatementHash, nil
}

func batchStatementErrorResponse(statement string, err error) map[string]any {
	tableName := parseTableNameFromStatement(statement)
	code, message, item := mapPartiQLError(err)
	out := map[string]any{
		"Error": map[string]any{
			"Code":    code,
			"Message": message,
		},
	}
	if item != nil {
		out["Error"].(map[string]any)["Item"] = item
	}
	if tableName != "" {
		out["TableName"] = tableName
	}
	return out
}

func parseTableNameFromStatement(statement string) string {
	statement = strings.TrimSpace(statement)
	for _, pattern := range []*regexp.Regexp{selectStatementPattern, insertStatementPattern, updateStatementPattern, deleteStatementPattern} {
		m := pattern.FindStringSubmatch(statement)
		if len(m) >= 2 {
			if pattern == selectStatementPattern {
				return parseTableIdentifier(m[2])
			}
			return parseTableIdentifier(m[1])
		}
	}
	return ""
}

func partiQLOperation(statement string) string {
	statement = strings.TrimSpace(statement)
	switch {
	case selectStatementPattern.MatchString(statement):
		return "SELECT"
	case insertStatementPattern.MatchString(statement):
		return "INSERT"
	case updateStatementPattern.MatchString(statement):
		return "UPDATE"
	case deleteStatementPattern.MatchString(statement):
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func statementHash(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (s *Server) lookupExecuteTransactionIdempotency(ctx context.Context, req executeTransactionRequest) (map[string]any, bool, error) {
	hash, err := executeTransactionRequestHash(req)
	if err != nil {
		return nil, false, err
	}
	nowMillis := time.Now().UnixMilli()
	var rec model.TransactWriteIdempotencyRecord
	found := false
	if err := s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		var err error
		rec, err = repos.Items().GetTransactWriteIdempotency(txCtx, req.ClientRequestToken, nowMillis)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		found = true
		return nil
	}); err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	if rec.RequestHash != hash {
		return nil, true, nil
	}
	if rec.Response == nil {
		return map[string]any{}, false, nil
	}
	return rec.Response, false, nil
}

func (s *Server) storeExecuteTransactionIdempotency(ctx context.Context, req executeTransactionRequest, resp map[string]any, nowMillis int64) error {
	hash, err := executeTransactionRequestHash(req)
	if err != nil {
		return err
	}
	record := model.TransactWriteIdempotencyRecord{
		Token:       req.ClientRequestToken,
		RequestHash: hash,
		Response:    resp,
		CreatedAt:   nowMillis,
		ExpiresAt:   nowMillis + int64((10*time.Minute)/time.Millisecond),
	}
	return s.unitOfWork.Do(ctx, func(txCtx context.Context, repos uow.Repos) error {
		return repos.Items().PutTransactWriteIdempotency(txCtx, record)
	})
}

func executeTransactionRequestHash(req executeTransactionRequest) (string, error) {
	req.ClientRequestToken = ""
	raw, err := json.Marshal(req)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func transactionCancellationReasons(total int, failedIndex int, err error) []awserr.CancellationReason {
	reasons := make([]awserr.CancellationReason, 0, total)
	for i := 0; i < total; i++ {
		if i != failedIndex {
			reasons = append(reasons, awserr.CancellationReason{Code: "None"})
			continue
		}
		reasons = append(reasons, mapPartiQLTransactionReason(err))
	}
	return reasons
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
