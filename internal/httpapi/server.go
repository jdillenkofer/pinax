package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	crand "crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	backupapp "github.com/jdillenkofer/pinax/internal/app/backup"
	pitrapp "github.com/jdillenkofer/pinax/internal/app/pitr"
	resourcepolicyapp "github.com/jdillenkofer/pinax/internal/app/resourcepolicy"
	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/expr"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
	"github.com/jdillenkofer/pinax/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const targetPrefix = "DynamoDB_20120810."
const streamsTargetPrefix = "DynamoDBStreams_20120810."

const (
	defaultAccountMaxReadCapacityUnits  int64 = 80000
	defaultAccountMaxWriteCapacityUnits int64 = 80000
	defaultTableMaxReadCapacityUnits    int64 = 40000
	defaultTableMaxWriteCapacityUnits   int64 = 40000
	defaultDescribeEndpointsCachePeriod int64 = 60
	listTagsOfResourcePageSize                = 10
	defaultStreamReadLimit                    = 1000
	defaultListStreamsLimit                   = 100
	defaultDescribeStreamLimit                = 100
	defaultStreamIteratorTTL                  = 15 * time.Minute
	streamRetentionMillis                     = 24 * 60 * 60 * 1000
	streamResponseMaxBytes                    = 1024 * 1024
	streamDefaultShardID                      = "shardId-000000000000"
	maxResourcePolicyBytes                    = 20 * 1024
	defaultLocalAccountID                     = "000000000000"
	scopedTableKeySeparator                   = "#"
)

const policyNotFoundMessage = "The operation tried to access a nonexistent resource-based policy."

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pinax_http_requests_total",
			Help: "Total HTTP requests processed by Pinax.",
		},
		[]string{"operation", "status_code", "result"},
	)

	httpRequestDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pinax_http_request_duration_seconds",
			Help:    "HTTP request duration by operation.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "status_code", "result"},
	)

	conditionalCheckFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pinax_conditional_check_failures_total",
			Help: "Total conditional check failures by operation.",
		},
		[]string{"operation"},
	)

	throttlingFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pinax_throttling_failures_total",
			Help: "Total provisioned throughput throttling failures by operation.",
		},
		[]string{"operation"},
	)
)

type Server struct {
	store                         store.Store
	unitOfWork                    uow.UnitOfWork
	tableLifecycle                *tableapp.LifecycleService
	tableService                  *tableapp.Service
	backupService                 *backupapp.Service
	pitrService                   *pitrapp.Service
	resourcePolicyService         *resourcepolicyapp.Service
	requestAuthorizer             authorization.RequestAuthorizer
	mutationExecutor              *mutation.Executor
	capMu                         sync.Mutex
	capacityWindows               map[string]capacityWindow
	pitrLatestRestorableLagMillis int64
	streamIteratorTTL             time.Duration
	streamIteratorSigningKey      []byte
}

type capacityWindow struct {
	second    int64
	readUsed  float64
	writeUsed float64
}

type ServerOption func(*Server)

func WithPITRLatestRestorableLagMillis(ms int64) ServerOption {
	if ms < 0 {
		ms = 0
	}
	return func(s *Server) {
		s.pitrLatestRestorableLagMillis = ms
	}
}

func WithStreamIteratorTTL(ttl time.Duration) ServerOption {
	if ttl <= 0 {
		ttl = defaultStreamIteratorTTL
	}
	return func(s *Server) {
		s.streamIteratorTTL = ttl
	}
}

func WithStreamIteratorSigningKey(key []byte) ServerOption {
	trimmed := append([]byte(nil), key...)
	if len(trimmed) == 0 {
		trimmed = newStreamIteratorSigningKey()
	}
	return func(s *Server) {
		s.streamIteratorSigningKey = trimmed
	}
}

func WithMutationHooks(hooks ...mutation.Hook) ServerOption {
	return func(s *Server) {
		s.mutationExecutor = mutation.NewExecutor(hooks...)
	}
}

func NewServer(store store.Store, requestAuthorizer authorization.RequestAuthorizer, opts ...ServerOption) *Server {
	tableLifecycle := tableapp.NewLifecycleService()
	unitOfWork := uow.NewStoreUnitOfWork(store)
	s := &Server{
		store:                         store,
		unitOfWork:                    unitOfWork,
		tableLifecycle:                tableLifecycle,
		tableService:                  tableapp.NewService(unitOfWork, tableLifecycle),
		backupService:                 backupapp.NewService(unitOfWork, tableLifecycle),
		pitrService:                   pitrapp.NewService(unitOfWork, tableLifecycle),
		resourcePolicyService:         resourcepolicyapp.NewService(unitOfWork, tableLifecycle),
		requestAuthorizer:             requestAuthorizer,
		mutationExecutor:              mutation.NewExecutor(),
		capacityWindows:               map[string]capacityWindow{},
		pitrLatestRestorableLagMillis: pitrLatestRestorableLagMillisFromEnv(),
		streamIteratorTTL:             defaultStreamIteratorTTL,
		streamIteratorSigningKey:      newStreamIteratorSigningKey(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	iw := &instrumentedResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
	operation := "unknown"
	errCode := ""
	start := time.Now()
	defer func() {
		s.observeRequest(operation, iw.statusCode, errCode, time.Since(start))
	}()

	if r.Method != http.MethodPost || r.URL.Path != "/" {
		errCode = "NotFound"
		http.NotFound(iw, r)
		return
	}

	target := strings.TrimSpace(r.Header.Get("X-Amz-Target"))
	op, ok := parseTargetOperation(target)
	if !ok {
		err := awserr.Validation("X-Amz-Target header must look like DynamoDB_20120810.<Operation> or DynamoDBStreams_20120810.<Operation>")
		errCode = apiErrorCodeForMetrics(err)
		awserr.Write(iw, err)
		return
	}
	operation = op
	slog.InfoContext(r.Context(), "DynamoDB operation", "operation", op)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		errCode = apiErrorCodeForMetrics(err)
		awserr.Write(iw, err)
		return
	}

	if err := s.authorizeRequest(r, op, body); err != nil {
		slog.WarnContext(r.Context(), "request authorization failed", "operation", op, "table", tableNameFromBody(body), "err", err)
		errCode = apiErrorCodeForMetrics(err)
		awserr.Write(iw, err)
		return
	}
	resp, err := s.dispatch(r, op, body)
	if err != nil {
		slog.WarnContext(r.Context(), "operation failed", "operation", op, "table", tableNameFromBody(body), "err", err)
		errCode = apiErrorCodeForMetrics(err)
		awserr.Write(iw, err)
		return
	}

	encoded, err := json.Marshal(resp)
	if err != nil {
		slog.Error("encode response", "operation", op, "err", err)
		errCode = "InternalServerError"
		awserr.Write(iw, awserr.Internal("failed to encode response"))
		return
	}
	iw.Header().Set("Content-Type", "application/x-amz-json-1.0")
	iw.Header().Set("X-Amz-Crc32", strconv.FormatUint(uint64(crc32.ChecksumIEEE(encoded)), 10))
	_, _ = iw.Write(encoded)
	slog.InfoContext(r.Context(), "operation succeeded", "operation", op, "table", tableNameFromBody(body))
}

func tableNameFromBody(body []byte) string {
	var payload struct {
		TableName string `json:"TableName"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return payload.TableName
}

func (s *Server) observeRequest(operation string, statusCode int, errorCode string, duration time.Duration) {
	statusLabel := strconv.Itoa(statusCode)
	result := "success"
	if statusCode >= http.StatusBadRequest {
		result = "error"
	}
	httpRequestsTotal.WithLabelValues(operation, statusLabel, result).Inc()
	httpRequestDurationSeconds.WithLabelValues(operation, statusLabel, result).Observe(duration.Seconds())
	if errorCode == "ConditionalCheckFailedException" {
		conditionalCheckFailuresTotal.WithLabelValues(operation).Inc()
	}
	if errorCode == "ProvisionedThroughputExceededException" {
		throttlingFailuresTotal.WithLabelValues(operation).Inc()
	}
}

func parseTargetOperation(target string) (string, bool) {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, targetPrefix) {
		return strings.TrimPrefix(target, targetPrefix), true
	}
	if strings.HasPrefix(target, streamsTargetPrefix) {
		return strings.TrimPrefix(target, streamsTargetPrefix), true
	}
	return "", false
}

func newStreamIteratorSigningKey() []byte {
	key := make([]byte, 32)
	if _, err := crand.Read(key); err == nil {
		return key
	}
	fallback := sha256.Sum256([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
	return fallback[:]
}

func apiErrorCodeForMetrics(err error) string {
	var apiErr *awserr.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return "InternalServerError"
}

type instrumentedResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *instrumentedResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

type monitoringHandler struct {
	store          store.Store
	metricsHandler http.Handler
}

func NewMonitoringHandler(store store.Store) http.Handler {
	return &monitoringHandler{store: store, metricsHandler: promhttp.Handler()}
}

func (h *monitoringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if r.URL.Path == "/health" {
		if err := h.store.DB().PingContext(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("Unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Healthy"))
		return
	}
	if r.URL.Path == "/metrics" {
		h.metricsHandler.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) authorizeRequest(r *http.Request, operation string, body []byte) error {
	if v, ok := r.Context().Value(authentication.IsAuthenticatedContextKey{}).(bool); ok && !v {
		return &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}

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
	case "DescribeLimits":
		return s.describeLimits(r, body)
	case "DescribeEndpoints":
		return s.describeEndpoints(r, body)
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
	case "CreateBackup":
		return s.createBackup(r, body)
	case "DescribeBackup":
		return s.describeBackup(r, body)
	case "ListBackups":
		return s.listBackups(r, body)
	case "DeleteBackup":
		return s.deleteBackup(r, body)
	case "RestoreTableFromBackup":
		return s.restoreTableFromBackup(r, body)
	case "UpdateContinuousBackups":
		return s.updateContinuousBackups(r, body)
	case "DescribeContinuousBackups":
		return s.describeContinuousBackups(r, body)
	case "RestoreTableToPointInTime":
		return s.restoreTableToPointInTime(r, body)
	case "TagResource":
		return s.tagResource(r, body)
	case "UntagResource":
		return s.untagResource(r, body)
	case "ListTagsOfResource":
		return s.listTagsOfResource(r, body)
	case "PutResourcePolicy":
		return s.putResourcePolicy(r, body)
	case "GetResourcePolicy":
		return s.getResourcePolicy(r, body)
	case "DeleteResourcePolicy":
		return s.deleteResourcePolicy(r, body)
	case "ExecuteStatement":
		return s.executeStatement(r, body)
	case "BatchExecuteStatement":
		return s.batchExecuteStatement(r, body)
	case "ExecuteTransaction":
		return s.executeTransaction(r, body)
	case "ListStreams":
		return s.listStreams(r, body)
	case "DescribeStream":
		return s.describeStream(r, body)
	case "GetShardIterator":
		return s.getShardIterator(r, body)
	case "GetRecords":
		return s.getRecords(r, body)
	default:
		return nil, awserr.Validation("unsupported operation " + operation)
	}
}

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
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, &awserr.APIError{Code: "TableNotFoundException", Message: "Cannot do operations on a non-existent table", Status: http.StatusBadRequest}
		}
		if errors.Is(err, backupapp.ErrTargetTableInUse) {
			return nil, &awserr.APIError{Code: "TableInUseException", Message: "A target table with the specified name is either being created or deleted.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, backupapp.ErrBackupExists) {
			return nil, &awserr.APIError{Code: "BackupInUseException", Message: "Backup with the requested name already exists", Status: http.StatusBadRequest}
		}
		return nil, err
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
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &awserr.APIError{Code: "BackupNotFoundException", Message: "Backup not found for the given BackupARN.", Status: http.StatusBadRequest}
		}
		return nil, err
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
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &awserr.APIError{Code: "BackupNotFoundException", Message: "Backup not found for the given BackupARN.", Status: http.StatusBadRequest}
		}
		return nil, err
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
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &awserr.APIError{Code: "BackupNotFoundException", Message: "Backup not found for the given BackupARN.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, backupapp.ErrTargetTableInUse) {
			return nil, &awserr.APIError{Code: "TableInUseException", Message: "A target table with the specified name is either being created or deleted.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, backupapp.ErrTargetTableExists) {
			return nil, &awserr.APIError{Code: "TableAlreadyExistsException", Message: "A target table with the specified name already exists.", Status: http.StatusBadRequest}
		}
		return nil, err
	}

	return map[string]any{"TableDescription": tableToCreate.Description(int64(restoredItems))}, nil
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

type tagResourceRequest struct {
	ResourceARN string `json:"ResourceArn"`
	Tags        []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsOfResourceRequest struct {
	ResourceARN string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
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
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, &awserr.APIError{Code: "TableNotFoundException", Message: "Cannot do operations on a non-existent table", Status: http.StatusBadRequest}
		}
		return nil, err
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
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, &awserr.APIError{Code: "TableNotFoundException", Message: "Cannot do operations on a non-existent table", Status: http.StatusBadRequest}
		}
		return nil, err
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
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, &awserr.APIError{Code: "TableNotFoundException", Message: "Cannot do operations on a non-existent table", Status: http.StatusBadRequest}
		}
		if errors.Is(err, pitrapp.ErrPointInTimeRecoveryUnavailable) {
			return nil, &awserr.APIError{Code: "PointInTimeRecoveryUnavailableException", Message: "Point in time recovery has not yet been enabled for this source table.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, pitrapp.ErrInvalidRestoreTime) {
			return nil, &awserr.APIError{Code: "InvalidRestoreTimeException", Message: "RestoreDateTime must be between EarliestRestorableDateTime and LatestRestorableDateTime.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, pitrapp.ErrTargetTableInUse) {
			return nil, &awserr.APIError{Code: "TableInUseException", Message: "A target table with the specified name is either being created or deleted.", Status: http.StatusBadRequest}
		}
		if errors.Is(err, pitrapp.ErrTargetTableExists) {
			return nil, &awserr.APIError{Code: "TableAlreadyExistsException", Message: "A target table with the specified name already exists.", Status: http.StatusBadRequest}
		}
		return nil, err
	}

	return map[string]any{"TableDescription": tableToCreate.Description(count)}, nil
}

func (s *Server) tagResource(r *http.Request, body []byte) (map[string]any, error) {
	var req tagResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}
	if len(req.Tags) == 0 {
		return nil, awserr.Validation("Tags is required")
	}
	tags, err := normalizeTags(req.Tags)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.tableService.TagTable(r.Context(), tableKey, tags, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		if errors.Is(err, tableapp.ErrTooManyTags) {
			return nil, awserr.Validation("Tags can have at most 50 items")
		}
		return nil, err
	}
	return map[string]any{}, nil
}

func (s *Server) untagResource(r *http.Request, body []byte) (map[string]any, error) {
	var req untagResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}
	if len(req.TagKeys) == 0 {
		return nil, awserr.Validation("TagKeys is required")
	}
	keys, err := normalizeTagKeys(req.TagKeys)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.tableService.UntagTable(r.Context(), tableKey, keys, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		return nil, err
	}
	return map[string]any{}, nil
}

func (s *Server) listTagsOfResource(r *http.Request, body []byte) (map[string]any, error) {
	var req listTagsOfResourceRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ResourceARN) == "" {
		return nil, awserr.Validation("ResourceArn is required")
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	sortedTags, err := s.tableService.ListTableTags(r.Context(), tableKey, lifecycleNow())
	if err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Requested resource not found")
		}
		return nil, err
	}
	tags := make([]map[string]any, 0, len(sortedTags))
	start, err := parseListTagsOfResourceStartToken(req.NextToken, len(sortedTags))
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	end := len(sortedTags)
	if max := start + listTagsOfResourcePageSize; max < end {
		end = max
	}
	for _, tag := range sortedTags[start:end] {
		tags = append(tags, map[string]any{"Key": tag.Key, "Value": tag.Value})
	}
	resp := map[string]any{"Tags": tags}
	if end < len(sortedTags) {
		resp["NextToken"] = encodeListTagsOfResourceStartToken(end)
	}
	return resp, nil
}

func parseListTagsOfResourceStartToken(nextToken string, total int) (int, error) {
	token := strings.TrimSpace(nextToken)
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("Invalid NextToken")
	}
	start, err := strconv.Atoi(string(raw))
	if err != nil || start < 0 || start >= total {
		return 0, fmt.Errorf("Invalid NextToken")
	}
	return start, nil
}

func encodeListTagsOfResourceStartToken(start int) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(start)))
}

type putResourcePolicyRequest struct {
	ResourceARN                     string `json:"ResourceArn"`
	Policy                          string `json:"Policy"`
	ExpectedRevisionID              string `json:"ExpectedRevisionId"`
	ConfirmRemoveSelfResourceAccess bool   `json:"ConfirmRemoveSelfResourceAccess"`
}

type getResourcePolicyRequest struct {
	ResourceARN string `json:"ResourceArn"`
}

type deleteResourcePolicyRequest struct {
	ResourceARN        string `json:"ResourceArn"`
	ExpectedRevisionID string `json:"ExpectedRevisionId"`
}

func (s *Server) putResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req putResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateResourcePolicyDocument(req.Policy); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateExpectedRevisionID(req.ExpectedRevisionID); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}

	revisionID, err := s.resourcePolicyService.Put(r.Context(), resourceARN, req.Policy, req.ExpectedRevisionID, req.ConfirmRemoveSelfResourceAccess, needsConfirmRemoveSelfResourceAccess)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		if errors.Is(err, resourcepolicyapp.ErrConfirmRemoveSelfResourceAccessRequired) {
			return nil, awserr.Validation("This policy contains a statement that may prevent future policy updates for this resource. Set ConfirmRemoveSelfResourceAccess to true to confirm this change")
		}
		return nil, err
	}
	return map[string]any{"RevisionId": revisionID}, nil
}

func (s *Server) getResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req getResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	policy, revisionID, err := s.resourcePolicyService.Get(r.Context(), resourceARN)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		return nil, err
	}
	return map[string]any{"Policy": policy, "RevisionId": revisionID}, nil
}

func (s *Server) deleteResourcePolicy(r *http.Request, body []byte) (map[string]any, error) {
	var req deleteResourcePolicyRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	resourceARN, isStream, err := validateResourcePolicyARN(req.ResourceARN)
	if err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if err := validateExpectedRevisionID(req.ExpectedRevisionID); err != nil {
		return nil, awserr.Validation(err.Error())
	}

	tableName, resourceAccountID, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	if resourceAccountID != "" && resourceAccountID != accountIDFromContext(r.Context()) {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	tableKey := scopedTableKeyFromAccountAndName(accountIDFromContext(r.Context()), tableName)
	if err := s.resourcePolicyService.EnsureTarget(r.Context(), tableKey, resourceARN, isStream, lifecycleNow()); err != nil {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	revisionID, err := s.resourcePolicyService.Delete(r.Context(), resourceARN, req.ExpectedRevisionID)
	if err != nil {
		if errors.Is(err, resourcepolicyapp.ErrPolicyNotFound) {
			return nil, awserr.PolicyNotFound(policyNotFoundMessage)
		}
		return nil, err
	}
	return map[string]any{"RevisionId": revisionID}, nil
}

func validateResourcePolicyARN(resourceARN string) (string, bool, error) {
	resourceARN = strings.TrimSpace(resourceARN)
	if resourceARN == "" {
		return "", false, fmt.Errorf("ResourceArn is required")
	}
	if len(resourceARN) > 1283 {
		return "", false, fmt.Errorf("Member must have length less than or equal to 1283")
	}
	if !strings.HasPrefix(resourceARN, "arn:") {
		return "", false, fmt.Errorf("ResourceArn must be a valid ARN")
	}
	tableName, _, err := taggableTableNameFromResourceARN(resourceARN)
	if err != nil || strings.TrimSpace(tableName) == "" {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	marker := ":table/" + tableName
	start := strings.Index(resourceARN, marker)
	if start < 0 {
		marker = "/table/" + tableName
		start = strings.Index(resourceARN, marker)
	}
	if start < 0 {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	remainder := resourceARN[start+len(marker):]
	if remainder == "" {
		return resourceARN, false, nil
	}
	if !strings.HasPrefix(remainder, "/stream/") {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	streamLabel := strings.TrimSpace(strings.TrimPrefix(remainder, "/stream/"))
	if streamLabel == "" || strings.Contains(streamLabel, "/") {
		return "", false, fmt.Errorf("ResourceArn must identify a DynamoDB table or stream")
	}
	return resourceARN, true, nil
}

func validateResourcePolicyDocument(policy string) error {
	if strings.TrimSpace(policy) == "" {
		return fmt.Errorf("Policy is required")
	}
	if len(policy) > maxResourcePolicyBytes {
		return fmt.Errorf("Policy must be less than or equal to 20480 bytes")
	}
	if !json.Valid([]byte(policy)) {
		return fmt.Errorf("Policy must be valid JSON")
	}
	return nil
}

func validateExpectedRevisionID(expected string) error {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return nil
	}
	if len(expected) > 255 {
		return fmt.Errorf("Member must have length less than or equal to 255")
	}
	return nil
}

func needsConfirmRemoveSelfResourceAccess(resourceARN, policy string) bool {
	var doc map[string]any
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return false
	}
	selfPrincipals := []string{}
	if root := rootPrincipalFromResourceARN(resourceARN); root != "" {
		selfPrincipals = append(selfPrincipals, root)
	}
	statements := normalizePolicyStatements(doc["Statement"])
	for _, raw := range statements {
		stmt, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(valueAsString(stmt["Effect"])), "Deny") {
			continue
		}
		if !policyStatementTargetsSelfResource(stmt, resourceARN) {
			continue
		}
		if !policyStatementDeniesPolicyMutation(stmt) {
			continue
		}
		if policyStatementCanMatchSelfPrincipal(stmt, selfPrincipals) {
			return true
		}
	}
	return false
}

func normalizePolicyStatements(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case map[string]any:
		return []any{t}
	default:
		return nil
	}
}

func policyStatementDeniesPolicyMutation(stmt map[string]any) bool {
	if notAction, ok := stmt["NotAction"]; ok {
		notActions := normalizedActionSet(notAction)
		if !containsAction(notActions, "dynamodb:PutResourcePolicy") || !containsAction(notActions, "dynamodb:DeleteResourcePolicy") {
			return true
		}
		return false
	}
	actions := normalizedActionSet(stmt["Action"])
	for _, a := range actions {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == "*" || a == "dynamodb:*" || a == "dynamodb:putresourcepolicy" || a == "dynamodb:deleteresourcepolicy" {
			return true
		}
	}
	return false
}

func policyStatementTargetsSelfResource(stmt map[string]any, targetARN string) bool {
	if notResource, ok := stmt["NotResource"]; ok {
		notResources := normalizedResourceSet(notResource)
		for _, nr := range notResources {
			nr = strings.TrimSpace(nr)
			if nr == "*" || nr == targetARN {
				return false
			}
		}
		return true
	}
	resources := normalizedResourceSet(stmt["Resource"])
	for _, r := range resources {
		r = strings.TrimSpace(r)
		if r == "*" || r == targetARN {
			return true
		}
	}
	return false
}

func policyStatementCanMatchSelfPrincipal(stmt map[string]any, selfPrincipals []string) bool {
	normalizedSelf := make([]string, 0, len(selfPrincipals))
	for _, s := range selfPrincipals {
		normalizedSelf = append(normalizedSelf, strings.ToLower(strings.TrimSpace(s)))
	}
	if notPrincipal, ok := stmt["NotPrincipal"]; ok {
		excluded := normalizedPrincipalSet(notPrincipal)
		for _, self := range normalizedSelf {
			if !containsPrincipal(excluded, self) && !containsPrincipal(excluded, "*") {
				return true
			}
		}
		return false
	}
	principal, ok := stmt["Principal"]
	if !ok {
		return true
	}
	allowed := normalizedPrincipalSet(principal)
	for _, self := range normalizedSelf {
		if containsPrincipal(allowed, self) || containsPrincipal(allowed, "*") {
			return true
		}
	}
	return false
}

func normalizedActionSet(v any) []string {
	return stringSetFromPolicyField(v)
}

func normalizedResourceSet(v any) []string {
	return stringSetFromPolicyField(v)
}

func normalizedPrincipalSet(principal any) []string {
	switch p := principal.(type) {
	case string:
		return []string{strings.ToLower(strings.TrimSpace(p))}
	case map[string]any:
		out := make([]string, 0)
		for k, vv := range p {
			if !strings.EqualFold(strings.TrimSpace(k), "AWS") {
				continue
			}
			for _, candidate := range stringSetFromPolicyField(vv) {
				out = append(out, strings.ToLower(strings.TrimSpace(candidate)))
			}
		}
		return out
	}
	return nil
}

func rootPrincipalFromResourceARN(resourceARN string) string {
	parts := strings.Split(resourceARN, ":")
	if len(parts) < 6 {
		return ""
	}
	acct := strings.TrimSpace(parts[4])
	if acct == "" {
		return ""
	}
	return "arn:aws:iam::" + acct + ":root"
}

func stringSetFromPolicyField(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func containsAction(actions []string, action string) bool {
	action = strings.ToLower(strings.TrimSpace(action))
	for _, a := range actions {
		a = strings.ToLower(strings.TrimSpace(a))
		if a == action || a == "*" || a == "dynamodb:*" {
			return true
		}
	}
	return false
}

func containsPrincipal(principals []string, principal string) bool {
	principal = strings.ToLower(strings.TrimSpace(principal))
	for _, p := range principals {
		if strings.ToLower(strings.TrimSpace(p)) == principal {
			return true
		}
	}
	return false
}

func valueAsString(v any) string {
	s, _ := v.(string)
	return s
}

type listStreamsRequest struct {
	TableName               string `json:"TableName"`
	Limit                   int    `json:"Limit"`
	ExclusiveStartStreamARN string `json:"ExclusiveStartStreamArn"`
}

type describeStreamRequest struct {
	StreamARN             string `json:"StreamArn"`
	Limit                 int    `json:"Limit"`
	ExclusiveStartShardID string `json:"ExclusiveStartShardId"`
}

type getShardIteratorRequest struct {
	StreamARN         string `json:"StreamArn"`
	ShardID           string `json:"ShardId"`
	ShardIteratorType string `json:"ShardIteratorType"`
	SequenceNumber    string `json:"SequenceNumber"`
}

type getRecordsRequest struct {
	ShardIterator string `json:"ShardIterator"`
	Limit         int    `json:"Limit"`
}

type streamIteratorToken struct {
	StreamARN string `json:"streamArn"`
	ShardID   string `json:"shardId"`
	Sequence  int64  `json:"sequence"`
	ExpiresAt int64  `json:"expiresAt"`
}

func (s *Server) listStreams(r *http.Request, body []byte) (map[string]any, error) {
	var req listStreamsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultListStreamsLimit
	}

	streams := make([]map[string]any, 0)
	start := ""
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		for {
			names, err := repos.Tables().ListTables(txCtx, start, defaultListStreamsLimit)
			if err != nil {
				return err
			}
			if len(names) == 0 {
				break
			}
			for _, name := range names {
				storedAccountID, storedTableName := splitScopedTableKey(name)
				if storedAccountID != accountIDFromContext(txCtx) {
					continue
				}
				if strings.TrimSpace(req.TableName) != "" && storedTableName != req.TableName {
					continue
				}
				t, err := s.getTableWithLifecycleByKey(txCtx, repos.Tables(), name)
				if err != nil {
					continue
				}
				if !t.Stream.Enabled || strings.TrimSpace(t.Stream.ARN) == "" {
					continue
				}
				if strings.TrimSpace(req.ExclusiveStartStreamARN) != "" && t.Stream.ARN <= req.ExclusiveStartStreamARN {
					continue
				}
				streams = append(streams, map[string]any{
					"StreamArn":   t.Stream.ARN,
					"TableName":   logicalTableNameFromKey(t.Name),
					"StreamLabel": t.Stream.Label,
				})
			}
			start = names[len(names)-1]
		}
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(streams, func(i, j int) bool {
		return streams[i]["StreamArn"].(string) < streams[j]["StreamArn"].(string)
	})
	truncated := streams
	if len(truncated) > limit {
		truncated = truncated[:limit]
	}
	resp := map[string]any{"Streams": truncated}
	if len(streams) > limit {
		resp["LastEvaluatedStreamArn"] = truncated[len(truncated)-1]["StreamArn"]
	}

	return resp, nil
}

func (s *Server) describeStream(r *http.Request, body []byte) (map[string]any, error) {
	var req describeStreamRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.StreamARN) == "" {
		return nil, awserr.Validation("StreamArn is required")
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultDescribeStreamLimit
	}

	var (
		t        model.Table
		firstSeq int64
		found    bool
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
		var txErr error
		t, txErr = s.getTableByStreamARN(txCtx, tx, req.StreamARN)
		if txErr != nil {
			return txErr
		}
		firstSeq, _, found, txErr = s.store.GetStreamSequenceBounds(txCtx, tx, req.StreamARN)
		if txErr != nil {
			return txErr
		}
		return nil
	}); err != nil {
		return nil, err
	}
	starting := "0"
	if found {
		starting = strconv.FormatInt(firstSeq, 10)
	}
	shards := []map[string]any{}
	if strings.TrimSpace(req.ExclusiveStartShardID) == "" {
		shards = append(shards, map[string]any{
			"ShardId": streamDefaultShardID,
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": starting,
			},
		})
	}
	if len(shards) > limit {
		shards = shards[:limit]
	}
	resp := map[string]any{
		"StreamDescription": map[string]any{
			"StreamArn":               t.Stream.ARN,
			"StreamStatus":            "ENABLED",
			"StreamViewType":          t.Stream.ViewType,
			"CreationRequestDateTime": t.CreatedAt,
			"TableName":               logicalTableNameFromKey(t.Name),
			"KeySchema":               t.KeySchema(),
			"Shards":                  shards,
		},
	}
	return resp, nil
}

func (s *Server) getShardIterator(r *http.Request, body []byte) (map[string]any, error) {
	var req getShardIteratorRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.StreamARN) == "" {
		return nil, awserr.Validation("StreamArn is required")
	}
	if strings.TrimSpace(req.ShardID) == "" {
		return nil, awserr.Validation("ShardId is required")
	}
	if req.ShardID != streamDefaultShardID {
		return nil, awserr.ResourceNotFound("Requested resource not found")
	}
	iteratorType := strings.TrimSpace(req.ShardIteratorType)
	if iteratorType == "" {
		return nil, awserr.Validation("ShardIteratorType is required")
	}
	if (iteratorType == "TRIM_HORIZON" || iteratorType == "LATEST") && strings.TrimSpace(req.SequenceNumber) != "" {
		return nil, awserr.Validation("SequenceNumber is not valid for this ShardIteratorType")
	}

	last := int64(0)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
		if _, err := s.getTableByStreamARN(txCtx, tx, req.StreamARN); err != nil {
			return err
		}
		_, seqLast, found, err := s.store.GetStreamSequenceBounds(txCtx, tx, req.StreamARN)
		if err != nil {
			return err
		}
		if found {
			last = seqLast
		}
		return nil
	}); err != nil {
		return nil, err
	}

	sequence := int64(0)
	switch iteratorType {
	case "TRIM_HORIZON":
		sequence = 0
	case "LATEST":
		sequence = last
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		if strings.TrimSpace(req.SequenceNumber) == "" {
			return nil, awserr.Validation("SequenceNumber is required for this ShardIteratorType")
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(req.SequenceNumber), 10, 64)
		if err != nil || parsed < 0 {
			return nil, awserr.Validation("Invalid SequenceNumber")
		}
		if iteratorType == "AT_SEQUENCE_NUMBER" {
			sequence = parsed - 1
		} else {
			sequence = parsed
		}
	default:
		return nil, awserr.Validation("Invalid ShardIteratorType")
	}
	if sequence < 0 {
		sequence = 0
	}
	iter, err := s.encodeStreamIterator(streamIteratorToken{
		StreamARN: req.StreamARN,
		ShardID:   req.ShardID,
		Sequence:  sequence,
		ExpiresAt: time.Now().Add(s.streamIteratorTTL).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{"ShardIterator": iter}, nil
}

func (s *Server) getRecords(r *http.Request, body []byte) (map[string]any, error) {
	var req getRecordsRequest
	if err := decode(body, &req); err != nil {
		return nil, awserr.Validation(err.Error())
	}
	if strings.TrimSpace(req.ShardIterator) == "" {
		return nil, awserr.Validation("ShardIterator is required")
	}
	limit := req.Limit
	if limit < 0 {
		return nil, awserr.Validation("Limit must be greater than or equal to 0")
	}
	if limit <= 0 {
		limit = defaultStreamReadLimit
	}
	if limit > 1000 {
		return nil, awserr.Validation("Limit must be less than or equal to 1000")
	}
	token, err := s.decodeStreamIterator(req.ShardIterator)
	if err != nil {
		return nil, awserr.Validation("Invalid ShardIterator")
	}
	if token.ExpiresAt <= time.Now().UnixMilli() {
		return nil, awserr.ExpiredIterator("Shard iterator has expired")
	}

	var (
		records         []model.StreamRecord
		latestChangedAt int64
	)
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
		if _, err := s.getTableByStreamARN(txCtx, tx, token.StreamARN); err != nil {
			return err
		}
		var err error
		records, err = s.store.ListStreamRecordsAfterSequence(txCtx, tx, token.StreamARN, token.Sequence, limit)
		if err != nil {
			return err
		}
		_, last, found, err := s.store.GetStreamSequenceBounds(txCtx, tx, token.StreamARN)
		if err != nil {
			return err
		}
		if !found {
			last = token.Sequence
		}
		if found {
			latestChangedAt, _, err = s.store.GetStreamRecordChangedAt(txCtx, tx, token.StreamARN, last)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	outRecords := make([]map[string]any, 0, len(records))
	nextSeq := token.Sequence
	bytesUsed := 0
	for _, record := range records {
		recordSize := itemSizeOrZero(record.Keys) + itemSizeOrZero(record.OldImage) + itemSizeOrZero(record.NewImage)
		if bytesUsed+recordSize > streamResponseMaxBytes && len(outRecords) > 0 {
			break
		}
		outRecords = append(outRecords, streamRecordResponse(record))
		nextSeq = record.Sequence
		bytesUsed += recordSize
	}
	behind := int64(0)
	if latestChangedAt > 0 {
		if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
			tx, ok := uow.TxFromContext(txCtx)
			if !ok {
				return fmt.Errorf("missing transaction in unit of work context")
			}
			cursorChangedAt, cursorFound, err := s.store.GetStreamRecordChangedAt(txCtx, tx, token.StreamARN, nextSeq)
			if err != nil {
				return err
			}
			if !cursorFound {
				cursorChangedAt = time.Now().UnixMilli()
			}
			if latestChangedAt > cursorChangedAt {
				behind = latestChangedAt - cursorChangedAt
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	nextIterator, err := s.encodeStreamIterator(streamIteratorToken{
		StreamARN: token.StreamARN,
		ShardID:   token.ShardID,
		Sequence:  nextSeq,
		ExpiresAt: time.Now().Add(s.streamIteratorTTL).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"Records":            outRecords,
		"NextShardIterator":  nextIterator,
		"MillisBehindLatest": behind,
	}, nil
}

type streamIteratorEnvelope struct {
	Token streamIteratorToken `json:"token"`
	Sig   string              `json:"sig"`
}

func (s *Server) encodeStreamIterator(token streamIteratorToken) (string, error) {
	raw, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	sig := hmac.New(sha256.New, s.streamIteratorSigningKey)
	_, _ = sig.Write(raw)
	envelopeRaw, err := json.Marshal(streamIteratorEnvelope{
		Token: token,
		Sig:   hex.EncodeToString(sig.Sum(nil)),
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(envelopeRaw), nil
}

func (s *Server) decodeStreamIterator(raw string) (streamIteratorToken, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return streamIteratorToken{}, err
	}
	var envelope streamIteratorEnvelope
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		return streamIteratorToken{}, err
	}
	rawToken, err := json.Marshal(envelope.Token)
	if err != nil {
		return streamIteratorToken{}, err
	}
	sig := hmac.New(sha256.New, s.streamIteratorSigningKey)
	_, _ = sig.Write(rawToken)
	expectedSig := hex.EncodeToString(sig.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(envelope.Sig)) != 1 {
		return streamIteratorToken{}, fmt.Errorf("invalid shard iterator")
	}
	token := envelope.Token
	if strings.TrimSpace(token.StreamARN) == "" || strings.TrimSpace(token.ShardID) == "" {
		return streamIteratorToken{}, fmt.Errorf("invalid shard iterator")
	}
	return token, nil
}

func streamRecordResponse(record model.StreamRecord) map[string]any {
	dynamodbRecord := map[string]any{
		"ApproximateCreationDateTime": float64(record.ChangedAt) / 1000,
		"SequenceNumber":              strconv.FormatInt(record.Sequence, 10),
		"StreamViewType":              streamViewTypeForRecord(record),
		"Keys":                        record.Keys,
		"SizeBytes":                   itemSizeOrZero(record.Keys) + itemSizeOrZero(record.OldImage) + itemSizeOrZero(record.NewImage),
	}
	if record.NewImage != nil {
		dynamodbRecord["NewImage"] = record.NewImage
	}
	if record.OldImage != nil {
		dynamodbRecord["OldImage"] = record.OldImage
	}
	return map[string]any{
		"eventID":        strconv.FormatInt(record.Sequence, 10),
		"eventName":      record.EventName,
		"eventVersion":   "1.1",
		"eventSource":    "aws:dynamodb",
		"awsRegion":      "local",
		"eventSourceARN": record.StreamARN,
		"dynamodb":       dynamodbRecord,
	}
}

func itemSizeOrZero(item map[string]any) int {
	if item == nil {
		return 0
	}
	return model.CalculateItemSizeBytes(item)
}

func streamViewTypeForRecord(record model.StreamRecord) string {
	if record.NewImage != nil && record.OldImage != nil {
		return "NEW_AND_OLD_IMAGES"
	}
	if record.NewImage != nil {
		return "NEW_IMAGE"
	}
	if record.OldImage != nil {
		return "OLD_IMAGE"
	}
	return "KEYS_ONLY"
}

func (s *Server) getTableByStreamARN(ctx context.Context, tx *sql.Tx, streamARN string) (model.Table, error) {
	name, err := tableNameFromStreamARN(streamARN)
	if err != nil {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	t, err := s.getTableWithLifecycle(ctx, tx, name)
	if err != nil {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	if !t.Stream.Enabled || t.Stream.ARN != strings.TrimSpace(streamARN) {
		return model.Table{}, awserr.ResourceNotFound("Requested resource not found")
	}
	return t, nil
}

func tableNameFromStreamARN(streamARN string) (string, error) {
	streamARN = strings.TrimSpace(streamARN)
	if !strings.HasPrefix(streamARN, "arn:") {
		return "", fmt.Errorf("invalid stream arn")
	}
	marker := ":table/"
	start := strings.Index(streamARN, marker)
	if start < 0 {
		marker = "/table/"
		start = strings.Index(streamARN, marker)
	}
	if start < 0 {
		return "", fmt.Errorf("invalid stream arn")
	}
	remainder := streamARN[start+len(marker):]
	parts := strings.Split(remainder, "/")
	if len(parts) < 3 || parts[1] != "stream" {
		return "", fmt.Errorf("invalid stream arn")
	}
	if strings.TrimSpace(parts[0]) == "" {
		return "", fmt.Errorf("invalid stream arn")
	}
	return strings.TrimSpace(parts[0]), nil
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

func (s *Server) emitMutationEventForWrite(ctx context.Context, tx *sql.Tx, t model.Table, eventName string, keys, oldImage, newImage map[string]any, changedAt int64) error {
	pk, sk, err := primaryKeyStringsFromMutationKeys(t, keys)
	if err != nil {
		return err
	}
	return s.mutationExecutor.Emit(ctx, tx, mutation.Event{
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
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
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
		if txErr := repos.Items().PutItem(txCtx, t.Name, pk, sk, req.Item); txErr != nil {
			return txErr
		}
		eventName := "INSERT"
		streamOld := current
		if existed {
			eventName = "MODIFY"
		} else {
			streamOld = nil
		}
		return s.emitMutationEventForWrite(txCtx, tx, t, eventName, keyAttributesFromItem(t, req.Item), streamOld, req.Item, changedAt)
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
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
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
			return s.emitMutationEventForWrite(txCtx, tx, t, "REMOVE", keyAttributesFromItem(t, current), current, nil, changedAt)
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
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
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
		if txErr := repos.Items().PutItem(txCtx, t.Name, pk, sk, updated); txErr != nil {
			return txErr
		}
		eventName := "INSERT"
		streamOld := oldItem
		if itemExisted {
			eventName = "MODIFY"
		} else {
			streamOld = nil
		}
		return s.emitMutationEventForWrite(txCtx, tx, t, eventName, keyAttributesFromKey(t, req.Key), streamOld, updated, changedAt)
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
	tableKey, err := scopedTableNameFromIdentifier(r.Context(), req.TableName)
	if err != nil {
		return nil, err
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
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return nil, awserr.ResourceNotFound("Cannot do operations on a non-existent table")
		}
		var inUseErr *tableapp.TableInUseError
		if errors.As(err, &inUseErr) {
			return nil, awserr.ResourceInUse("Table is currently " + inUseErr.Status)
		}
		if strings.Contains(err.Error(), "GlobalSecondaryIndexUpdates") || strings.Contains(err.Error(), "index") || strings.Contains(err.Error(), "ProvisionedThroughput") {
			return nil, awserr.Validation(err.Error())
		}
		if strings.Contains(err.Error(), "SSE") || strings.Contains(err.Error(), "TableClass") || strings.Contains(err.Error(), "Stream") || strings.Contains(err.Error(), "DeletionProtection") {
			return nil, awserr.Validation(err.Error())
		}
		return nil, err
	}

	return map[string]any{"TableDescription": t.Description(count)}, nil
}

func (s *Server) refreshTableLifecycle(ctx context.Context, tx *sql.Tx, t *model.Table) error {
	repo := sqlTxTableRepo{store: s.store, tx: tx}
	if err := s.tableLifecycle.RefreshLifecycle(ctx, repo, t, lifecycleNow()); err != nil {
		if errors.Is(err, tableapp.ErrTableNotFound) {
			return sql.ErrNoRows
		}
		return err
	}
	return nil
}

func (s *Server) getTableWithLifecycle(ctx context.Context, tx *sql.Tx, tableName string) (model.Table, error) {
	scopedTableName, err := scopedTableNameFromIdentifier(ctx, tableName)
	if err != nil {
		return model.Table{}, err
	}
	return s.getTableWithLifecycleByKey(ctx, sqlTxTableRepo{store: s.store, tx: tx}, scopedTableName)
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
	scopedTableName, err := scopedTableNameFromIdentifier(ctx, tableName)
	if err != nil {
		return model.Table{}, err
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

type sqlTxTableRepo struct {
	store store.Store
	tx    *sql.Tx
}

func (r sqlTxTableRepo) CreateTable(ctx context.Context, table model.Table) error {
	return r.store.CreateTable(ctx, r.tx, table)
}
func (r sqlTxTableRepo) GetTable(ctx context.Context, tableKey string) (model.Table, error) {
	return r.store.GetTable(ctx, r.tx, tableKey)
}
func (r sqlTxTableRepo) ListTables(ctx context.Context, start string, limit int) ([]string, error) {
	return r.store.ListTables(ctx, r.tx, start, limit)
}
func (r sqlTxTableRepo) DeleteTable(ctx context.Context, tableKey string) error {
	return r.store.DeleteTable(ctx, r.tx, tableKey)
}
func (r sqlTxTableRepo) UpdateTableIndexes(ctx context.Context, tableKey string, tableStatus string, tableStatusAt int64, gsis []model.GlobalSecondaryIndex, lsis []model.LocalSecondaryIndex) error {
	return r.store.UpdateTableIndexes(ctx, r.tx, tableKey, tableStatus, tableStatusAt, gsis, lsis)
}
func (r sqlTxTableRepo) UpdateTableBilling(ctx context.Context, tableKey string, billingMode string, readCapacityUnits, writeCapacityUnits int64) error {
	return r.store.UpdateTableBilling(ctx, r.tx, tableKey, billingMode, readCapacityUnits, writeCapacityUnits)
}
func (r sqlTxTableRepo) UpdateTableOptions(ctx context.Context, tableKey string, tableClass string, deletionProtection bool, stream model.StreamSpecification, sse model.SSESpecification, tags []model.Tag) error {
	return r.store.UpdateTableOptions(ctx, r.tx, tableKey, tableClass, deletionProtection, stream, sse, tags)
}
func (r sqlTxTableRepo) UpdateTimeToLive(ctx context.Context, tableKey string, ttl model.TimeToLive) error {
	return r.store.UpdateTimeToLive(ctx, r.tx, tableKey, ttl)
}
func (r sqlTxTableRepo) UpdatePointInTimeRecovery(ctx context.Context, tableKey string, pitr model.PointInTimeRecovery) error {
	return r.store.UpdatePointInTimeRecovery(ctx, r.tx, tableKey, pitr)
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
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
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
					if err := s.emitMutationEventForWrite(txCtx, tx, t, eventName, keyAttributesFromItem(t, op.PutRequest.Item), streamOld, op.PutRequest.Item, time.Now().UnixMilli()); err != nil {
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
							if err := s.emitMutationEventForWrite(txCtx, tx, t, "REMOVE", keyAttributesFromKey(t, op.DeleteRequest.Key), current, nil, time.Now().UnixMilli()); err != nil {
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
		tx, ok := uow.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing transaction in unit of work context")
		}
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
				if err := s.emitMutationEventForWrite(txCtx, tx, t, eventName, keyAttributesFromItem(t, txItem.Put.Item), streamOld, txItem.Put.Item, time.Now().UnixMilli()); err != nil {
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
					if err := s.emitMutationEventForWrite(txCtx, tx, t, "REMOVE", keyAttributesFromKey(t, txItem.Delete.Key), current, nil, time.Now().UnixMilli()); err != nil {
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
				if err := s.emitMutationEventForWrite(txCtx, tx, t, eventName, keyAttributesFromKey(t, txItem.Update.Key), streamOld, updated, time.Now().UnixMilli()); err != nil {
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

func lifecycleNow() int64 {
	return time.Now().UnixMilli()
}

func lifecycleDelayMillis() int64 {
	raw := strings.TrimSpace(os.Getenv("PINAX_LIFECYCLE_DELAY_MS"))
	if raw == "" {
		return 1000
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 1000
	}
	return v
}

func enforceProvisionedLimits() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("PINAX_ENFORCE_PROVISIONED_LIMITS")), "true")
}

func pitrLatestRestorableLagMillisFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("PINAX_PITR_LATEST_RESTORABLE_LAG_MS"))
	if raw == "" {
		return 0
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

func (s *Server) ensureReadCapacity(t model.Table, units float64) error {
	if !s.reserveReadCapacity(t, units) {
		return awserr.ProvisionedThroughputExceeded("read capacity exceeded for table " + logicalTableNameFromKey(t.Name))
	}
	return nil
}

func (s *Server) ensureWriteCapacity(t model.Table, units float64) error {
	if !s.reserveWriteCapacity(t, units) {
		return awserr.ProvisionedThroughputExceeded("write capacity exceeded for table " + logicalTableNameFromKey(t.Name))
	}
	return nil
}

func (s *Server) reserveReadCapacity(t model.Table, units float64) bool {
	if !enforceProvisionedLimits() || t.BillingMode != "PROVISIONED" {
		return true
	}
	if units <= 0 || t.ReadCapacityUnits <= 0 {
		return true
	}
	return s.reserveCapacityUnits(t.Name, float64(t.ReadCapacityUnits), units, true)
}

func (s *Server) reserveWriteCapacity(t model.Table, units float64) bool {
	if !enforceProvisionedLimits() || t.BillingMode != "PROVISIONED" {
		return true
	}
	if units <= 0 || t.WriteCapacityUnits <= 0 {
		return true
	}
	return s.reserveCapacityUnits(t.Name, float64(t.WriteCapacityUnits), units, false)
}

func (s *Server) reserveCapacityUnits(tableName string, perSecondLimit float64, units float64, isRead bool) bool {
	if units <= 0 {
		return true
	}
	nowSec := time.Now().Unix()
	key := tableName
	if isRead {
		key += "|r"
	} else {
		key += "|w"
	}

	s.capMu.Lock()
	defer s.capMu.Unlock()

	window := s.capacityWindows[key]
	if window.second != nowSec {
		window = capacityWindow{second: nowSec}
	}
	used := window.writeUsed
	if isRead {
		used = window.readUsed
	}
	if used+units > perSecondLimit+0.00001 {
		s.capacityWindows[key] = window
		return false
	}
	if isRead {
		window.readUsed += units
	} else {
		window.writeUsed += units
	}
	s.capacityWindows[key] = window
	return true
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

func normalizeTableClass(tableClass string) (string, error) {
	tableClass = strings.TrimSpace(tableClass)
	if tableClass == "" {
		return "STANDARD", nil
	}
	switch tableClass {
	case "STANDARD", "STANDARD_INFREQUENT_ACCESS":
		return tableClass, nil
	default:
		return "", fmt.Errorf("unsupported TableClass %q", tableClass)
	}
}

func normalizeStreamSpec(spec *struct {
	StreamEnabled  bool   `json:"StreamEnabled"`
	StreamViewType string `json:"StreamViewType"`
}) (model.StreamSpecification, error) {
	if spec == nil {
		return model.StreamSpecification{}, nil
	}
	if !spec.StreamEnabled {
		if strings.TrimSpace(spec.StreamViewType) != "" {
			return model.StreamSpecification{}, fmt.Errorf("StreamViewType must not be set when StreamEnabled is false")
		}
		return model.StreamSpecification{Enabled: false}, nil
	}
	viewType := strings.TrimSpace(spec.StreamViewType)
	switch viewType {
	case "KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES":
		return model.StreamSpecification{Enabled: true, ViewType: viewType}, nil
	default:
		return model.StreamSpecification{}, fmt.Errorf("unsupported StreamViewType %q", spec.StreamViewType)
	}
}

func setStreamMetadata(spec *model.StreamSpecification, accountID string, tableName string) {
	if spec == nil || !spec.Enabled {
		return
	}
	if strings.TrimSpace(spec.Label) == "" {
		spec.Label = time.Now().UTC().Format("2006-01-02T15:04:05.000")
	}
	if strings.TrimSpace(spec.ARN) == "" {
		spec.ARN = fmt.Sprintf("arn:aws:dynamodb:local:%s:table/%s/stream/%s", accountID, tableName, spec.Label)
	}
}

func normalizeSSESpecCreate(spec *struct {
	Enabled        bool   `json:"Enabled"`
	SSEType        string `json:"SSEType"`
	KMSMasterKeyID string `json:"KMSMasterKeyId"`
}) (model.SSESpecification, error) {
	if spec == nil || !spec.Enabled {
		return model.SSESpecification{Enabled: false, Status: "DISABLED"}, nil
	}
	sseType := strings.TrimSpace(spec.SSEType)
	if sseType == "" {
		sseType = "AES256"
	}
	if sseType != "AES256" && sseType != "KMS" {
		return model.SSESpecification{}, fmt.Errorf("unsupported SSEType %q", spec.SSEType)
	}
	if sseType == "AES256" && strings.TrimSpace(spec.KMSMasterKeyID) != "" {
		return model.SSESpecification{}, fmt.Errorf("KMSMasterKeyId is only allowed when SSEType is KMS")
	}
	return model.SSESpecification{
		Enabled:        true,
		SSEType:        sseType,
		Status:         "ENABLED",
		KMSMasterKeyID: strings.TrimSpace(spec.KMSMasterKeyID),
	}, nil
}

func normalizeTags(in []struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}) ([]model.Tag, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]model.Tag, 0, len(in))
	seen := map[string]struct{}{}
	for _, tag := range in {
		key := strings.TrimSpace(tag.Key)
		if key == "" {
			return nil, fmt.Errorf("Tag Key is required")
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate tag key %q", key)
		}
		seen[key] = struct{}{}
		out = append(out, model.Tag{Key: key, Value: tag.Value})
	}
	return out, nil
}

func normalizeTagKeys(keys []string) ([]string, error) {
	out := make([]string, 0, len(keys))
	seen := map[string]struct{}{}
	for _, raw := range keys {
		key := strings.TrimSpace(raw)
		if key == "" {
			return nil, fmt.Errorf("TagKeys must not contain empty values")
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out, nil
}

func mergeTags(existing []model.Tag, updates []model.Tag) []model.Tag {
	merged := append([]model.Tag(nil), existing...)
	positions := make(map[string]int, len(merged))
	for i, tag := range merged {
		positions[tag.Key] = i
	}
	for _, tag := range updates {
		if idx, ok := positions[tag.Key]; ok {
			merged[idx].Value = tag.Value
			continue
		}
		positions[tag.Key] = len(merged)
		merged = append(merged, tag)
	}
	return merged
}

func removeTags(existing []model.Tag, keys []string) []model.Tag {
	if len(existing) == 0 || len(keys) == 0 {
		return existing
	}
	remove := map[string]struct{}{}
	for _, key := range keys {
		remove[key] = struct{}{}
	}
	out := make([]model.Tag, 0, len(existing))
	for _, tag := range existing {
		if _, ok := remove[tag.Key]; ok {
			continue
		}
		out = append(out, tag)
	}
	return out
}

func taggableTableNameFromResourceARN(resourceARN string) (string, string, error) {
	tableName, accountID, isARN, err := parseTableARN(resourceARN)
	if err != nil {
		return "", "", err
	}
	if !isARN {
		return "", "", fmt.Errorf("ResourceArn must be a valid ARN")
	}
	return tableName, accountID, nil
}

func normalizeGSIThroughputForCreate(billingMode string, throughput *struct {
	ReadCapacityUnits  int64 `json:"ReadCapacityUnits"`
	WriteCapacityUnits int64 `json:"WriteCapacityUnits"`
}) (int64, int64, error) {
	if strings.TrimSpace(billingMode) == "PROVISIONED" {
		if throughput == nil {
			return 0, 0, fmt.Errorf("ProvisionedThroughput is required for global secondary indexes when table BillingMode is PROVISIONED")
		}
		if throughput.ReadCapacityUnits <= 0 || throughput.WriteCapacityUnits <= 0 {
			return 0, 0, fmt.Errorf("ProvisionedThroughput ReadCapacityUnits and WriteCapacityUnits must be greater than 0")
		}
		return throughput.ReadCapacityUnits, throughput.WriteCapacityUnits, nil
	}
	if throughput != nil && (throughput.ReadCapacityUnits > 0 || throughput.WriteCapacityUnits > 0) {
		return 0, 0, fmt.Errorf("ProvisionedThroughput for global secondary indexes is only allowed when table BillingMode is PROVISIONED")
	}
	return 0, 0, nil
}

func applyUpdateTableOptions(t *model.Table, req updateTableRequest) error {
	if strings.TrimSpace(req.TableClass) != "" {
		tableClass, err := normalizeTableClass(req.TableClass)
		if err != nil {
			return err
		}
		t.TableClass = tableClass
	}
	if req.DeletionProtection != nil {
		t.DeletionProtection = *req.DeletionProtection
	}
	if req.StreamSpecification != nil {
		stream, err := normalizeStreamSpec(req.StreamSpecification)
		if err != nil {
			return err
		}
		if stream.Enabled {
			if !t.Stream.Enabled || t.Stream.ViewType != stream.ViewType {
				accountID, tableName := splitScopedTableKey(t.Name)
				setStreamMetadata(&stream, accountID, tableName)
			} else {
				stream.ARN = t.Stream.ARN
				stream.Label = t.Stream.Label
			}
		}
		t.Stream = stream
	}
	if req.SSESpecification != nil {
		next := t.SSE
		if req.SSESpecification.Enabled != nil {
			next.Enabled = *req.SSESpecification.Enabled
		}
		if !next.Enabled {
			next.Status = "DISABLED"
			next.SSEType = ""
			next.KMSMasterKeyID = ""
		} else {
			next.Status = "ENABLED"
			if strings.TrimSpace(req.SSESpecification.SSEType) != "" {
				next.SSEType = strings.TrimSpace(req.SSESpecification.SSEType)
			}
			if next.SSEType == "" {
				next.SSEType = "AES256"
			}
			if next.SSEType != "AES256" && next.SSEType != "KMS" {
				return fmt.Errorf("unsupported SSEType %q", req.SSESpecification.SSEType)
			}
			if strings.TrimSpace(req.SSESpecification.KMSMasterKeyID) != "" {
				next.KMSMasterKeyID = strings.TrimSpace(req.SSESpecification.KMSMasterKeyID)
			}
			if next.SSEType == "AES256" {
				next.KMSMasterKeyID = ""
			}
		}
		t.SSE = next
	}
	return nil
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

func accountIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(authentication.AccountIDContextKey{}).(string); ok {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			return trimmed
		}
	}
	return defaultLocalAccountID
}

func scopedTableKeyFromAccountAndName(accountID string, tableName string) string {
	return strings.TrimSpace(accountID) + scopedTableKeySeparator + strings.TrimSpace(tableName)
}

func splitScopedTableKey(v string) (string, string) {
	v = strings.TrimSpace(v)
	parts := strings.SplitN(v, scopedTableKeySeparator, 2)
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return defaultLocalAccountID, v
}

func normalizeTableNameIdentifier(v string) string {
	name, _, _, err := parseTableARN(v)
	if err == nil {
		return name
	}
	return strings.TrimSpace(v)
}

func parseTableARN(v string) (tableName string, accountID string, isARN bool, err error) {
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, "arn:") {
		return strings.TrimSpace(v), "", false, nil
	}
	parts := strings.SplitN(v, ":", 6)
	if len(parts) < 6 {
		return "", "", true, fmt.Errorf("ResourceArn must be a valid ARN")
	}
	accountID = strings.TrimSpace(parts[4])
	resource := strings.TrimSpace(parts[5])
	if !strings.HasPrefix(resource, "table/") {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	remainder := strings.TrimPrefix(resource, "table/")
	if remainder == "" {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	tableName = remainder
	if slash := strings.Index(tableName, "/"); slash >= 0 {
		tableName = tableName[:slash]
	}
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return "", "", true, fmt.Errorf("ResourceArn must identify a DynamoDB table")
	}
	return tableName, accountID, true, nil
}

func scopedTableNameFromIdentifier(ctx context.Context, tableIdentifier string) (string, error) {
	tableName, arnAccountID, isARN, err := parseTableARN(tableIdentifier)
	if err != nil {
		return "", awserr.Validation(err.Error())
	}
	accountID := accountIDFromContext(ctx)
	if isARN && arnAccountID != "" && arnAccountID != accountID {
		return "", &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	if strings.TrimSpace(tableName) == "" {
		return "", awserr.Validation("TableName is required")
	}
	return scopedTableKeyFromAccountAndName(accountID, tableName), nil
}

func validateTableARNAccountForRequest(ctx context.Context, resourceARN string) (string, error) {
	tableName, arnAccountID, isARN, err := parseTableARN(resourceARN)
	if err != nil {
		return "", awserr.Validation(err.Error())
	}
	if !isARN {
		return "", awserr.Validation("ResourceArn must be a valid ARN")
	}
	if arnAccountID != "" && arnAccountID != accountIDFromContext(ctx) {
		return "", &awserr.APIError{Code: "AccessDeniedException", Message: "Access denied", Status: http.StatusBadRequest}
	}
	return tableName, nil
}

func logicalTableNameFromKey(v string) string {
	_, tableName := splitScopedTableKey(v)
	return tableName
}

func localBackupARN(tableName string, backupName string, createdAt int64) string {
	accountID, logicalName := splitScopedTableKey(tableName)
	h := fnv.New64a()
	_, _ = h.Write([]byte(tableName + "|" + backupName + "|" + strconv.FormatInt(createdAt, 10)))
	return fmt.Sprintf("arn:aws:dynamodb:local:%s:table/%s/backup/%016x", accountID, logicalName, h.Sum64())
}

func anyString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func estimateBackupSizeBytes(items []map[string]any) int64 {
	var total int64
	for _, item := range items {
		total += int64(model.CalculateItemSizeBytes(item))
	}
	return total
}

func sourceTableDetailsFromDescription(tableDescription map[string]any, itemCount int64) map[string]any {
	resp := map[string]any{
		"TableName":             tableDescription["TableName"],
		"TableArn":              tableDescription["TableArn"],
		"TableId":               tableDescription["TableId"],
		"TableCreationDateTime": tableDescription["CreationDateTime"],
		"KeySchema":             tableDescription["KeySchema"],
		"TableSizeBytes":        tableDescription["TableSizeBytes"],
		"ItemCount":             itemCount,
	}
	if billingModeSummary, ok := tableDescription["BillingModeSummary"].(map[string]any); ok {
		resp["BillingMode"] = billingModeSummary["BillingMode"]
	}
	if throughput, ok := tableDescription["ProvisionedThroughput"].(map[string]any); ok {
		resp["ProvisionedThroughput"] = map[string]any{
			"ReadCapacityUnits":  throughput["ReadCapacityUnits"],
			"WriteCapacityUnits": throughput["WriteCapacityUnits"],
		}
	}
	return resp
}

func sourceTableFeatureDetailsFromDescription(tableDescription map[string]any) map[string]any {
	resp := map[string]any{}
	if gsis, ok := tableDescription["GlobalSecondaryIndexes"]; ok {
		resp["GlobalSecondaryIndexes"] = gsis
	}
	if lsis, ok := tableDescription["LocalSecondaryIndexes"]; ok {
		resp["LocalSecondaryIndexes"] = lsis
	}
	if stream, ok := tableDescription["StreamSpecification"]; ok {
		resp["StreamDescription"] = stream
	}
	if sse, ok := tableDescription["SSEDescription"]; ok {
		resp["SSEDescription"] = sse
	}
	if ttl, ok := tableDescription["TimeToLive"]; ok {
		resp["TimeToLiveDescription"] = ttl
	}
	return resp
}

func backupDetailsMap(backup model.Backup) map[string]any {
	return map[string]any{
		"BackupArn":              backup.BackupARN,
		"BackupName":             backup.BackupName,
		"BackupStatus":           backup.BackupStatus,
		"BackupType":             backup.BackupType,
		"BackupCreationDateTime": backup.BackupCreationDateTime,
		"BackupSizeBytes":        backup.BackupSizeBytes,
	}
}

func backupSummaryMap(backup model.Backup) map[string]any {
	out := backupDetailsMap(backup)
	out["TableName"] = backup.TableName
	out["TableArn"] = backup.TableARN
	out["TableId"] = backup.TableID
	return out
}

func backupDescriptionMap(backup model.Backup) map[string]any {
	return map[string]any{
		"BackupDetails":             backupDetailsMap(backup),
		"SourceTableDetails":        backup.SourceTableDetails,
		"SourceTableFeatureDetails": backup.SourceTableFeatureDetails,
	}
}

func applyRestoreGSIOverride(source []model.GlobalSecondaryIndex, overrides []struct {
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
}, billingMode string) ([]model.GlobalSecondaryIndex, error) {
	if len(overrides) == 0 {
		return source, nil
	}
	byName := map[string]model.GlobalSecondaryIndex{}
	for _, g := range source {
		byName[g.IndexName] = g
	}
	out := make([]model.GlobalSecondaryIndex, 0, len(overrides))
	for _, o := range overrides {
		if strings.TrimSpace(o.IndexName) == "" {
			return nil, fmt.Errorf("GlobalSecondaryIndexOverride IndexName is required")
		}
		g, ok := byName[o.IndexName]
		if !ok {
			return nil, fmt.Errorf("GlobalSecondaryIndexOverride contains unknown index %q", o.IndexName)
		}
		if len(o.KeySchema) > 0 {
			if !gsiKeySchemaMatches(g, o.KeySchema) {
				return nil, fmt.Errorf("GlobalSecondaryIndexOverride KeySchema must match source index %q", o.IndexName)
			}
		}
		if strings.TrimSpace(o.Projection.ProjectionType) != "" || len(o.Projection.NonKeyAttributes) > 0 {
			projType, attrs, err := normalizeIndexProjection(o.Projection.ProjectionType, o.Projection.NonKeyAttributes)
			if err != nil {
				return nil, err
			}
			if projType != g.ProjectionType || !stringSlicesEqual(attrs, g.NonKeyAttrs) {
				return nil, fmt.Errorf("GlobalSecondaryIndexOverride Projection must match source index %q", o.IndexName)
			}
		}
		if o.ProvisionedThroughput != nil {
			if billingMode != "PROVISIONED" {
				return nil, fmt.Errorf("ProvisionedThroughput for global secondary indexes is only allowed when table BillingMode is PROVISIONED")
			}
			if o.ProvisionedThroughput.ReadCapacityUnits <= 0 || o.ProvisionedThroughput.WriteCapacityUnits <= 0 {
				return nil, fmt.Errorf("ProvisionedThroughput ReadCapacityUnits and WriteCapacityUnits must be greater than 0")
			}
			g.ReadCapacity = o.ProvisionedThroughput.ReadCapacityUnits
			g.WriteCapacity = o.ProvisionedThroughput.WriteCapacityUnits
		}
		out = append(out, g)
	}
	return out, nil
}

func applyRestoreLSIOverride(source []model.LocalSecondaryIndex, overrides []struct {
	IndexName string `json:"IndexName"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType       string `json:"KeyType"`
	} `json:"KeySchema"`
	Projection struct {
		ProjectionType   string   `json:"ProjectionType"`
		NonKeyAttributes []string `json:"NonKeyAttributes"`
	} `json:"Projection"`
}) ([]model.LocalSecondaryIndex, error) {
	if len(overrides) == 0 {
		return source, nil
	}
	byName := map[string]model.LocalSecondaryIndex{}
	for _, l := range source {
		byName[l.IndexName] = l
	}
	out := make([]model.LocalSecondaryIndex, 0, len(overrides))
	for _, o := range overrides {
		if strings.TrimSpace(o.IndexName) == "" {
			return nil, fmt.Errorf("LocalSecondaryIndexOverride IndexName is required")
		}
		l, ok := byName[o.IndexName]
		if !ok {
			return nil, fmt.Errorf("LocalSecondaryIndexOverride contains unknown index %q", o.IndexName)
		}
		if len(o.KeySchema) > 0 {
			if !lsiKeySchemaMatches(l, o.KeySchema) {
				return nil, fmt.Errorf("LocalSecondaryIndexOverride KeySchema must match source index %q", o.IndexName)
			}
		}
		if strings.TrimSpace(o.Projection.ProjectionType) != "" || len(o.Projection.NonKeyAttributes) > 0 {
			projType, attrs, err := normalizeIndexProjection(o.Projection.ProjectionType, o.Projection.NonKeyAttributes)
			if err != nil {
				return nil, err
			}
			if projType != l.ProjectionType || !stringSlicesEqual(attrs, l.NonKeyAttrs) {
				return nil, fmt.Errorf("LocalSecondaryIndexOverride Projection must match source index %q", o.IndexName)
			}
		}
		out = append(out, l)
	}
	return out, nil
}

func gsiKeySchemaMatches(g model.GlobalSecondaryIndex, keySchema []struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}) bool {
	expected := map[string]string{"HASH": g.HashKey}
	if g.RangeKey != "" {
		expected["RANGE"] = g.RangeKey
	}
	if len(keySchema) != len(expected) {
		return false
	}
	for _, ks := range keySchema {
		if expected[strings.TrimSpace(ks.KeyType)] != strings.TrimSpace(ks.AttributeName) {
			return false
		}
	}
	return true
}

func lsiKeySchemaMatches(l model.LocalSecondaryIndex, keySchema []struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"`
}) bool {
	if len(keySchema) != 2 {
		return false
	}
	hashMatch := false
	rangeMatch := false
	for _, ks := range keySchema {
		switch strings.TrimSpace(ks.KeyType) {
		case "HASH":
			hashMatch = strings.TrimSpace(ks.AttributeName) != ""
		case "RANGE":
			rangeMatch = strings.TrimSpace(ks.AttributeName) == l.RangeKey
		}
	}
	return hashMatch && rangeMatch
}

func stringSlicesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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

	ttl := model.TimeToLive{
		Enabled:  req.TimeToLiveSpecification.Enabled,
		AttrName: req.TimeToLiveSpecification.AttributeName,
		StatusAt: lifecycleNow() + lifecycleDelayMillis(),
	}
	if ttl.Enabled {
		ttl.Status = model.TTLStatusEnabling
	} else {
		ttl.Status = model.TTLStatusDisabling
	}
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		t, err := s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		return repos.Tables().UpdateTimeToLive(txCtx, t.Name, ttl)
	}); err != nil {
		return nil, err
	}
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

	var t model.Table
	if err := s.unitOfWork.Do(r.Context(), func(txCtx context.Context, repos uow.Repos) error {
		var err error
		t, err = s.getTableWithLifecycleFromRepo(txCtx, repos.Tables(), req.TableName)
		if err != nil {
			return err
		}
		now := lifecycleNow()
		if t.TimeToLive.Status == model.TTLStatusEnabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
			t.TimeToLive.Status = model.TTLStatusEnabled
			t.TimeToLive.StatusAt = 0
			t.TimeToLive.Enabled = true
			if err := repos.Tables().UpdateTimeToLive(txCtx, t.Name, t.TimeToLive); err != nil {
				return err
			}
		}
		if t.TimeToLive.Status == model.TTLStatusDisabling && t.TimeToLive.StatusAt > 0 && now >= t.TimeToLive.StatusAt {
			t.TimeToLive.Status = model.TTLStatusDisabled
			t.TimeToLive.StatusAt = 0
			t.TimeToLive.Enabled = false
			if err := repos.Tables().UpdateTimeToLive(txCtx, t.Name, t.TimeToLive); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
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
		"TableName": logicalTableNameFromKey(tableName),
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
	entry := map[string]any{"TableName": logicalTableNameFromKey(tableName)}
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
		"TableName":         logicalTableNameFromKey(tableName),
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
		"TableName":         logicalTableNameFromKey(tableName),
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

func cloneExpressionValues(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func normalizeQuerySelect(req queryRequest, hasIndex bool, isGSI bool) (string, string, error) {
	projectionExpression, err := normalizeLegacyAttributesProjection(req.AttributesToGet, req.ProjectionExpression)
	if err != nil {
		return "", "", err
	}

	selectMode := strings.ToUpper(strings.TrimSpace(req.Select))
	if selectMode == "" {
		switch {
		case projectionExpression != "":
			selectMode = "SPECIFIC_ATTRIBUTES"
		case hasIndex:
			selectMode = "ALL_PROJECTED_ATTRIBUTES"
		default:
			selectMode = "ALL_ATTRIBUTES"
		}
	}

	switch selectMode {
	case "ALL_ATTRIBUTES", "ALL_PROJECTED_ATTRIBUTES", "SPECIFIC_ATTRIBUTES", "COUNT":
	default:
		return "", "", fmt.Errorf("unsupported Query Select value %q", req.Select)
	}

	if projectionExpression != "" && selectMode != "SPECIFIC_ATTRIBUTES" {
		return "", "", fmt.Errorf("Select can only be SPECIFIC_ATTRIBUTES when ProjectionExpression or AttributesToGet is set")
	}
	if selectMode == "SPECIFIC_ATTRIBUTES" && projectionExpression == "" {
		return "", "", fmt.Errorf("ProjectionExpression or AttributesToGet is required when Select is SPECIFIC_ATTRIBUTES")
	}
	if selectMode == "ALL_PROJECTED_ATTRIBUTES" && !hasIndex {
		return "", "", fmt.Errorf("ALL_PROJECTED_ATTRIBUTES is only valid when querying an index")
	}
	if selectMode == "ALL_ATTRIBUTES" && isGSI {
		return "", "", fmt.Errorf("ALL_ATTRIBUTES is not supported when querying a global secondary index")
	}

	return selectMode, projectionExpression, nil
}

func normalizeLegacyAttributesProjection(attributesToGet []string, projectionExpression string) (string, error) {
	projectionExpression = strings.TrimSpace(projectionExpression)
	if len(attributesToGet) == 0 {
		return projectionExpression, nil
	}
	if projectionExpression != "" {
		return "", fmt.Errorf("AttributesToGet and ProjectionExpression cannot both be set")
	}
	attrs := make([]string, 0, len(attributesToGet))
	seen := map[string]struct{}{}
	for _, attr := range attributesToGet {
		attr = strings.TrimSpace(attr)
		if attr == "" {
			continue
		}
		if _, ok := seen[attr]; ok {
			continue
		}
		seen[attr] = struct{}{}
		attrs = append(attrs, attr)
	}
	return strings.Join(attrs, ", "), nil
}

func parseLegacyQueryKeyConditions(
	keyConditions map[string]struct {
		AttributeValueList []any  `json:"AttributeValueList"`
		ComparisonOperator string `json:"ComparisonOperator"`
	},
	targetHashKey string,
	targetRangeKey string,
	expressionValues map[string]any,
) (keyExprToken, *sortKeyCondition, map[string]any, error) {
	if len(keyConditions) == 0 {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("KeyConditionExpression is required")
	}

	for attr := range keyConditions {
		if attr != targetHashKey && attr != targetRangeKey {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("legacy KeyConditions may only include HASH and RANGE key attributes")
		}
	}

	hashCond, ok := keyConditions[targetHashKey]
	if !ok {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition must target HASH key")
	}
	if strings.ToUpper(strings.TrimSpace(hashCond.ComparisonOperator)) != "EQ" {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition must use '='")
	}
	if len(hashCond.AttributeValueList) != 1 {
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("partition key condition requires exactly one value")
	}

	pkToken := nextLegacyToken(expressionValues, ":__legacy_pk")
	expressionValues[pkToken] = hashCond.AttributeValueList[0]
	pk := keyExprToken{attr: targetHashKey, value: pkToken}

	if targetRangeKey == "" {
		if _, exists := keyConditions[targetRangeKey]; exists {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition is not supported for tables without a RANGE key")
		}
		return pk, nil, expressionValues, nil
	}

	rangeCond, ok := keyConditions[targetRangeKey]
	if !ok {
		return pk, nil, expressionValues, nil
	}

	op := strings.ToUpper(strings.TrimSpace(rangeCond.ComparisonOperator))
	sk := &sortKeyCondition{attr: targetRangeKey}
	valueToken1 := nextLegacyToken(expressionValues, ":__legacy_sk")

	switch op {
	case "EQ", "LT", "LE", "GT", "GE", "BEGINS_WITH":
		if len(rangeCond.AttributeValueList) != 1 {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition %s requires exactly one value", op)
		}
		expressionValues[valueToken1] = rangeCond.AttributeValueList[0]
		switch op {
		case "EQ":
			sk.op = "="
		case "LT":
			sk.op = "<"
		case "LE":
			sk.op = "<="
		case "GT":
			sk.op = ">"
		case "GE":
			sk.op = ">="
		case "BEGINS_WITH":
			sk.op = "begins_with"
		}
		sk.value1 = valueToken1
	case "BETWEEN":
		if len(rangeCond.AttributeValueList) != 2 {
			return keyExprToken{}, nil, expressionValues, fmt.Errorf("sort key condition BETWEEN requires exactly two values")
		}
		expressionValues[valueToken1] = rangeCond.AttributeValueList[0]
		valueToken2 := nextLegacyToken(expressionValues, ":__legacy_sk")
		expressionValues[valueToken2] = rangeCond.AttributeValueList[1]
		sk.op = "BETWEEN"
		sk.value1 = valueToken1
		sk.value2 = valueToken2
	default:
		return keyExprToken{}, nil, expressionValues, fmt.Errorf("unsupported sort key condition")
	}

	return pk, sk, expressionValues, nil
}

func scanSegmentForPK(serializedPK string, totalSegments int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(serializedPK))
	return int(h.Sum32() % uint32(totalSegments))
}

func nextLegacyToken(values map[string]any, prefix string) string {
	if _, exists := values[prefix]; !exists {
		return prefix
	}
	for i := 1; ; i++ {
		token := fmt.Sprintf("%s_%d", prefix, i)
		if _, exists := values[token]; !exists {
			return token
		}
	}
}

func expressionValidationMessage(expressionName string, err error) string {
	msg := err.Error()
	if strings.HasPrefix(msg, "missing expression value ") {
		token := strings.TrimPrefix(msg, "missing expression value ")
		token = strings.Trim(token, "\"")
		return "Invalid " + expressionName + ": An expression attribute value used in expression is not defined; attribute value: " + token
	}
	if strings.HasPrefix(msg, "missing expression name ") {
		token := strings.TrimPrefix(msg, "missing expression name ")
		token = strings.Trim(token, "\"")
		return "Invalid " + expressionName + ": An expression attribute name used in the document path is not defined; attribute name: " + token
	}
	return msg
}

func filterExpressionValidationMessage(err error) string {
	return expressionValidationMessage("FilterExpression", err)
}

func conditionExpressionValidationMessage(err error) string {
	return expressionValidationMessage("ConditionExpression", err)
}
