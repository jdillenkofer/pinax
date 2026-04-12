package httpapi

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	backupapp "github.com/jdillenkofer/pinax/internal/app/backup"
	itemopsapp "github.com/jdillenkofer/pinax/internal/app/itemops"
	pitrapp "github.com/jdillenkofer/pinax/internal/app/pitr"
	queryapp "github.com/jdillenkofer/pinax/internal/app/query"
	resourcepolicyapp "github.com/jdillenkofer/pinax/internal/app/resourcepolicy"
	approot "github.com/jdillenkofer/pinax/internal/app/root"
	tableapp "github.com/jdillenkofer/pinax/internal/app/table"
	transactionapp "github.com/jdillenkofer/pinax/internal/app/transaction"
	"github.com/jdillenkofer/pinax/internal/app/uow"
	"github.com/jdillenkofer/pinax/internal/awserr"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	"github.com/jdillenkofer/pinax/internal/identity"
	"github.com/jdillenkofer/pinax/internal/model"
	"github.com/jdillenkofer/pinax/internal/mutation"
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
	unitOfWork                    uow.UnitOfWork
	tableLifecycle                *tableapp.LifecycleService
	tableService                  *tableapp.Service
	queryService                  *queryapp.Service
	itemOpsService                *itemopsapp.Service
	transactionService            *transactionapp.Service
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

func NewServer(unitOfWork uow.UnitOfWork, requestAuthorizer authorization.RequestAuthorizer, opts ...ServerOption) *Server {
	s := &Server{
		unitOfWork:                    unitOfWork,
		requestAuthorizer:             requestAuthorizer,
		mutationExecutor:              mutation.NewExecutor(),
		capacityWindows:               map[string]capacityWindow{},
		pitrLatestRestorableLagMillis: pitrLatestRestorableLagMillisFromEnv(),
		streamIteratorTTL:             defaultStreamIteratorTTL,
		streamIteratorSigningKey:      newStreamIteratorSigningKey(),
	}
	services := approot.NewServices(unitOfWork, s.getActiveTableFromRepo)
	s.tableLifecycle = services.TableLifecycle
	s.tableService = services.TableService
	s.queryService = services.QueryService
	s.itemOpsService = services.ItemOpsService
	s.transactionService = services.TransactionService
	s.backupService = services.BackupService
	s.pitrService = services.PITRService
	s.resourcePolicyService = services.ResourcePolicyService
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
	db             *sql.DB
	metricsHandler http.Handler
}

func NewMonitoringHandler(db *sql.DB) http.Handler {
	return &monitoringHandler{db: db, metricsHandler: promhttp.Handler()}
}

func (h *monitoringHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	if r.URL.Path == "/health" {
		if err := h.db.PingContext(r.Context()); err != nil {
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
	tableName, accountID, isARN, err := identity.ParseTableARN(resourceARN)
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
	return identity.DefaultLocalAccountID
}

func scopedTableKeyFromAccountAndName(accountID string, tableName string) string {
	return identity.ScopedTableKey(accountID, tableName)
}

func splitScopedTableKey(v string) (string, string) {
	return identity.SplitScopedTableKey(v)
}

func parseTableARN(v string) (tableName string, accountID string, isARN bool, err error) {
	return identity.ParseTableARN(v)
}

func logicalTableNameFromKey(v string) string {
	return identity.LogicalTableNameFromKey(v)
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
