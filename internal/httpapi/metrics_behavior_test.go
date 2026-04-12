package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestMetricsFailureCountersIncrement(t *testing.T) {
	t.Setenv("PINAX_ENFORCE_PROVISIONED_LIMITS", "true")

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	apiSrv := httptest.NewServer(newTestServer(backend, nil))
	t.Cleanup(apiSrv.Close)
	monitoringSrv := httptest.NewServer(NewMonitoringHandler(db))
	t.Cleanup(monitoringSrv.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(apiSrv.URL) })

	before, err := readMetrics(monitoringSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	beforeConditional := metricValue(before, "pinax_conditional_check_failures_total", map[string]string{"operation": "UpdateItem"})
	beforeThrottle := metricValue(before, "pinax_throttling_failures_total", map[string]string{"operation": "GetItem"})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("m_cond"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("m_cond"), Item: map[string]types.AttributeValue{
		"pk":      &types.AttributeValueMemberS{Value: "a"},
		"version": &types.AttributeValueMemberN{Value: "1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String("m_cond"),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		UpdateExpression:          aws.String("SET #v = #v + :one"),
		ConditionExpression:       aws.String("#v = :bad"),
		ExpressionAttributeNames:  map[string]string{"#v": "version"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}, ":bad": &types.AttributeValueMemberN{Value: "999"}},
	})
	if err == nil {
		t.Fatal("expected conditional failure")
	}
	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		t.Fatalf("expected ConditionalCheckFailedException, got %T: %v", err, err)
	}

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("m_throttle"),
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(10),
		},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("m_throttle"), Item: map[string]types.AttributeValue{
		"pk":      &types.AttributeValueMemberS{Value: "a"},
		"payload": &types.AttributeValueMemberS{Value: strings.Repeat("x", 6000)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String("m_throttle"),
		Key:            map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		ConsistentRead: aws.Bool(true),
	})
	if err == nil {
		t.Fatal("expected throttling failure")
	}
	var throttleErr *types.ProvisionedThroughputExceededException
	if !errors.As(err, &throttleErr) {
		t.Fatalf("expected ProvisionedThroughputExceededException, got %T: %v", err, err)
	}

	after, err := readMetrics(monitoringSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	afterConditional := metricValue(after, "pinax_conditional_check_failures_total", map[string]string{"operation": "UpdateItem"})
	afterThrottle := metricValue(after, "pinax_throttling_failures_total", map[string]string{"operation": "GetItem"})

	if afterConditional < beforeConditional+1 {
		t.Fatalf("expected conditional failure counter to increase, before=%v after=%v", beforeConditional, afterConditional)
	}
	if afterThrottle < beforeThrottle+1 {
		t.Fatalf("expected throttling failure counter to increase, before=%v after=%v", beforeThrottle, afterThrottle)
	}
}

func readMetrics(baseURL string) (string, error) {
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected metrics status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func metricValue(metrics string, metricName string, labels map[string]string) float64 {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(labels))
	for _, k := range keys {
		v := labels[k]
		parts = append(parts, fmt.Sprintf(`%s="%s"`, regexp.QuoteMeta(k), regexp.QuoteMeta(v)))
	}
	pattern := fmt.Sprintf(`(?m)^%s\{[^\n]*%s[^\n]*\}\s+([0-9eE+\-.]+)$`, regexp.QuoteMeta(metricName), strings.Join(parts, `[^\n]*`))
	re := regexp.MustCompile(pattern)
	m := re.FindStringSubmatch(metrics)
	if len(m) != 2 {
		return 0
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	return v
}
