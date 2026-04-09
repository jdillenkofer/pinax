package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

type conformanceSnapshot struct {
	QueryOrder               []string
	QueryPage1Count          int32
	QueryPage2Count          int32
	BatchProjectionHasShown  bool
	BatchProjectionHasHidden bool
	ConditionalErrorCode     string
	ConditionalErrorHasMsg   bool
	BatchGetDupHasMsg        bool
	BatchWriteDupHasMsg      bool
	TransactionErrorHasMsg   bool
	TransactionReasonCount   int
	FilteredScanCount        int32
	FilteredScanScanned      int32
	FilteredScanHasLEK       bool
	ComplexFilterCount       int32
	NestedPathFilterCount    int32
	IfNotExistsInitialized   bool
	ListAppendLength         int
	CreateTableStatus        types.TableStatus
	UpdateTableStatus        types.TableStatus
	BatchGetDupErrorCode     string
	BatchWriteDupErrorCode   string
	TransactionErrorCode     string
	TransactionReasonCodes   []string
	TransactionReasonMsgs    []string
	GetHasConsumedCapacity   bool
	BatchGetConsumedCount    int
	QueryHasConsumedCapacity bool
	ScanHasConsumedCapacity  bool
	TransactGetConsumedCount int
	TransactWriteConsumedLen int
	GSIIncludeHasSummary     bool
	GSIIncludeHasHidden      bool
	LSIKeysOnlyHasNonKey     bool
	TTLStatusRecognized      bool
	TTLHasAttributeName      bool
	CRCDescribeTableValid    bool
	CRCConditionalErrValid   bool
	CRCTxCanceledErrValid    bool
	ExprValidationCode       string
	ExprValidationMessage    string
	ExprSyntaxCode           string
	ExprSyntaxMessage        string
	TxValidationCode         string
	TxValidationMessage      string
	TxConflictCode           string
	TxConflictMessage        string
	TxItemSizeCode           string
	TxItemSizeMessage        string
}

type knownConformanceDifferences struct {
	Fields map[string]string `json:"fields"`
}

type operationErrorParitySnapshot struct {
	PutWrongTypeCode       string
	PutWrongTypeMessage    string
	GetWrongTypeCode       string
	GetWrongTypeMessage    string
	DeleteWrongTypeCode    string
	DeleteWrongTypeMessage string
	QueryWrongTypeCode     string
	QueryWrongTypeMessage  string
	ScanMissingValueCode   string
	ScanMissingValueMsg    string
	UpdateSyntaxCode       string
	UpdateSyntaxMessage    string
	BatchGetDupCode        string
	BatchGetDupMessage     string
	BatchWriteDupCode      string
	BatchWriteDupMessage   string
}

func TestConformanceAgainstDynamoDBLocal(t *testing.T) {
	testutils.SkipIfNotIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	localEndpoint := os.Getenv("PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT")
	if localEndpoint == "" {
		t.Skip("set PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT (for example http://localhost:8000) to run conformance test")
	}

	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	pinaxSrv := httptest.NewServer(NewServer(store, nil))
	t.Cleanup(pinaxSrv.Close)

	pinaxClient := mustConformanceClient(ctx, t, pinaxSrv.URL)
	localClient := mustConformanceClient(ctx, t, localEndpoint)

	pinaxSnap := runConformanceScenario(ctx, t, pinaxClient, "pinax")
	localSnap := runConformanceScenario(ctx, t, localClient, "local")
	ignored := loadKnownConformanceDifferences(t)
	mismatches, ignoredMismatches := compareConformanceSnapshots(pinaxSnap, localSnap, ignored)
	for _, ignoredMsg := range ignoredMismatches {
		t.Logf("known conformance difference: %s", ignoredMsg)
	}

	if len(mismatches) > 0 {
		t.Fatalf("conformance mismatches:\n%s\n\npinax: %+v\nlocal: %+v", strings.Join(mismatches, "\n"), pinaxSnap, localSnap)
	}
}

func TestOperationErrorParityAgainstDynamoDBLocal(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

	localEndpoint := os.Getenv("PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT")
	if localEndpoint == "" {
		t.Skip("set PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT (for example http://localhost:8000) to run operation error parity test")
	}

	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	pinaxSrv := httptest.NewServer(NewServer(store, nil))
	t.Cleanup(pinaxSrv.Close)

	pinaxClient := mustConformanceClient(ctx, t, pinaxSrv.URL)
	localClient := mustConformanceClient(ctx, t, localEndpoint)

	pinax := runOperationErrorParityScenario(ctx, t, pinaxClient.ddb, "pinax")
	local := runOperationErrorParityScenario(ctx, t, localClient.ddb, "local")

	if !reflect.DeepEqual(pinax, local) {
		t.Fatalf("operation error parity mismatches\npinax: %+v\nlocal: %+v", pinax, local)
	}
}

type conformanceStressSnapshot struct {
	TotalItems   int
	PageCount    int
	ScannedCount int32
}

func TestConformanceStressAgainstDynamoDBLocal(t *testing.T) {
	testutils.SkipIfNotIntegration(t)
	if os.Getenv("PINAX_CONFORMANCE_STRESS") != "1" {
		t.Skip("set PINAX_CONFORMANCE_STRESS=1 to run stress conformance scenario")
	}

	localEndpoint := os.Getenv("PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT")
	if localEndpoint == "" {
		t.Skip("set PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT (for example http://localhost:8000) to run conformance test")
	}

	ctx := context.Background()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	pinaxSrv := httptest.NewServer(NewServer(store, nil))
	t.Cleanup(pinaxSrv.Close)

	pinaxClient := mustConformanceClient(ctx, t, pinaxSrv.URL)
	localClient := mustConformanceClient(ctx, t, localEndpoint)

	pinaxSnap := runConformanceStressScenario(ctx, t, pinaxClient.ddb, "pinax")
	localSnap := runConformanceStressScenario(ctx, t, localClient.ddb, "local")
	if pinaxSnap != localSnap {
		t.Fatalf("stress conformance mismatch\npinax: %+v\nlocal: %+v", pinaxSnap, localSnap)
	}
}

type conformanceClient struct {
	ddb      *dynamodb.Client
	recorder *crcHeaderRecorder
}

func mustConformanceClient(ctx context.Context, t *testing.T, endpoint string) conformanceClient {
	t.Helper()
	recorder := &crcHeaderRecorder{}
	httpClient := &http.Client{Transport: &recordingTransport{base: http.DefaultTransport, recorder: recorder}}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return conformanceClient{ddb: client, recorder: recorder}
}

func runConformanceScenario(ctx context.Context, t *testing.T, cc conformanceClient, prefix string) conformanceSnapshot {
	t.Helper()
	client := cc.ddb

	table := fmt.Sprintf("cf_%s_%d", prefix, time.Now().UnixNano())
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	descAfterCreate, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		t.Fatal(err)
	}
	crcDescribeTableValid := crcHeaderParseable(cc.recorder.last())

	for _, sk := range []string{"1", "2", "10"} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(table),
			Item: map[string]types.AttributeValue{
				"pk":      &types.AttributeValueMemberS{Value: "u#1"},
				"sk":      &types.AttributeValueMemberN{Value: sk},
				"version": &types.AttributeValueMemberN{Value: "1"},
				"shown":   &types.AttributeValueMemberS{Value: "yes"},
				"hidden":  &types.AttributeValueMemberS{Value: "no"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	q1, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		Limit:                  aws.Int32(1),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	q2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		ExclusiveStartKey:      q1.LastEvaluatedKey,
		Limit:                  aws.Int32(2),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	qAll, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	order := make([]string, 0, len(qAll.Items))
	for _, it := range qAll.Items {
		order = append(order, it["sk"].(*types.AttributeValueMemberN).Value)
	}

	bg, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		table: {
			Keys:                 []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}}},
			ProjectionExpression: aws.String("pk, shown"),
		},
	}, ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal})
	if err != nil {
		t.Fatal(err)
	}
	batchItem := bg.Responses[table][0]
	_, hasShown := batchItem["shown"]
	_, hasHidden := batchItem["hidden"]

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "u#1"},
			"sk": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression:    aws.String("SET #v = #v + :one"),
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":      &types.AttributeValueMemberN{Value: "1"},
			":expected": &types.AttributeValueMemberN{Value: "999"},
		},
	})
	if err == nil {
		t.Fatal("expected conditional update error")
	}
	condCode := apiErrorCode(err)
	condHasMsg := apiErrorMessage(err) != ""
	crcConditionalErrValid := crcHeaderParseable(cc.recorder.last())

	_, err = client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		table: {
			Keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
				{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
			},
		},
	}})
	if err == nil {
		t.Fatal("expected batch get duplicate key error")
	}
	batchGetDupCode := apiErrorCode(err)
	batchGetDupHasMsg := apiErrorMessage(err) != ""

	_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		table: {
			{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
			{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
		},
	}})
	if err == nil {
		t.Fatal("expected batch write duplicate key error")
	}
	batchWriteDupCode := apiErrorCode(err)
	batchWriteDupHasMsg := apiErrorMessage(err) != ""

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("gsiPk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
			Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName:  aws.String("gsi_1"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("gsiPk"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	descAfterUpdate, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{
			TableName: aws.String(table),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "u#1"},
				"sk": &types.AttributeValueMemberN{Value: "1"},
			},
			UpdateExpression:         aws.String("SET #v = #v + :one"),
			ConditionExpression:      aws.String("#v = :bad"),
			ExpressionAttributeNames: map[string]string{"#v": "version"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":one": &types.AttributeValueMemberN{Value: "1"},
				":bad": &types.AttributeValueMemberN{Value: "999"},
			},
		}},
		{Put: &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#2"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
	}})
	if err == nil {
		t.Fatal("expected transaction cancellation")
	}
	txCode := apiErrorCode(err)
	txHasMsg := apiErrorMessage(err) != ""
	txReasons := transactionReasonCodes(err)
	txReasonMsgs := transactionReasonMessages(err)
	txReasonCount := len(txReasons)
	crcTxCanceledErrValid := crcHeaderParseable(cc.recorder.last())

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
		UpdateExpression:          aws.String("SET #v = :missing"),
		ExpressionAttributeNames:  map[string]string{"#v": "version"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":other": &types.AttributeValueMemberN{Value: "1"}},
	})
	if err == nil {
		t.Fatal("expected invalid expression update error")
	}
	exprValidationCode := apiErrorCode(err)
	exprValidationMsg := apiErrorMessage(err)

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
		UpdateExpression:          aws.String("SET #v = "),
		ExpressionAttributeNames:  map[string]string{"#v": "version"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
	})
	if err == nil {
		t.Fatal("expected invalid expression syntax error")
	}
	exprSyntaxCode := apiErrorCode(err)
	exprSyntaxMsg := apiErrorMessage(err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{{
		Put:    &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#tx"}, "sk": &types.AttributeValueMemberN{Value: "1"}}},
		Delete: &types.Delete{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#tx"}, "sk": &types.AttributeValueMemberN{Value: "1"}}},
	}}})
	if err == nil {
		t.Fatal("expected invalid transact write shape error")
	}
	txValidationCode := apiErrorCode(err)
	txValidationMsg := apiErrorMessage(err)

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#dup"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
		{Delete: &types.Delete{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#dup"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
	}})
	if err == nil {
		t.Fatal("expected duplicate target transaction cancellation")
	}
	txConflictCode := apiErrorCode(err)
	txConflictMsg := apiErrorMessage(err)

	bigPayload := strings.Repeat("x", 410000)
	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#ok"}, "sk": &types.AttributeValueMemberN{Value: "1"}}}},
		{Put: &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#big"}, "sk": &types.AttributeValueMemberN{Value: "1"}, "blob": &types.AttributeValueMemberS{Value: bigPayload}}}},
	}})
	if err == nil {
		t.Fatal("expected item-size transaction validation error")
	}
	txItemSizeCode := apiErrorCode(err)
	txItemSizeMessage := apiErrorMessage(err)

	tg, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, TransactItems: []types.TransactGetItem{{
		Get: &types.Get{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	tw, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal, TransactItems: []types.TransactWriteItem{{
		Put: &types.Put{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#3"}, "sk": &types.AttributeValueMemberN{Value: "1"}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}

	indexTable := fmt.Sprintf("cf_idx_%s_%d", prefix, time.Now().UnixNano())
	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(indexTable),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("gpk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("lsiSk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("gsi_main"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("gpk"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeInclude, NonKeyAttributes: []string{"summary"}},
		}},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName:  aws.String("lsi_main"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("lsiSk"), KeyType: types.KeyTypeRange}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(indexTable), Item: map[string]types.AttributeValue{
		"pk":      &types.AttributeValueMemberS{Value: "u#1"},
		"sk":      &types.AttributeValueMemberN{Value: "1"},
		"gpk":     &types.AttributeValueMemberS{Value: "g#1"},
		"lsiSk":   &types.AttributeValueMemberS{Value: "A"},
		"summary": &types.AttributeValueMemberS{Value: "yes"},
		"hidden":  &types.AttributeValueMemberS{Value: "no"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	gsiOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(indexTable),
		IndexName:              aws.String("gsi_main"),
		KeyConditionExpression: aws.String("gpk = :g"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":g": &types.AttributeValueMemberS{Value: "g#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	lsiOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(indexTable),
		IndexName:              aws.String("lsi_main"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gsiItem := gsiOut.Items[0]
	_, gsiHasSummary := gsiItem["summary"]
	_, gsiHasHidden := gsiItem["hidden"]
	lsiItem := lsiOut.Items[0]
	_, lsiHasNonKey := lsiItem["hidden"]

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
		"pk":      &types.AttributeValueMemberS{Value: "u#2"},
		"sk":      &types.AttributeValueMemberN{Value: "1"},
		"score":   &types.AttributeValueMemberN{Value: "10"},
		"counter": &types.AttributeValueMemberN{Value: "1"},
		"meta": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"state": &types.AttributeValueMemberS{Value: "active"},
		}},
		"tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "a"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	complexScan, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("NOT (#meta.#state = :archived) AND (#score >= :min OR attribute_not_exists(#ghost))"),
		ExpressionAttributeNames: map[string]string{
			"#meta":  "meta",
			"#state": "state",
			"#score": "score",
			"#ghost": "ghost",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":archived": &types.AttributeValueMemberS{Value: "archived"},
			":min":      &types.AttributeValueMemberN{Value: "5"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	nestedScan, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("#meta.#state = :active"),
		ExpressionAttributeNames: map[string]string{
			"#meta":  "meta",
			"#state": "state",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":active": &types.AttributeValueMemberS{Value: "active"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#2"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
		UpdateExpression:          aws.String("SET #missing = if_not_exists(#missing, :seed), #tags = list_append(if_not_exists(#tags, :empty), :newtag)"),
		ExpressionAttributeNames:  map[string]string{"#missing": "missingCounter", "#tags": "tags"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":seed": &types.AttributeValueMemberN{Value: "1"}, ":empty": &types.AttributeValueMemberL{Value: []types.AttributeValue{}}, ":newtag": &types.AttributeValueMemberL{Value: []types.AttributeValue{&types.AttributeValueMemberS{Value: "b"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#2"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
		UpdateExpression:          aws.String("ADD #ctr :one"),
		ExpressionAttributeNames:  map[string]string{"#ctr": "counter"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	updatedExpr, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#2"}, "sk": &types.AttributeValueMemberN{Value: "1"}}})
	if err != nil {
		t.Fatal(err)
	}
	missingCounterAttr, ok := updatedExpr.Item["missingCounter"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatal("expected numeric missingCounter")
	}
	counterAttr, ok := updatedExpr.Item["counter"].(*types.AttributeValueMemberN)
	if !ok {
		t.Fatal("expected numeric counter")
	}
	tagsAttr, ok := updatedExpr.Item["tags"].(*types.AttributeValueMemberL)
	if !ok {
		t.Fatal("expected list tags")
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("ttl"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ttlDesc, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{TableName: aws.String(table)})
	if err != nil {
		t.Fatal(err)
	}
	ttlStatus := ttlDesc.TimeToLiveDescription.TimeToLiveStatus
	ttlHasAttr := ttlDesc.TimeToLiveDescription.AttributeName != nil && *ttlDesc.TimeToLiveDescription.AttributeName == "ttl"

	scanOut, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("sk = :missing"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":missing": &types.AttributeValueMemberN{Value: "999"},
		},
		Limit:                  aws.Int32(2),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(table),
		Key:                    map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "u#1"}, "sk": &types.AttributeValueMemberN{Value: "1"}},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}

	return conformanceSnapshot{
		QueryOrder:               order,
		QueryPage1Count:          q1.Count,
		QueryPage2Count:          q2.Count,
		BatchProjectionHasShown:  hasShown,
		BatchProjectionHasHidden: hasHidden,
		ConditionalErrorCode:     condCode,
		ConditionalErrorHasMsg:   condHasMsg,
		BatchGetDupHasMsg:        batchGetDupHasMsg,
		BatchWriteDupHasMsg:      batchWriteDupHasMsg,
		TransactionErrorHasMsg:   txHasMsg,
		TransactionReasonCount:   txReasonCount,
		FilteredScanCount:        scanOut.Count,
		FilteredScanScanned:      scanOut.ScannedCount,
		FilteredScanHasLEK:       scanOut.LastEvaluatedKey != nil,
		ComplexFilterCount:       complexScan.Count,
		NestedPathFilterCount:    nestedScan.Count,
		IfNotExistsInitialized:   missingCounterAttr.Value == "1" && counterAttr.Value == "2",
		ListAppendLength:         len(tagsAttr.Value),
		CreateTableStatus:        descAfterCreate.Table.TableStatus,
		UpdateTableStatus:        descAfterUpdate.Table.TableStatus,
		BatchGetDupErrorCode:     batchGetDupCode,
		BatchWriteDupErrorCode:   batchWriteDupCode,
		TransactionErrorCode:     txCode,
		TransactionReasonCodes:   txReasons,
		TransactionReasonMsgs:    txReasonMsgs,
		GetHasConsumedCapacity:   getOut.ConsumedCapacity != nil,
		BatchGetConsumedCount:    len(bg.ConsumedCapacity),
		QueryHasConsumedCapacity: q1.ConsumedCapacity != nil,
		ScanHasConsumedCapacity:  scanOut.ConsumedCapacity != nil,
		TransactGetConsumedCount: len(tg.ConsumedCapacity),
		TransactWriteConsumedLen: len(tw.ConsumedCapacity),
		GSIIncludeHasSummary:     gsiHasSummary,
		GSIIncludeHasHidden:      gsiHasHidden,
		LSIKeysOnlyHasNonKey:     lsiHasNonKey,
		TTLStatusRecognized:      ttlStatus == types.TimeToLiveStatusEnabled || ttlStatus == types.TimeToLiveStatusEnabling,
		TTLHasAttributeName:      ttlHasAttr,
		CRCDescribeTableValid:    crcDescribeTableValid,
		CRCConditionalErrValid:   crcConditionalErrValid,
		CRCTxCanceledErrValid:    crcTxCanceledErrValid,
		ExprValidationCode:       exprValidationCode,
		ExprValidationMessage:    exprValidationMsg,
		ExprSyntaxCode:           exprSyntaxCode,
		ExprSyntaxMessage:        exprSyntaxMsg,
		TxValidationCode:         txValidationCode,
		TxValidationMessage:      txValidationMsg,
		TxConflictCode:           txConflictCode,
		TxConflictMessage:        txConflictMsg,
		TxItemSizeCode:           txItemSizeCode,
		TxItemSizeMessage:        txItemSizeMessage,
	}
}

func runConformanceStressScenario(ctx context.Context, t *testing.T, client *dynamodb.Client, prefix string) conformanceStressSnapshot {
	t.Helper()

	table := fmt.Sprintf("cf_stress_%s_%d", prefix, time.Now().UnixNano())
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	const workers = 4
	const writesPerWorker = 25
	const seeded = 10
	for i := 0; i < seeded; i++ {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "load#1"},
			"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				sk := seeded + (workerID * writesPerWorker) + i
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "load#1"},
					"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", sk)},
				}})
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for writeErr := range errCh {
		if writeErr != nil {
			t.Fatal(writeErr)
		}
	}

	total := 0
	pages := 0
	var scannedCount int32
	var last map[string]types.AttributeValue
	for {
		out, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(table),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "load#1"},
			},
			Limit:             aws.Int32(7),
			ExclusiveStartKey: last,
		})
		if err != nil {
			t.Fatal(err)
		}
		total += len(out.Items)
		scannedCount += out.ScannedCount
		pages++
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		last = out.LastEvaluatedKey
	}

	expected := seeded + (workers * writesPerWorker)
	if total != expected {
		t.Fatalf("expected %d items in stress table, got %d", expected, total)
	}

	return conformanceStressSnapshot{TotalItems: total, PageCount: pages, ScannedCount: scannedCount}
}

func runOperationErrorParityScenario(ctx context.Context, t *testing.T, client *dynamodb.Client, prefix string) operationErrorParitySnapshot {
	t.Helper()

	table := fmt.Sprintf("err_%s_%d", prefix, time.Now().UnixNano())
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "wrong"}}})
	if err == nil {
		t.Fatal("expected put wrong key type error")
	}
	putWrongTypeCode := apiErrorCode(err)
	putWrongTypeMessage := apiErrorMessage(err)

	_, err = client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "wrong"}}})
	if err == nil {
		t.Fatal("expected get wrong key type error")
	}
	getWrongTypeCode := apiErrorCode(err)
	getWrongTypeMessage := apiErrorMessage(err)

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(table), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "wrong"}}})
	if err == nil {
		t.Fatal("expected delete wrong key type error")
	}
	deleteWrongTypeCode := apiErrorCode(err)
	deleteWrongTypeMessage := apiErrorMessage(err)

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "wrong"},
		},
	})
	if err == nil {
		t.Fatal("expected query wrong key type error")
	}
	queryWrongTypeCode := apiErrorCode(err)
	queryWrongTypeMessage := apiErrorMessage(err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberN{Value: "0"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("pk = :missing"),
	})
	if err == nil {
		t.Fatal("expected scan missing value error")
	}
	scanMissingValueCode := apiErrorCode(err)
	scanMissingValueMsg := apiErrorMessage(err)

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberN{Value: "1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(table),
		Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: "1"}},
		UpdateExpression:          aws.String("SET #v = "),
		ExpressionAttributeNames:  map[string]string{"#v": "version"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
	})
	if err == nil {
		t.Fatal("expected update syntax error")
	}
	updateSyntaxCode := apiErrorCode(err)
	updateSyntaxMessage := apiErrorMessage(err)

	_, err = client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{
		table: {
			Keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberN{Value: "1"}},
				{"pk": &types.AttributeValueMemberN{Value: "1"}},
			},
		},
	}})
	if err == nil {
		t.Fatal("expected batch get duplicate key error")
	}
	batchGetDupCode := apiErrorCode(err)
	batchGetDupMessage := apiErrorMessage(err)

	_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		table: {
			{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: "9"}}}},
			{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberN{Value: "9"}}}},
		},
	}})
	if err == nil {
		t.Fatal("expected batch write duplicate key error")
	}
	batchWriteDupCode := apiErrorCode(err)
	batchWriteDupMessage := apiErrorMessage(err)

	return operationErrorParitySnapshot{
		PutWrongTypeCode:       putWrongTypeCode,
		PutWrongTypeMessage:    putWrongTypeMessage,
		GetWrongTypeCode:       getWrongTypeCode,
		GetWrongTypeMessage:    getWrongTypeMessage,
		DeleteWrongTypeCode:    deleteWrongTypeCode,
		DeleteWrongTypeMessage: deleteWrongTypeMessage,
		QueryWrongTypeCode:     queryWrongTypeCode,
		QueryWrongTypeMessage:  queryWrongTypeMessage,
		ScanMissingValueCode:   scanMissingValueCode,
		ScanMissingValueMsg:    scanMissingValueMsg,
		UpdateSyntaxCode:       updateSyntaxCode,
		UpdateSyntaxMessage:    updateSyntaxMessage,
		BatchGetDupCode:        batchGetDupCode,
		BatchGetDupMessage:     batchGetDupMessage,
		BatchWriteDupCode:      batchWriteDupCode,
		BatchWriteDupMessage:   batchWriteDupMessage,
	}
}

func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

func apiErrorMessage(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorMessage()
	}
	return ""
}

func transactionReasonCodes(err error) []string {
	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		return nil
	}
	out := make([]string, 0, len(txErr.CancellationReasons))
	for _, r := range txErr.CancellationReasons {
		if r.Code == nil {
			out = append(out, "")
			continue
		}
		out = append(out, *r.Code)
	}
	return out
}

func transactionReasonMessages(err error) []string {
	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		return nil
	}
	out := make([]string, 0, len(txErr.CancellationReasons))
	for _, r := range txErr.CancellationReasons {
		if r.Message == nil {
			out = append(out, "")
			continue
		}
		out = append(out, *r.Message)
	}
	return out
}

type crcHeaderRecorder struct {
	mu      sync.Mutex
	lastCRC string
}

func (r *crcHeaderRecorder) set(headers http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastCRC = headers.Get("X-Amz-Crc32")
}

func (r *crcHeaderRecorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastCRC
}

type recordingTransport struct {
	base     http.RoundTripper
	recorder *crcHeaderRecorder
}

func (t *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err == nil && resp != nil && t.recorder != nil {
		t.recorder.set(resp.Header)
	}
	return resp, err
}

func crcHeaderParseable(crcStr string) bool {
	crcStr = strings.TrimSpace(crcStr)
	if crcStr == "" {
		return false
	}
	_, err := strconv.ParseUint(crcStr, 10, 32)
	if err != nil {
		return false
	}
	return true
}

func loadKnownConformanceDifferences(t *testing.T) map[string]string {
	t.Helper()
	data, err := os.ReadFile("testdata/conformance_known_differences.json")
	if err != nil {
		t.Fatalf("read known differences file: %v", err)
	}
	var known knownConformanceDifferences
	if err := json.Unmarshal(data, &known); err != nil {
		t.Fatalf("parse known differences file: %v", err)
	}
	if known.Fields == nil {
		return map[string]string{}
	}
	return known.Fields
}

func compareConformanceSnapshots(pinax conformanceSnapshot, local conformanceSnapshot, ignored map[string]string) ([]string, []string) {
	vp := reflect.ValueOf(pinax)
	vl := reflect.ValueOf(local)
	typeOf := vp.Type()
	mismatches := make([]string, 0)
	ignoredMsgs := make([]string, 0)
	seen := map[string]struct{}{}

	for i := 0; i < typeOf.NumField(); i++ {
		name := typeOf.Field(i).Name
		seen[name] = struct{}{}
		pv := vp.Field(i).Interface()
		lv := vl.Field(i).Interface()
		if reflect.DeepEqual(pv, lv) {
			if _, ok := ignored[name]; ok {
				mismatches = append(mismatches, fmt.Sprintf("%s: known-difference entry is stale (values now match)", name))
			}
			continue
		}
		if reason, ok := ignored[name]; ok {
			ignoredMsgs = append(ignoredMsgs, fmt.Sprintf("%s (reason: %s): pinax=%s local=%s", name, reason, formatConformanceValue(pv), formatConformanceValue(lv)))
			continue
		}
		mismatches = append(mismatches, fmt.Sprintf("%s:\n  pinax=%s\n  local=%s", name, formatConformanceValue(pv), formatConformanceValue(lv)))
	}

	for field := range ignored {
		if _, ok := seen[field]; !ok {
			mismatches = append(mismatches, fmt.Sprintf("%s: known-difference entry does not match any snapshot field", field))
		}
	}
	sort.Strings(mismatches)
	sort.Strings(ignoredMsgs)
	return mismatches, ignoredMsgs
}

func formatConformanceValue(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}
