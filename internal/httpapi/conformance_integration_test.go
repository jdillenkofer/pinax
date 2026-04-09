package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
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
	FilteredScanCount        int32
	FilteredScanScanned      int32
	FilteredScanHasLEK       bool
	CreateTableStatus        types.TableStatus
	UpdateTableStatus        types.TableStatus
	BatchGetDupErrorCode     string
	BatchWriteDupErrorCode   string
	TransactionErrorCode     string
	TransactionReasonCodes   []string
	GetHasConsumedCapacity   bool
	BatchGetConsumedCount    int
	QueryHasConsumedCapacity bool
	ScanHasConsumedCapacity  bool
	TransactGetConsumedCount int
	TransactWriteConsumedLen int
	GSIIncludeHasSummary     bool
	GSIIncludeHasHidden      bool
	LSIKeysOnlyHasNonKey     bool
	TTLStatusAfterEnable     string
}

type knownConformanceDifferences struct {
	Fields map[string]string `json:"fields"`
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

func mustConformanceClient(ctx context.Context, t *testing.T, endpoint string) *dynamodb.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func runConformanceScenario(ctx context.Context, t *testing.T, client *dynamodb.Client, prefix string) conformanceSnapshot {
	t.Helper()

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
	txReasons := transactionReasonCodes(err)

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
	ttlStatus := string(ttlDesc.TimeToLiveDescription.TimeToLiveStatus)

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
		FilteredScanCount:        scanOut.Count,
		FilteredScanScanned:      scanOut.ScannedCount,
		FilteredScanHasLEK:       scanOut.LastEvaluatedKey != nil,
		CreateTableStatus:        descAfterCreate.Table.TableStatus,
		UpdateTableStatus:        descAfterUpdate.Table.TableStatus,
		BatchGetDupErrorCode:     batchGetDupCode,
		BatchWriteDupErrorCode:   batchWriteDupCode,
		TransactionErrorCode:     txCode,
		TransactionReasonCodes:   txReasons,
		GetHasConsumedCapacity:   getOut.ConsumedCapacity != nil,
		BatchGetConsumedCount:    len(bg.ConsumedCapacity),
		QueryHasConsumedCapacity: q1.ConsumedCapacity != nil,
		ScanHasConsumedCapacity:  scanOut.ConsumedCapacity != nil,
		TransactGetConsumedCount: len(tg.ConsumedCapacity),
		TransactWriteConsumedLen: len(tw.ConsumedCapacity),
		GSIIncludeHasSummary:     gsiHasSummary,
		GSIIncludeHasHidden:      gsiHasHidden,
		LSIKeysOnlyHasNonKey:     lsiHasNonKey,
		TTLStatusAfterEnable:     ttlStatus,
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

	for i := 0; i < typeOf.NumField(); i++ {
		name := typeOf.Field(i).Name
		pv := vp.Field(i).Interface()
		lv := vl.Field(i).Interface()
		if reflect.DeepEqual(pv, lv) {
			continue
		}
		if reason, ok := ignored[name]; ok {
			ignoredMsgs = append(ignoredMsgs, fmt.Sprintf("%s (reason: %s): pinax=%v local=%v", name, reason, pv, lv))
			continue
		}
		mismatches = append(mismatches, fmt.Sprintf("%s: pinax=%v local=%v", name, pv, lv))
	}
	sort.Strings(mismatches)
	sort.Strings(ignoredMsgs)
	return mismatches, ignoredMsgs
}
