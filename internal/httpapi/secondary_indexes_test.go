package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestCreateTableWithGSIAndQueryByIndexName(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("createdAt"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("createdAt"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:  aws.String("status-index"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("orders"),
		Item: map[string]types.AttributeValue{
			"pk":        &types.AttributeValueMemberS{Value: "o#1"},
			"createdAt": &types.AttributeValueMemberN{Value: "1"},
			"status":    &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item from GSI query, got %d", out.Count)
	}
}

func TestQueryGSIKeysOnlyProjectionReturnsOnlyKeys(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("tenant"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("createdAt"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("tenant"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("status-created-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("createdAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("orders2"),
		Item: map[string]types.AttributeValue{
			"tenant":    &types.AttributeValueMemberS{Value: "t#1"},
			"id":        &types.AttributeValueMemberS{Value: "o#1"},
			"status":    &types.AttributeValueMemberS{Value: "OPEN"},
			"createdAt": &types.AttributeValueMemberN{Value: "1"},
			"note":      &types.AttributeValueMemberS{Value: "keep me hidden"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders2"),
		IndexName:              aws.String("status-created-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item from GSI query, got %d", out.Count)
	}
	item := out.Items[0]
	if _, ok := item["tenant"]; !ok {
		t.Fatal("expected table partition key in projected item")
	}
	if _, ok := item["id"]; !ok {
		t.Fatal("expected table sort key in projected item")
	}
	if _, ok := item["status"]; !ok {
		t.Fatal("expected index partition key in projected item")
	}
	if _, ok := item["createdAt"]; !ok {
		t.Fatal("expected index sort key in projected item")
	}
	if _, ok := item["note"]; ok {
		t.Fatal("did not expect non-key attribute for KEYS_ONLY projection")
	}
}

func TestQueryGSIIncludeProjectionReturnsConfiguredNonKeyAttrs(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders3"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("tenant"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("createdAt"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("tenant"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("status-created-index"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("createdAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeInclude, NonKeyAttributes: []string{"summary"}},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("orders3"),
		Item: map[string]types.AttributeValue{
			"tenant":    &types.AttributeValueMemberS{Value: "t#1"},
			"id":        &types.AttributeValueMemberS{Value: "o#1"},
			"status":    &types.AttributeValueMemberS{Value: "OPEN"},
			"createdAt": &types.AttributeValueMemberN{Value: "1"},
			"summary":   &types.AttributeValueMemberS{Value: "project me"},
			"details":   &types.AttributeValueMemberS{Value: "do not project me"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders3"),
		IndexName:              aws.String("status-created-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 1 {
		t.Fatalf("expected 1 item from GSI query, got %d", out.Count)
	}
	item := out.Items[0]
	if _, ok := item["summary"]; !ok {
		t.Fatal("expected INCLUDE non-key attribute in projected item")
	}
	if _, ok := item["details"]; ok {
		t.Fatal("did not expect non-configured non-key attribute in projected item")
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("orders3")})
	if err != nil {
		t.Fatal(err)
	}
	if len(desc.Table.GlobalSecondaryIndexes) != 1 {
		t.Fatalf("expected one gsi in describe response, got %d", len(desc.Table.GlobalSecondaryIndexes))
	}
	proj := desc.Table.GlobalSecondaryIndexes[0].Projection
	if proj == nil || proj.ProjectionType != types.ProjectionTypeInclude {
		t.Fatalf("expected INCLUDE projection in describe response, got %+v", proj)
	}
	if len(proj.NonKeyAttributes) != 1 || proj.NonKeyAttributes[0] != "summary" {
		t.Fatalf("expected NonKeyAttributes [summary], got %+v", proj.NonKeyAttributes)
	}
}

func TestQueryLSIKeysOnlyProjectionAndOrdering(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_lsi"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("status-lsi"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("status"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeKeysOnly},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_lsi"), Item: map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "u#1"},
		"sk":     &types.AttributeValueMemberN{Value: "2"},
		"status": &types.AttributeValueMemberS{Value: "B"},
		"note":   &types.AttributeValueMemberS{Value: "hidden"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_lsi"), Item: map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "u#1"},
		"sk":     &types.AttributeValueMemberN{Value: "1"},
		"status": &types.AttributeValueMemberS{Value: "A"},
		"note":   &types.AttributeValueMemberS{Value: "hidden"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_lsi"), Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: "u#1"},
		"sk":   &types.AttributeValueMemberN{Value: "3"},
		"note": &types.AttributeValueMemberS{Value: "sparse-skip"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_lsi"),
		IndexName:              aws.String("status-lsi"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 2 {
		t.Fatalf("expected 2 items from LSI query, got %d", out.Count)
	}
	if out.Items[0]["status"].(*types.AttributeValueMemberS).Value != "A" {
		t.Fatalf("expected first item status A, got %+v", out.Items[0]["status"])
	}
	if _, ok := out.Items[0]["note"]; ok {
		t.Fatal("did not expect non-key attribute for KEYS_ONLY LSI projection")
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("orders_lsi")})
	if err != nil {
		t.Fatal(err)
	}
	if len(desc.Table.LocalSecondaryIndexes) != 1 {
		t.Fatalf("expected one lsi in describe response, got %d", len(desc.Table.LocalSecondaryIndexes))
	}
}

func TestCreateTableWithInvalidLSIHashKeyFails(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_invalid_lsi"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("other"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("bad-lsi"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("other"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err == nil {
		t.Fatal("expected validation error for invalid LSI hash key")
	}
}

func TestUpdateTableCreateAndDeleteGSI(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_update_gsi"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
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

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_update_gsi"), Item: map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "u#1"},
		"sk":     &types.AttributeValueMemberN{Value: "1"},
		"status": &types.AttributeValueMemberS{Value: "OPEN"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("orders_update_gsi"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName: aws.String("status-index"),
				KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_update_gsi"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err == nil {
		t.Fatal("expected ResourceInUse while index is CREATING")
	}

	time.Sleep(1100 * time.Millisecond)
	qOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_update_gsi"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if qOut.Count != 1 {
		t.Fatalf("expected 1 item from created GSI query, got %d", qOut.Count)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("orders_update_gsi"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("status-index")}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_update_gsi"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err == nil {
		t.Fatal("expected ResourceInUse while index is DELETING")
	}
	time.Sleep(1100 * time.Millisecond)
	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_update_gsi"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :status"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":status": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err == nil {
		t.Fatal("expected query error for deleted GSI")
	}
}

func TestUpdateTableGSITransitionDelayCanBeConfigured(t *testing.T) {
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	h := newTestServer(backend, nil)
	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)
	go h.StartGSIBackfillWorker(workerCtx, 25*time.Millisecond)
	srv := httptest.NewServer(h)
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_update_gsi_nodelay"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
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
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_update_gsi_nodelay"), Item: map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "u#1"},
		"sk":     &types.AttributeValueMemberN{Value: "1"},
		"status": &types.AttributeValueMemberS{Value: "OPEN"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:            aws.String("orders_update_gsi_nodelay"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS}},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
			Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName:  aws.String("status-index"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		out, qErr := client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String("orders_update_gsi_nodelay"),
			IndexName:              aws.String("status-index"),
			KeyConditionExpression: aws.String("status = :status"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":status": &types.AttributeValueMemberS{Value: "OPEN"},
			},
		})
		if qErr == nil {
			if out.Count != 1 {
				t.Fatalf("expected 1 item from created GSI query, got %d", out.Count)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected GSI query to succeed after async backfill, got: %v", qErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestUpdateTableRejectsUnknownGSIDelete(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_update_gsi_err"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("orders_update_gsi_err"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("missing")}},
		},
	})
	if err == nil {
		t.Fatal("expected update error for unknown GSI")
	}
	var apiErr *types.ResourceNotFoundException
	if errors.As(err, &apiErr) {
		t.Fatalf("expected validation error, got resource not found: %v", err)
	}
}

func TestUpdateTableRejectsGSINameCollisionWithExistingLSI(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_lsi_name_collision"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName:  aws.String("status-lsi"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("status"), KeyType: types.KeyTypeRange}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName: aws.String("orders_lsi_name_collision"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{
			Create: &types.CreateGlobalSecondaryIndexAction{
				IndexName:  aws.String("status-lsi"),
				KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		}},
	})
	if err == nil {
		t.Fatal("expected update table validation error due to GSI/LSI name collision")
	}
}

func TestUpdateTableRejectsMultipleUpdatesForSameGSI(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("orders_multi_gsi_updates"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}, {AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:            aws.String("orders_multi_gsi_updates"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS}},
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
			{Create: &types.CreateGlobalSecondaryIndexAction{IndexName: aws.String("status-index"), KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}}, Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll}}},
			{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("status-index")}},
		},
	})
	if err == nil {
		t.Fatal("expected validation error for multiple updates to same GSI")
	}
}

func TestQueryGSIPaginationUsesExclusiveStartKey(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_gsi_page"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("status-index"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_gsi_page"), Item: map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "u#1"},
			"sk":     &types.AttributeValueMemberN{Value: string(rune('0' + i))},
			"status": &types.AttributeValueMemberS{Value: "OPEN"},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	page1, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_gsi_page"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": &types.AttributeValueMemberS{Value: "OPEN"},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if page1.LastEvaluatedKey == nil || page1.Count != 1 {
		t.Fatalf("expected first page with LEK and count 1, got count=%d lek=%v", page1.Count, page1.LastEvaluatedKey)
	}

	page2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_gsi_page"),
		IndexName:              aws.String("status-index"),
		KeyConditionExpression: aws.String("status = :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": &types.AttributeValueMemberS{Value: "OPEN"},
		},
		ExclusiveStartKey: page1.LastEvaluatedKey,
		Limit:             aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if page2.Count != 2 {
		t.Fatalf("expected second page count 2, got %d", page2.Count)
	}
}

func TestQueryLSIPaginationUsesExclusiveStartKey(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_lsi_page"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{{
			IndexName:  aws.String("status-lsi"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("status"), KeyType: types.KeyTypeRange}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("orders_lsi_page"), Item: map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "u#1"},
			"sk":     &types.AttributeValueMemberN{Value: string(rune('0' + i))},
			"status": &types.AttributeValueMemberS{Value: string(rune('A' + i - 1))},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	page1, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_lsi_page"),
		IndexName:              aws.String("status-lsi"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		Limit: aws.Int32(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if page1.LastEvaluatedKey == nil || page1.Count != 1 {
		t.Fatalf("expected first page with LEK and count 1, got count=%d lek=%v", page1.Count, page1.LastEvaluatedKey)
	}

	page2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_lsi_page"),
		IndexName:              aws.String("status-lsi"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		ExclusiveStartKey: page1.LastEvaluatedKey,
		Limit:             aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if page2.Count != 2 {
		t.Fatalf("expected second page count 2, got %d", page2.Count)
	}
}

func TestQueryGSIRejectsConsistentRead(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("orders_gsi_consistent"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("status"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("status-index"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("status"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("orders_gsi_consistent"),
		IndexName:              aws.String("status-index"),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("status = :s"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": &types.AttributeValueMemberS{Value: "OPEN"},
		},
	})
	if err == nil {
		t.Fatal("expected consistent read validation error on GSI query")
	}
}
