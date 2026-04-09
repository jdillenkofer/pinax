package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestCreateTableWithGSIAndQueryByIndexName(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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
		t.Fatal("expected query error for deleted GSI")
	}
}

func TestUpdateTableRejectsUnknownGSIDelete(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))
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
