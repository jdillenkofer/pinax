package httpapi

import (
	"context"
	"database/sql"
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
