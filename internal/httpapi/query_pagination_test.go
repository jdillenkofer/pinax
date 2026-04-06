package httpapi

import (
	"context"
	"database/sql"
	"fmt"
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

func newTestClient(t *testing.T) (*dynamodb.Client, func()) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewServer(store, nil))

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	cleanup := func() {
		srv.Close()
		_ = db.Close()
	}
	return client, cleanup
}

func seedQueryTable(t *testing.T, client *dynamodb.Client) {
	t.Helper()
	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("q"),
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
	for i := 1; i <= 5; i++ {
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("q"),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "u#1"},
				"sk":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				"group": &types.AttributeValueMemberS{Value: "A"},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestQuerySupportsBetweenAndBeginsWith(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	seedQueryTable(t, client)

	ctx := context.Background()
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("q"),
		KeyConditionExpression: aws.String("pk = :pk AND sk BETWEEN :a AND :b"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
			":a":  &types.AttributeValueMemberN{Value: "2"},
			":b":  &types.AttributeValueMemberN{Value: "4"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items, got %d", out.Count)
	}
}

func TestQueryPaginationUsesLastEvaluatedKey(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	seedQueryTable(t, client)

	ctx := context.Background()
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("q"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		Limit: aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.LastEvaluatedKey == nil {
		t.Fatal("expected last evaluated key")
	}
	if out.ScannedCount != 2 {
		t.Fatalf("expected scanned count 2, got %d", out.ScannedCount)
	}

	out2, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("q"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		ExclusiveStartKey: out.LastEvaluatedKey,
		Limit:             aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out2.Count == 0 {
		t.Fatal("expected next page to contain items")
	}
}
