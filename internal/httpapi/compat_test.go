package httpapi

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestSDKCreatePutGetQuery(t *testing.T) {
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

	srv := httptest.NewServer(NewServer(store, nil))
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("users"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
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

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("users"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "u#1"},
			"sk":   &types.AttributeValueMemberS{Value: "profile"},
			"name": &types.AttributeValueMemberS{Value: "Jane"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("users"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "u#1"},
			"sk": &types.AttributeValueMemberS{Value: "profile"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var got struct {
		Name string `dynamodbav:"name"`
	}
	if err := attributevalue.UnmarshalMap(getOut.Item, &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "Jane" {
		t.Fatalf("expected Jane, got %q", got.Name)
	}

	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("users"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if queryOut.Count != 1 {
		t.Fatalf("expected query count 1, got %v", queryOut.Count)
	}
}
