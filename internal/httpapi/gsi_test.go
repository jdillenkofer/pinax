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
