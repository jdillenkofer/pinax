package httpapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestGetItemNotFoundStillReturnsConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("ccnotfound"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String("ccnotfound"),
		Key:                    map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "missing"}},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.ConsumedCapacity == nil {
		t.Fatal("expected consumed capacity even when item is missing")
	}
}

func TestBatchGetNotFoundStillReturnsConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("bgccmiss"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string]types.KeysAndAttributes{
			"bgccmiss": {Keys: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "missing"}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity for not-found batch get key")
	}
}

func TestQueryFilteredOutItemsStillReturnConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("ccquerymiss"),
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
	for i := 1; i <= 3; i++ {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("ccquerymiss"), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("ccquerymiss"),
		KeyConditionExpression: aws.String("pk = :pk"),
		FilterExpression:       aws.String("sk = :never"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":    &types.AttributeValueMemberS{Value: "a"},
			":never": &types.AttributeValueMemberN{Value: "999"},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 0 {
		t.Fatalf("expected 0 matching items, got %d", out.Count)
	}
	if out.ConsumedCapacity == nil {
		t.Fatal("expected consumed capacity on fully filtered query")
	}
}
