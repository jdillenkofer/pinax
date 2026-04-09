package httpapi

import (
	"context"
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
