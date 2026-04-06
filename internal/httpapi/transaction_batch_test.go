package httpapi

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestTransactWriteAndGet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tx"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("tx"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}, "v": &types.AttributeValueMemberN{Value: "1"}}}},
		{Put: &types.Put{TableName: aws.String("tx"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}, "v": &types.AttributeValueMemberN{Value: "2"}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []types.TransactGetItem{
		{Get: &types.Get{TableName: aws.String("tx"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}}},
		{Get: &types.Get{TableName: aws.String("tx"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(out.Responses))
	}
}

func TestBatchWriteLimitValidation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bw"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	wr := make([]types.WriteRequest, 0, 26)
	for i := 0; i < 26; i++ {
		wr = append(wr, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: string(rune('a' + i))}}}})
	}
	_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"bw": wr}})
	if err == nil {
		t.Fatal("expected batch write validation error")
	}
}

func TestBatchWriteDuplicateKeysValidation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bw2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	wr := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k1"}}}},
		{DeleteRequest: &types.DeleteRequest{Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k1"}}}},
	}
	_, err = client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"bw2": wr}})
	if err == nil {
		t.Fatal("expected duplicate key validation error")
	}
}

func TestTransactWriteDuplicateTargetValidation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tx2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("tx2"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}}},
		{Delete: &types.Delete{TableName: aws.String("tx2"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}}},
	}})
	if err == nil {
		t.Fatal("expected duplicate target validation error")
	}
}
