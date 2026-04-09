package httpapi

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestConditionExpressionMissingNameUsesDynamoStyleMessage(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("condmsg1"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String("condmsg1"),
		Item:                map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		ConditionExpression: aws.String("#missing = :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberS{Value: "a"},
		},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "Invalid ConditionExpression") {
		t.Fatalf("expected condition expression validation prefix, got %v", err)
	}
	if !strings.Contains(err.Error(), "attribute name") {
		t.Fatalf("expected attribute name hint, got %v", err)
	}
}
