package httpapi

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestQuerySupportsLegacyKeyConditions(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	seedQueryTable(t, client)

	ctx := context.Background()
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName: aws.String("q"),
		KeyConditions: map[string]types.Condition{
			"pk": {
				ComparisonOperator: types.ComparisonOperatorEq,
				AttributeValueList: []types.AttributeValue{&types.AttributeValueMemberS{Value: "u#1"}},
			},
			"sk": {
				ComparisonOperator: types.ComparisonOperatorBetween,
				AttributeValueList: []types.AttributeValue{
					&types.AttributeValueMemberN{Value: "2"},
					&types.AttributeValueMemberN{Value: "4"},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 3 {
		t.Fatalf("expected 3 items from legacy KeyConditions query, got %d", out.Count)
	}
}

func TestQuerySupportsLegacyAttributesToGet(t *testing.T) {
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
		AttributesToGet: []string{"pk"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) == 0 {
		t.Fatal("expected query items")
	}
	if _, ok := out.Items[0]["pk"]; !ok {
		t.Fatal("expected projected pk attribute")
	}
	if _, ok := out.Items[0]["sk"]; ok {
		t.Fatal("did not expect sk attribute when AttributesToGet only includes pk")
	}
}

func TestQuerySelectCountOmitsItems(t *testing.T) {
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
		Select: types.SelectCount,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 5 {
		t.Fatalf("expected count 5, got %d", out.Count)
	}
	if len(out.Items) != 0 {
		t.Fatalf("expected no items for COUNT query, got %d", len(out.Items))
	}
}

func TestQueryAllProjectedAttributesRequiresIndex(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()
	seedQueryTable(t, client)

	ctx := context.Background()
	_, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("q"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "u#1"},
		},
		Select: types.SelectAllProjectedAttributes,
	})
	if err == nil {
		t.Fatal("expected validation error for ALL_PROJECTED_ATTRIBUTES without IndexName")
	}
}
