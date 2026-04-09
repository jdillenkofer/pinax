package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestGetItemConsumedCapacityIsSingleObject(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("ccshape"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("ccshape"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String("ccshape"),
		Key:                    map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.ConsumedCapacity == nil {
		t.Fatal("expected consumed capacity object")
	}
}

func TestBatchGetAppliesProjectionExpression(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("bgproj"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("bgproj"), Item: map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "a"},
		"hidden": &types.AttributeValueMemberS{Value: "x"},
		"shown":  &types.AttributeValueMemberS{Value: "y"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"bgproj": {
				Keys:                 []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}},
				ProjectionExpression: aws.String("pk, shown"),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	item := out.Responses["bgproj"][0]
	if _, ok := item["shown"]; !ok {
		t.Fatal("expected projected attribute shown")
	}
	if _, ok := item["hidden"]; ok {
		t.Fatal("did not expect hidden attribute in projected batch-get item")
	}
}

func TestListTablesReturnsLastEvaluatedTableName(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	for _, name := range []string{"lt1", "lt2", "lt3"} {
		_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:            aws.String(name),
			AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
			KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
			BillingMode:          types.BillingModePayPerRequest,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	out, err := client.ListTables(ctx, &dynamodb.ListTablesInput{Limit: aws.Int32(2)})
	if err != nil {
		t.Fatal(err)
	}
	if out.LastEvaluatedTableName == nil || *out.LastEvaluatedTableName == "" {
		t.Fatal("expected LastEvaluatedTableName for truncated table listing")
	}
}

func TestQueryNumericSortKeyOrdering(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("qnum"),
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
	for _, sk := range []string{"10", "2", "3"} {
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("qnum"), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"sk": &types.AttributeValueMemberN{Value: sk},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}

	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("qnum"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(out.Items))
	}
	if out.Items[0]["sk"].(*types.AttributeValueMemberN).Value != "2" || out.Items[2]["sk"].(*types.AttributeValueMemberN).Value != "10" {
		t.Fatalf("expected numeric ordering 2,3,10 got %+v %+v %+v", out.Items[0]["sk"], out.Items[1]["sk"], out.Items[2]["sk"])
	}
}

func TestGetItemRejectsWrongKeyType(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("keytype1"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeN}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("keytype1"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "wrong"}},
	})
	if err == nil {
		t.Fatal("expected validation error for wrong key type")
	}
	var vErr *types.ConditionalCheckFailedException
	if errors.As(err, &vErr) {
		t.Fatalf("expected validation error, got conditional failure: %v", err)
	}
}

func TestQueryRejectsWrongKeyType(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("keytype2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}, {AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String("keytype2"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "wrong"},
		},
	})
	if err == nil {
		t.Fatal("expected validation error for wrong query key type")
	}
}

func TestDeleteTableTransitionsAndFinalRemoval(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("dellife"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{TableName: aws.String("dellife")})
	if err != nil {
		t.Fatal(err)
	}
	if out.TableDescription == nil || out.TableDescription.TableStatus != types.TableStatusDeleting {
		t.Fatalf("expected DELETING status from DeleteTable, got %+v", out.TableDescription)
	}
	time.Sleep(1100 * time.Millisecond)
	_, err = client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("dellife")})
	if err == nil {
		t.Fatal("expected table to be removed after deletion transition")
	}
}

func TestScanFilterLimitReturnsLastEvaluatedKeyByScannedItems(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("scanpage"),
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
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("scanpage"), Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"sk": &types.AttributeValueMemberN{Value: string(rune('0' + i))},
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := client.Scan(ctx, &dynamodb.ScanInput{
		TableName:        aws.String("scanpage"),
		FilterExpression: aws.String("sk = :target"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":target": &types.AttributeValueMemberN{Value: "3"},
		},
		Limit: aws.Int32(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	if out.Count != 0 {
		t.Fatalf("expected 0 returned items in first page, got %d", out.Count)
	}
	if out.ScannedCount != 2 {
		t.Fatalf("expected scanned count 2, got %d", out.ScannedCount)
	}
	if out.LastEvaluatedKey == nil {
		t.Fatal("expected LastEvaluatedKey based on scanned limit")
	}
}
