package httpapi

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
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

func TestTransactGetAppliesPerItemProjection(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txproj"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("txproj"),
		Item: map[string]types.AttributeValue{
			"pk":     &types.AttributeValueMemberS{Value: "a"},
			"hidden": &types.AttributeValueMemberS{Value: "x"},
			"shown":  &types.AttributeValueMemberS{Value: "y"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []types.TransactGetItem{{
		Get: &types.Get{
			TableName:            aws.String("txproj"),
			Key:                  map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
			ProjectionExpression: aws.String("pk, #s"),
			ExpressionAttributeNames: map[string]string{
				"#s": "shown",
			},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(out.Responses))
	}
	item := out.Responses[0].Item
	if _, ok := item["shown"]; !ok {
		t.Fatalf("expected shown attribute in projection, got %+v", item)
	}
	if _, ok := item["hidden"]; ok {
		t.Fatalf("did not expect hidden attribute in projection, got %+v", item)
	}
}

func TestTransactGetProjectionRequiresExpressionNames(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txproj2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{TransactItems: []types.TransactGetItem{{
		Get: &types.Get{
			TableName:            aws.String("txproj2"),
			Key:                  map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "missing"}},
			ProjectionExpression: aws.String("#missing"),
		},
	}}})
	if err == nil {
		t.Fatal("expected validation error for missing expression name")
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

func TestBatchGetDuplicateKeysValidation(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bgdup"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"bgdup": {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "a"}},
					{"pk": &types.AttributeValueMemberS{Value: "a"}},
				},
			},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate key validation error for BatchGetItem")
	}
}

func TestBatchGetReturnsUnprocessedKeysWhenOverProcessingLimit(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_BATCH_GET_PROCESS_LIMIT", "80")
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bgcap"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]map[string]types.AttributeValue, 0, 90)
	for i := 0; i < 90; i++ {
		id := "k" + strconv.Itoa(i)
		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("bgcap"),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: id},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: id}})
	}

	out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{"bgcap": {Keys: keys}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.UnprocessedKeys["bgcap"].Keys) == 0 {
		t.Fatal("expected unprocessed keys for oversized batch get processing")
	}
}

func TestBatchWriteReturnsUnprocessedItemsWhenOverProcessingLimit(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_BATCH_WRITE_PROCESS_LIMIT", "20")
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bwcap"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	writes := make([]types.WriteRequest, 0, 25)
	for i := 0; i < 25; i++ {
		writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "w" + strconv.Itoa(i)}}}})
	}
	out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{"bwcap": writes}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.UnprocessedItems["bwcap"]) == 0 {
		t.Fatal("expected unprocessed items for oversized batch write processing")
	}
}

func TestBatchGetReturnConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bgcc"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("bgcc"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string]types.KeysAndAttributes{
			"bgcc": {
				Keys: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "a"}}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity in batch get response")
	}
}

func TestBatchWriteReturnConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bwcc"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			"bwcc": {{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity in batch write response")
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
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected smithy API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException code, got %q", apiErr.ErrorCode())
	}
	if apiErr.ErrorMessage() != "Transaction request cannot include multiple operations on one item" {
		t.Fatalf("unexpected duplicate target message: %q", apiErr.ErrorMessage())
	}
}

func TestTransactWriteRejectsMultipleOperationsInOneItem(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txmultiop"),
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
		{
			Put:    &types.Put{TableName: aws.String("txmultiop"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}},
			Delete: &types.Delete{TableName: aws.String("txmultiop"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}},
		},
	}})
	if err == nil {
		t.Fatal("expected validation error for multiple operations in one transaction item")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected smithy API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException code, got %q", apiErr.ErrorCode())
	}
	if apiErr.ErrorMessage() != "TransactItems can only contain one of Check, Put, Update or Delete" {
		t.Fatalf("unexpected validation message: %q", apiErr.ErrorMessage())
	}
}

func TestTransactWriteInvalidUpdateExpressionReturnsValidationReason(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("txvalreason"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("txvalreason"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}}},
		{Update: &types.Update{
			TableName:        aws.String("txvalreason"),
			Key:              map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}},
			UpdateExpression: aws.String("SET #v = "),
			ExpressionAttributeNames: map[string]string{
				"#v": "v",
			},
		}},
	}})
	if err == nil {
		t.Fatal("expected transaction canceled error")
	}
	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransactionCanceledException, got %T: %v", err, err)
	}
	if len(txErr.CancellationReasons) != 2 {
		t.Fatalf("expected 2 cancellation reasons, got %d", len(txErr.CancellationReasons))
	}
	if txErr.CancellationReasons[0].Code == nil || *txErr.CancellationReasons[0].Code != "None" {
		t.Fatalf("expected first reason None, got %+v", txErr.CancellationReasons[0])
	}
	if txErr.CancellationReasons[1].Code == nil || *txErr.CancellationReasons[1].Code != "ValidationError" {
		t.Fatalf("expected second reason ValidationError, got %+v", txErr.CancellationReasons[1])
	}
}

func TestTransactWriteConditionFailureReturnsCancellationReasons(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tx3"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("tx3"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"v":  &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{
			ConditionCheck: &types.ConditionCheck{
				TableName:                           aws.String("tx3"),
				Key:                                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
				ConditionExpression:                 aws.String("attribute_not_exists(pk)"),
				ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
			},
		},
		{Put: &types.Put{TableName: aws.String("tx3"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}}},
	}})
	if err == nil {
		t.Fatal("expected transaction canceled error")
	}

	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransactionCanceledException, got %T: %v", err, err)
	}
	if len(txErr.CancellationReasons) != 2 {
		t.Fatalf("expected 2 cancellation reasons, got %d", len(txErr.CancellationReasons))
	}
	if txErr.CancellationReasons[0].Code == nil || *txErr.CancellationReasons[0].Code != "ConditionalCheckFailed" {
		t.Fatalf("expected first reason ConditionalCheckFailed, got %+v", txErr.CancellationReasons[0])
	}
	if txErr.CancellationReasons[0].Item == nil {
		t.Fatal("expected old item in first cancellation reason")
	}
	if _, ok := txErr.CancellationReasons[0].Item["v"]; !ok {
		t.Fatal("expected old item attributes in first cancellation reason")
	}
	if txErr.CancellationReasons[1].Code == nil || *txErr.CancellationReasons[1].Code != "None" {
		t.Fatalf("expected second reason None, got %+v", txErr.CancellationReasons[1])
	}
}

func TestTransactWriteMixedFailureOrderingPrefersFirstFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("txmixed"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("txmixed"), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "a"},
		"v":  &types.AttributeValueMemberN{Value: "1"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{
			TableName:                 aws.String("txmixed"),
			Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
			UpdateExpression:          aws.String("SET #v = #v + :one"),
			ConditionExpression:       aws.String("#v = :bad"),
			ExpressionAttributeNames:  map[string]string{"#v": "v"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}, ":bad": &types.AttributeValueMemberN{Value: "999"}},
		}},
		{Update: &types.Update{
			TableName:                 aws.String("txmixed"),
			Key:                       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
			UpdateExpression:          aws.String("SET #v = "),
			ExpressionAttributeNames:  map[string]string{"#v": "v"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":one": &types.AttributeValueMemberN{Value: "1"}},
		}},
	}})
	if err == nil {
		t.Fatal("expected transaction canceled error")
	}
	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransactionCanceledException, got %T: %v", err, err)
	}
	if len(txErr.CancellationReasons) != 2 {
		t.Fatalf("expected 2 cancellation reasons, got %d", len(txErr.CancellationReasons))
	}
	if txErr.CancellationReasons[0].Code == nil || *txErr.CancellationReasons[0].Code != "ConditionalCheckFailed" {
		t.Fatalf("expected first reason ConditionalCheckFailed, got %+v", txErr.CancellationReasons[0])
	}
	if txErr.CancellationReasons[1].Code == nil || *txErr.CancellationReasons[1].Code != "None" {
		t.Fatalf("expected second reason None, got %+v", txErr.CancellationReasons[1])
	}
}

func TestTransactWriteLargeItemReturnsValidationReason(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("txbig"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Put: &types.Put{TableName: aws.String("txbig"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "ok"}}}},
		{Put: &types.Put{TableName: aws.String("txbig"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "big"}, "blob": &types.AttributeValueMemberS{Value: strings.Repeat("x", 410000)}}}},
	}})
	if err == nil {
		t.Fatal("expected transaction canceled error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected smithy API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException code, got %q", apiErr.ErrorCode())
	}
	if apiErr.ErrorMessage() != "Item size has exceeded the maximum allowed size" {
		t.Fatalf("unexpected item size message: %q", apiErr.ErrorMessage())
	}
}

func TestTransactWriteConditionFailureWithoutReturnValuesOmitsItem(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tx4"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("tx4"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"v":  &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{
			ConditionCheck: &types.ConditionCheck{
				TableName:           aws.String("tx4"),
				Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
			},
		},
	}})
	if err == nil {
		t.Fatal("expected transaction canceled error")
	}

	var txErr *types.TransactionCanceledException
	if !errors.As(err, &txErr) {
		t.Fatalf("expected TransactionCanceledException, got %T: %v", err, err)
	}
	if len(txErr.CancellationReasons) != 1 {
		t.Fatalf("expected 1 cancellation reason, got %d", len(txErr.CancellationReasons))
	}
	if txErr.CancellationReasons[0].Item != nil {
		t.Fatal("expected no item when ReturnValuesOnConditionCheckFailure is not ALL_OLD")
	}
}

func TestTransactGetReturnConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txcc1"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("txcc1"),
		Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TransactItems: []types.TransactGetItem{{
			Get: &types.Get{TableName: aws.String("txcc1"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity in transact get response")
	}
}

func TestTransactGetNotFoundStillReturnsConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txccmissing"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TransactItems: []types.TransactGetItem{{
			Get: &types.Get{TableName: aws.String("txccmissing"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "missing"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity for missing transact get item")
	}
}

func TestTransactWriteReturnConsumedCapacity(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txcc2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TransactItems: []types.TransactWriteItem{{
			Put: &types.Put{TableName: aws.String("txcc2"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected consumed capacity in transact write response")
	}
}
