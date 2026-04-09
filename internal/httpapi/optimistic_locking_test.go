package httpapi

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestOptimisticLockingCreateAndUpdate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("lock1"),
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
		TableName:           aws.String("lock1"),
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "a"},
			"version": &types.AttributeValueMemberN{Value: "1"},
			"payload": &types.AttributeValueMemberS{Value: "v1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String("lock1"),
		Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		UpdateExpression:    aws.String("SET #v = #v + :one, #payload = :next"),
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v":       "version",
			"#payload": "payload",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":      &types.AttributeValueMemberN{Value: "1"},
			":expected": &types.AttributeValueMemberN{Value: "1"},
			":next":     &types.AttributeValueMemberS{Value: "v2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("lock1"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if getOut.Item["version"].(*types.AttributeValueMemberN).Value != "2" {
		t.Fatalf("expected version 2 after optimistic update, got %+v", getOut.Item["version"])
	}
	if getOut.Item["payload"].(*types.AttributeValueMemberS).Value != "v2" {
		t.Fatalf("expected payload v2 after optimistic update, got %+v", getOut.Item["payload"])
	}
}

func TestOptimisticLockingRejectsStaleUpdateAndDelete(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("lock2"),
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
		TableName: aws.String("lock2"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "a"},
			"version": &types.AttributeValueMemberN{Value: "2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String("lock2"),
		Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		UpdateExpression:    aws.String("SET #v = #v + :one"),
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one":      &types.AttributeValueMemberN{Value: "1"},
			":expected": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err == nil {
		t.Fatal("expected stale optimistic update to fail")
	}
	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		t.Fatalf("expected ConditionalCheckFailedException, got %T: %v", err, err)
	}

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String("lock2"),
		Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err == nil {
		t.Fatal("expected stale optimistic delete to fail")
	}
	if !errors.As(err, &condErr) {
		t.Fatalf("expected ConditionalCheckFailedException, got %T: %v", err, err)
	}
}

func TestOptimisticLockingTransactionStaleVersionReturnsCancellationReason(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("lock3"),
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
		TableName: aws.String("lock3"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "a"},
			"version": &types.AttributeValueMemberN{Value: "2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{
			Update: &types.Update{
				TableName:           aws.String("lock3"),
				Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
				UpdateExpression:    aws.String("SET #v = #v + :one"),
				ConditionExpression: aws.String("#v = :expected"),
				ExpressionAttributeNames: map[string]string{
					"#v": "version",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":one":      &types.AttributeValueMemberN{Value: "1"},
					":expected": &types.AttributeValueMemberN{Value: "1"},
				},
			},
		},
		{Put: &types.Put{TableName: aws.String("lock3"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}}},
	}})
	if err == nil {
		t.Fatal("expected stale transactional optimistic update to fail")
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

func TestOptimisticLockingConcurrentWritersSingleWinner(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("lock4"),
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
		TableName: aws.String("lock4"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "a"},
			"version": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	const writers = 8
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName:           aws.String("lock4"),
				Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
				UpdateExpression:    aws.String("SET #v = #v + :one"),
				ConditionExpression: aws.String("#v = :expected"),
				ExpressionAttributeNames: map[string]string{
					"#v": "version",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":one":      &types.AttributeValueMemberN{Value: "1"},
					":expected": &types.AttributeValueMemberN{Value: "1"},
				},
			})
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	succeeded := 0
	failed := 0
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		failed++
	}

	if succeeded != 1 {
		t.Fatalf("expected exactly one winning writer, got %d successes and %d failures", succeeded, failed)
	}

	getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("lock4"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if getOut.Item["version"].(*types.AttributeValueMemberN).Value != "2" {
		t.Fatalf("expected version 2 after concurrent optimistic writers, got %+v", getOut.Item["version"])
	}
}
