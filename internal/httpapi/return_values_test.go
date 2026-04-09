package httpapi

import (
	"context"
	"database/sql"
	"errors"
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

func TestPutDeleteReturnValuesAllOld(t *testing.T) {
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
		TableName:            aws.String("rv"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("rv"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "k1"},
			"name": &types.AttributeValueMemberS{Value: "Jane"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	putOut, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:    aws.String("rv"),
		ReturnValues: types.ReturnValueAllOld,
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "k1"},
			"name": &types.AttributeValueMemberS{Value: "Janet"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := putOut.Attributes["name"]; !ok {
		t.Fatal("expected old attributes from PutItem")
	}

	delOut, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:    aws.String("rv"),
		ReturnValues: types.ReturnValueAllOld,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := delOut.Attributes["name"]; !ok {
		t.Fatal("expected old attributes from DeleteItem")
	}
}

func TestUpdateReturnValuesUpdatedNew(t *testing.T) {
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
		TableName:            aws.String("rv2"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("rv2"),
		Item: map[string]types.AttributeValue{
			"pk":    &types.AttributeValueMemberS{Value: "k1"},
			"count": &types.AttributeValueMemberN{Value: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String("rv2"),
		ReturnValues:     types.ReturnValueUpdatedNew,
		UpdateExpression: aws.String("SET #n = :v ADD count :inc"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":   &types.AttributeValueMemberS{Value: "Jane"},
			":inc": &types.AttributeValueMemberN{Value: "2"},
		},
		Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := out.Attributes["name"]; !ok {
		t.Fatal("expected name in UPDATED_NEW")
	}
	if _, ok := out.Attributes["count"]; !ok {
		t.Fatal("expected count in UPDATED_NEW")
	}
}

func TestPutReturnValuesOnConditionCheckFailureAllOld(t *testing.T) {
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("rv3"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("rv3"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "k1"},
			"name": &types.AttributeValueMemberS{Value: "Jane"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                           aws.String("rv3"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		ConditionExpression:                 aws.String("attribute_not_exists(pk)"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "k1"},
			"name": &types.AttributeValueMemberS{Value: "Janet"},
		},
	})
	if err == nil {
		t.Fatal("expected conditional check failure")
	}
	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		t.Fatalf("expected ConditionalCheckFailedException, got %T: %v", err, err)
	}
	if _, ok := condErr.Item["name"]; !ok {
		t.Fatalf("expected old item in conditional check failure, got %+v", condErr.Item)
	}
}

func TestDeleteReturnValuesOnConditionCheckFailureAllOld(t *testing.T) {
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("rv4"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("rv4"),
		Item: map[string]types.AttributeValue{
			"pk":      &types.AttributeValueMemberS{Value: "k1"},
			"version": &types.AttributeValueMemberN{Value: "2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:                           aws.String("rv4"),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		ConditionExpression:                 aws.String("version = :expected"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberN{Value: "1"},
		},
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "k1"},
		},
	})
	if err == nil {
		t.Fatal("expected conditional check failure")
	}
	var condErr *types.ConditionalCheckFailedException
	if !errors.As(err, &condErr) {
		t.Fatalf("expected ConditionalCheckFailedException, got %T: %v", err, err)
	}
	if _, ok := condErr.Item["version"]; !ok {
		t.Fatalf("expected old item in conditional check failure, got %+v", condErr.Item)
	}
}
