package httpapi

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestCreateTableProvisionedRequiresThroughput(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:            aws.String("bm1"),
		BillingMode:          types.BillingModeProvisioned,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	if err == nil {
		t.Fatal("expected validation error when ProvisionedThroughput is missing")
	}
}

func TestCreateTableProvisionedRoundTripsDescription(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String("bm2"),
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(7),
		},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("bm2")})
	if err != nil {
		t.Fatal(err)
	}
	if out.Table.BillingModeSummary == nil || out.Table.BillingModeSummary.BillingMode != types.BillingModeProvisioned {
		t.Fatalf("expected PROVISIONED billing mode, got %+v", out.Table.BillingModeSummary)
	}
	if out.Table.ProvisionedThroughput == nil || out.Table.ProvisionedThroughput.ReadCapacityUnits == nil || *out.Table.ProvisionedThroughput.ReadCapacityUnits != 5 {
		t.Fatalf("expected read capacity 5, got %+v", out.Table.ProvisionedThroughput)
	}
}

func TestUpdateTableBillingMode(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("bm3"),
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:   aws.String("bm3"),
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(4),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("bm3")})
	if err != nil {
		t.Fatal(err)
	}
	if out.Table.BillingModeSummary == nil || out.Table.BillingModeSummary.BillingMode != types.BillingModeProvisioned {
		t.Fatalf("expected PROVISIONED billing mode, got %+v", out.Table.BillingModeSummary)
	}
}

func TestCreateTableRejectsProvisionedThroughputWithPayPerRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:   aws.String("bm4"),
		BillingMode: types.BillingModePayPerRequest,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var inUse *types.ResourceInUseException
	if errors.As(err, &inUse) {
		t.Fatalf("expected validation error, got resource in use: %v", err)
	}
}
