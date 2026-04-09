package httpapi

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestCreateTableOptionsRoundTripDescribeTable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tableopts1"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:                 []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:               types.BillingModePayPerRequest,
		TableClass:                types.TableClassStandardInfrequentAccess,
		DeletionProtectionEnabled: aws.Bool(true),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
		SSESpecification: &types.SSESpecification{
			Enabled:        aws.Bool(true),
			SSEType:        types.SSETypeKms,
			KMSMasterKeyId: aws.String("arn:aws:kms:local:000000000000:key/test-key"),
		},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("tableopts1")})
	if err != nil {
		t.Fatal(err)
	}
	if out.Table.TableClassSummary == nil || out.Table.TableClassSummary.TableClass != types.TableClassStandardInfrequentAccess {
		t.Fatalf("expected STANDARD_INFREQUENT_ACCESS table class, got %+v", out.Table.TableClassSummary)
	}
	if out.Table.DeletionProtectionEnabled == nil || !*out.Table.DeletionProtectionEnabled {
		t.Fatalf("expected deletion protection enabled, got %+v", out.Table.DeletionProtectionEnabled)
	}
	if out.Table.StreamSpecification == nil || out.Table.StreamSpecification.StreamEnabled == nil || !*out.Table.StreamSpecification.StreamEnabled {
		t.Fatalf("expected stream enabled, got %+v", out.Table.StreamSpecification)
	}
	if out.Table.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("expected NEW_AND_OLD_IMAGES stream view type, got %v", out.Table.StreamSpecification.StreamViewType)
	}
	if out.Table.LatestStreamArn == nil || *out.Table.LatestStreamArn == "" {
		t.Fatalf("expected LatestStreamArn to be set, got %+v", out.Table.LatestStreamArn)
	}
	if out.Table.SSEDescription == nil || out.Table.SSEDescription.Status != types.SSEStatusEnabled {
		t.Fatalf("expected SSE enabled, got %+v", out.Table.SSEDescription)
	}
	if out.Table.SSEDescription.SSEType != types.SSETypeKms {
		t.Fatalf("expected KMS SSE type, got %+v", out.Table.SSEDescription.SSEType)
	}
}

func TestUpdateTableOptionsRoundTripDescribeTable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tableopts2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:                 aws.String("tableopts2"),
		TableClass:                types.TableClassStandardInfrequentAccess,
		DeletionProtectionEnabled: aws.Bool(true),
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
		SSESpecification: &types.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: types.SSETypeAes256,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("tableopts2")})
	if err != nil {
		t.Fatal(err)
	}
	if out.Table.TableClassSummary == nil || out.Table.TableClassSummary.TableClass != types.TableClassStandardInfrequentAccess {
		t.Fatalf("expected STANDARD_INFREQUENT_ACCESS table class, got %+v", out.Table.TableClassSummary)
	}
	if out.Table.DeletionProtectionEnabled == nil || !*out.Table.DeletionProtectionEnabled {
		t.Fatalf("expected deletion protection enabled, got %+v", out.Table.DeletionProtectionEnabled)
	}
	if out.Table.StreamSpecification == nil || out.Table.StreamSpecification.StreamEnabled == nil || !*out.Table.StreamSpecification.StreamEnabled {
		t.Fatalf("expected stream enabled after update, got %+v", out.Table.StreamSpecification)
	}
	if out.Table.StreamSpecification.StreamViewType != types.StreamViewTypeKeysOnly {
		t.Fatalf("expected KEYS_ONLY stream view type, got %+v", out.Table.StreamSpecification.StreamViewType)
	}
	if out.Table.SSEDescription == nil || out.Table.SSEDescription.Status != types.SSEStatusEnabled {
		t.Fatalf("expected SSE enabled after update, got %+v", out.Table.SSEDescription)
	}
}

func TestTransactWriteClientRequestTokenIdempotency(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txidempotency"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	req := &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String("token-1"),
		TransactItems: []types.TransactWriteItem{{
			Put: &types.Put{
				TableName:           aws.String("txidempotency"),
				Item:                map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
				ConditionExpression: aws.String("attribute_not_exists(pk)"),
			},
		}},
	}

	if _, err := client.TransactWriteItems(ctx, req); err != nil {
		t.Fatalf("first transact write failed: %v", err)
	}
	if _, err := client.TransactWriteItems(ctx, req); err != nil {
		t.Fatalf("idempotent replay transact write failed: %v", err)
	}
}

func TestTransactWriteClientRequestTokenMismatch(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("txidempotency2"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String("token-2"),
		TransactItems: []types.TransactWriteItem{{
			Put: &types.Put{TableName: aws.String("txidempotency2"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		ClientRequestToken: aws.String("token-2"),
		TransactItems: []types.TransactWriteItem{{
			Put: &types.Put{TableName: aws.String("txidempotency2"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}}},
		}},
	})
	if err == nil {
		t.Fatal("expected idempotent parameter mismatch")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "IdempotentParameterMismatchException" {
		t.Fatalf("expected IdempotentParameterMismatchException, got %q", apiErr.ErrorCode())
	}
}
