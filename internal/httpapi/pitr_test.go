package httpapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestUpdateAndDescribeContinuousBackups(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrdesc"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	before, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("pitrdesc")})
	if err != nil {
		t.Fatal(err)
	}
	if before.ContinuousBackupsDescription == nil || before.ContinuousBackupsDescription.PointInTimeRecoveryDescription == nil {
		t.Fatalf("expected PITR description, got %+v", before)
	}
	if before.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusDisabled {
		t.Fatalf("expected disabled PITR by default, got %+v", before.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	}

	enabled, err := client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrdesc"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
			RecoveryPeriodInDays:       aws.Int32(7),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if enabled.ContinuousBackupsDescription == nil || enabled.ContinuousBackupsDescription.PointInTimeRecoveryDescription == nil {
		t.Fatalf("expected update response PITR description, got %+v", enabled)
	}
	if enabled.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusEnabled {
		t.Fatalf("expected enabled PITR status, got %+v", enabled.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	}

	after, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("pitrdesc")})
	if err != nil {
		t.Fatal(err)
	}
	if after.ContinuousBackupsDescription.PointInTimeRecoveryDescription.RecoveryPeriodInDays == nil || *after.ContinuousBackupsDescription.PointInTimeRecoveryDescription.RecoveryPeriodInDays != 7 {
		t.Fatalf("expected recovery period 7 days, got %+v", after.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	}
	if after.ContinuousBackupsDescription.PointInTimeRecoveryDescription.EarliestRestorableDateTime == nil {
		t.Fatalf("expected earliest restorable datetime when enabled, got %+v", after.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	}
}

func TestRestoreToPointInTimeRequiresEnabledPITR(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrrequired"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName:         aws.String("pitrrequired"),
		TargetTableName:         aws.String("pitrrequiredcopy"),
		UseLatestRestorableTime: aws.Bool(true),
	})
	if err == nil {
		t.Fatal("expected PointInTimeRecoveryUnavailableException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "PointInTimeRecoveryUnavailableException" {
		t.Fatalf("expected PointInTimeRecoveryUnavailableException, got %q", apiErr.ErrorCode())
	}
}

func TestRestoreTableToPointInTimeRestoresHistoricalState(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrsrc"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pitrsrc"), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "a"},
		"v":  &types.AttributeValueMemberS{Value: "old"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrsrc"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(25 * time.Millisecond)
	restoreTime := time.Now().UTC()
	time.Sleep(25 * time.Millisecond)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pitrsrc"), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "a"},
		"v":  &types.AttributeValueMemberS{Value: "new"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName:     aws.String("pitrsrc"),
		TargetTableName:     aws.String("pitrrestored"),
		RestoreDateTime:     aws.Time(restoreTime),
		BillingModeOverride: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("pitrrestored"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := out.Item["v"].(*types.AttributeValueMemberS)
	if !ok || v.Value != "old" {
		t.Fatalf("expected restored historical value old, got %+v", out.Item)
	}

	targetPITR, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("pitrrestored")})
	if err != nil {
		t.Fatal(err)
	}
	if targetPITR.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus != types.PointInTimeRecoveryStatusDisabled {
		t.Fatalf("expected restored table PITR disabled by default, got %+v", targetPITR.ContinuousBackupsDescription.PointInTimeRecoveryDescription)
	}
}

func TestRestoreTableToPointInTimeRejectsOutOfWindowRestoreTime(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrrange"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrrange"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tooEarly := time.Now().UTC().Add(-48 * time.Hour)
	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitrrange"),
		TargetTableName: aws.String("pitrrangecopy"),
		RestoreDateTime: aws.Time(tooEarly),
	})
	if err == nil {
		t.Fatal("expected InvalidRestoreTimeException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "InvalidRestoreTimeException" {
		t.Fatalf("expected InvalidRestoreTimeException, got %q", apiErr.ErrorCode())
	}
}

func TestUpdateContinuousBackupsRejectsInvalidRecoveryPeriod(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrbadperiod"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrbadperiod"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
			RecoveryPeriodInDays:       aws.Int32(40),
		},
	})
	if err == nil {
		t.Fatal("expected validation exception")
	}
}
