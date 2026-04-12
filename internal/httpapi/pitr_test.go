package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pinax/internal/mutation"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
	"github.com/jdillenkofer/pinax/internal/ttl"

	_ "github.com/mattn/go-sqlite3"
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

func TestRestoreTableToPointInTimeHonorsTTLDeletes(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(newTestServer(backend, nil, WithMutationHooks(mutation.DefaultHooks()...)))
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrttl"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String("pitrttl"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("ttl"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrttl"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	expiresAt := time.Now().Unix() + 3
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pitrttl"), Item: map[string]types.AttributeValue{
		"pk":  &types.AttributeValueMemberS{Value: "a"},
		"v":   &types.AttributeValueMemberS{Value: "alive"},
		"ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresAt, 10)},
	}})
	if err != nil {
		t.Fatal(err)
	}

	restoreBeforeExpiry := time.Now().UTC()
	for time.Now().Unix() <= expiresAt {
		time.Sleep(100 * time.Millisecond)
	}

	sweeper := ttl.NewSweeper(backend.DB(), sqlite.NewUnitOfWork(backend.DB(), sqlite.NewFactory(backend)), time.Hour, mutation.NewExecutor(mutation.NewPITRHook()))
	sweeper.RunOnce(ctx)

	restoreAfterExpiry := time.Now().UTC()

	current, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("pitrttl"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if current.Item != nil {
		t.Fatalf("expected source item to be expired and deleted, got %+v", current.Item)
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitrttl"),
		TargetTableName: aws.String("pitrttlbefore"),
		RestoreDateTime: aws.Time(restoreBeforeExpiry),
	})
	if err != nil {
		t.Fatal(err)
	}

	beforeItem, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("pitrttlbefore"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if beforeItem.Item == nil {
		t.Fatal("expected item to exist in restore before TTL expiration")
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitrttl"),
		TargetTableName: aws.String("pitrttlafter"),
		RestoreDateTime: aws.Time(restoreAfterExpiry),
	})
	if err != nil {
		t.Fatal(err)
	}

	afterItem, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String("pitrttlafter"),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if afterItem.Item != nil {
		t.Fatalf("expected item to be absent in restore after TTL expiration, got %+v", afterItem.Item)
	}
}

func TestRestoreTableToPointInTimeRejectsTimeAfterLatestLaggedBoundary(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")
	t.Setenv("PINAX_PITR_LATEST_RESTORABLE_LAG_MS", "50")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrlag"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName: aws.String("pitrlag"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
			PointInTimeRecoveryEnabled: aws.Bool(true),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(120 * time.Millisecond)
	desc, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("pitrlag")})
	if err != nil {
		t.Fatal(err)
	}
	if desc.ContinuousBackupsDescription == nil || desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription == nil || desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription.LatestRestorableDateTime == nil {
		t.Fatalf("expected latest restorable datetime, got %+v", desc)
	}

	nowMs := time.Now().UnixMilli()
	latestMs := aws.ToTime(desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription.LatestRestorableDateTime).UnixMilli()
	if nowMs-latestMs < 40 {
		t.Fatalf("expected latest restorable time to lag current time, nowMs=%d latestMs=%d", nowMs, latestMs)
	}

	now := time.Now().UTC()
	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitrlag"),
		TargetTableName: aws.String("pitrlagcopy"),
		RestoreDateTime: aws.Time(now),
	})
	if err == nil {
		t.Fatal("expected InvalidRestoreTimeException when restoring after latest lagged boundary")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "InvalidRestoreTimeException" {
		t.Fatalf("expected InvalidRestoreTimeException, got %q", apiErr.ErrorCode())
	}
}

func TestRestoreTableToPointInTimeAcceptsExactEarliestAndLatestBoundaries(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")
	t.Setenv("PINAX_PITR_LATEST_RESTORABLE_LAG_MS", "0")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitredges"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pitredges"), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "a"},
		"v":  &types.AttributeValueMemberS{Value: "before"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String("pitredges"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(15 * time.Millisecond)
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("pitredges"), Item: map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "a"},
		"v":  &types.AttributeValueMemberS{Value: "after"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	desc, err := client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{TableName: aws.String("pitredges")})
	if err != nil {
		t.Fatal(err)
	}
	earliest := desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription.EarliestRestorableDateTime
	latest := desc.ContinuousBackupsDescription.PointInTimeRecoveryDescription.LatestRestorableDateTime
	if earliest == nil || latest == nil {
		t.Fatalf("expected earliest/latest boundaries, got %+v", desc)
	}

	earliestTime := time.UnixMilli(aws.ToTime(earliest).UnixMilli()).UTC()
	latestTime := time.UnixMilli(aws.ToTime(latest).UnixMilli()).UTC()

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitredges"),
		TargetTableName: aws.String("pitredgesearliest"),
		RestoreDateTime: aws.Time(earliestTime),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitredges"),
		TargetTableName: aws.String("pitredgeslatest"),
		RestoreDateTime: aws.Time(latestTime),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRestoreTableToPointInTimeResetsWindowAfterDisableReenable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("pitrreenable"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String("pitrreenable"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String("pitrreenable"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}

	gapTime := time.Now().UTC()
	time.Sleep(10 * time.Millisecond)

	_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
		TableName:                        aws.String("pitrreenable"),
		PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{PointInTimeRecoveryEnabled: aws.Bool(true)},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.RestoreTableToPointInTime(ctx, &dynamodb.RestoreTableToPointInTimeInput{
		SourceTableName: aws.String("pitrreenable"),
		TargetTableName: aws.String("pitrreenableold"),
		RestoreDateTime: aws.Time(gapTime),
	})
	if err == nil {
		t.Fatal("expected InvalidRestoreTimeException for restore before re-enable point")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "InvalidRestoreTimeException" {
		t.Fatalf("expected InvalidRestoreTimeException, got %q", apiErr.ErrorCode())
	}
}
