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

func TestBackupLifecycleAndDeleteSemantics(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("backupsrc"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("backupsrc"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"v":  &types.AttributeValueMemberS{Value: "1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	created, err := client.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("backupsrc"), BackupName: aws.String("backup-one")})
	if err != nil {
		t.Fatal(err)
	}
	if created.BackupDetails == nil || created.BackupDetails.BackupArn == nil || *created.BackupDetails.BackupArn == "" {
		t.Fatalf("expected backup arn, got %+v", created)
	}
	if created.BackupDetails.BackupStatus != types.BackupStatusAvailable {
		t.Fatalf("expected AVAILABLE backup status, got %q", created.BackupDetails.BackupStatus)
	}

	described, err := client.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{BackupArn: created.BackupDetails.BackupArn})
	if err != nil {
		t.Fatal(err)
	}
	if described.BackupDescription == nil || described.BackupDescription.SourceTableDetails == nil {
		t.Fatalf("expected source table details, got %+v", described)
	}
	if described.BackupDescription.SourceTableDetails.TableName == nil || *described.BackupDescription.SourceTableDetails.TableName != "backupsrc" {
		t.Fatalf("expected source table name backupsrc, got %+v", described.BackupDescription.SourceTableDetails)
	}

	listed, err := client.ListBackups(ctx, &dynamodb.ListBackupsInput{TableName: aws.String("backupsrc")})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.BackupSummaries) != 1 {
		t.Fatalf("expected one backup summary, got %+v", listed.BackupSummaries)
	}

	deleted, err := client.DeleteBackup(ctx, &dynamodb.DeleteBackupInput{BackupArn: created.BackupDetails.BackupArn})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.BackupDescription == nil || deleted.BackupDescription.BackupDetails == nil {
		t.Fatalf("expected backup description in delete response, got %+v", deleted)
	}
	if deleted.BackupDescription.BackupDetails.BackupStatus != types.BackupStatusDeleted {
		t.Fatalf("expected DELETED backup status in delete response, got %q", deleted.BackupDescription.BackupDetails.BackupStatus)
	}

	_, err = client.DescribeBackup(ctx, &dynamodb.DescribeBackupInput{BackupArn: created.BackupDetails.BackupArn})
	if err == nil {
		t.Fatal("expected BackupNotFoundException after delete")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "BackupNotFoundException" {
		t.Fatalf("expected BackupNotFoundException, got %q", apiErr.ErrorCode())
	}
}

func TestDeleteBackupMissingReturnsBackupNotFound(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.DeleteBackup(ctx, &dynamodb.DeleteBackupInput{BackupArn: aws.String("arn:aws:dynamodb:local:000000000000:table/missing/backup/0000000000000000")})
	if err == nil {
		t.Fatal("expected BackupNotFoundException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "BackupNotFoundException" {
		t.Fatalf("expected BackupNotFoundException, got %q", apiErr.ErrorCode())
	}
}

func TestListBackupsPaginationAndTimeFilters(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("listbackupsrc"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("listbackupsrc"), BackupName: aws.String("list-one")})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	second, err := client.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("listbackupsrc"), BackupName: aws.String("list-two")})
	if err != nil {
		t.Fatal(err)
	}

	page1, err := client.ListBackups(ctx, &dynamodb.ListBackupsInput{TableName: aws.String("listbackupsrc"), Limit: aws.Int32(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.BackupSummaries) != 1 {
		t.Fatalf("expected one backup summary on first page, got %+v", page1.BackupSummaries)
	}
	if page1.LastEvaluatedBackupArn == nil || *page1.LastEvaluatedBackupArn == "" {
		t.Fatalf("expected LastEvaluatedBackupArn on first page, got %+v", page1)
	}

	page2, err := client.ListBackups(ctx, &dynamodb.ListBackupsInput{TableName: aws.String("listbackupsrc"), Limit: aws.Int32(5), ExclusiveStartBackupArn: page1.LastEvaluatedBackupArn})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.BackupSummaries) != 1 {
		t.Fatalf("expected one backup summary on second page, got %+v", page2.BackupSummaries)
	}

	if first.BackupDetails == nil || first.BackupDetails.BackupCreationDateTime == nil {
		t.Fatalf("expected first backup creation time, got %+v", first)
	}
	upperBound := first.BackupDetails.BackupCreationDateTime.Add(1 * time.Second)
	filtered, err := client.ListBackups(ctx, &dynamodb.ListBackupsInput{TableName: aws.String("listbackupsrc"), TimeRangeUpperBound: aws.Time(upperBound)})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered.BackupSummaries) != 1 {
		t.Fatalf("expected one filtered backup, got %+v", filtered.BackupSummaries)
	}
	if filtered.BackupSummaries[0].BackupArn == nil || second.BackupDetails.BackupArn == nil {
		t.Fatalf("missing backup arn in filtered response: %+v", filtered)
	}
	if *filtered.BackupSummaries[0].BackupArn == *second.BackupDetails.BackupArn {
		t.Fatalf("expected time upper bound to exclude second backup, got %+v", filtered.BackupSummaries)
	}
}

func TestRestoreTableFromBackupRestoresItemsAndSupportsOverrides(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv("PINAX_LIFECYCLE_DELAY_MS", "0")

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("restoresrc"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gpk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName:             aws.String("g1"),
				KeySchema:             []types.KeySchemaElement{{AttributeName: aws.String("gpk"), KeyType: types.KeyTypeHash}},
				Projection:            &types.Projection{ProjectionType: types.ProjectionTypeAll},
				ProvisionedThroughput: &types.ProvisionedThroughput{ReadCapacityUnits: aws.Int64(1), WriteCapacityUnits: aws.Int64(1)},
			},
		},
		BillingMode: types.BillingModeProvisioned,
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("restoresrc"), Item: map[string]types.AttributeValue{
		"pk":  &types.AttributeValueMemberS{Value: "a"},
		"gpk": &types.AttributeValueMemberS{Value: "ga"},
		"v":   &types.AttributeValueMemberS{Value: "x"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	backup, err := client.CreateBackup(ctx, &dynamodb.CreateBackupInput{TableName: aws.String("restoresrc"), BackupName: aws.String("restore-one")})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.RestoreTableFromBackup(ctx, &dynamodb.RestoreTableFromBackupInput{
		BackupArn:           backup.BackupDetails.BackupArn,
		TargetTableName:     aws.String("restoretarget"),
		BillingModeOverride: types.BillingModePayPerRequest,
		GlobalSecondaryIndexOverride: []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("g1"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("gpk"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	item, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("restoretarget"), Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := item.Item["v"]; !ok {
		t.Fatalf("expected restored item attribute, got %+v", item.Item)
	}
}
