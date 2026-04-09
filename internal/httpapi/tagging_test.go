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

func TestTaggingLifecycle(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("tagging1"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode: types.BillingModePayPerRequest,
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil {
		t.Fatal(err)
	}

	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("tagging1")})
	if err != nil {
		t.Fatal(err)
	}
	if desc.Table == nil || desc.Table.TableArn == nil || *desc.Table.TableArn == "" {
		t.Fatalf("expected table arn, got %+v", desc)
	}

	_, err = client.TagResource(ctx, &dynamodb.TagResourceInput{
		ResourceArn: desc.Table.TableArn,
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("core")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	tagsOut, err := client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: desc.Table.TableArn})
	if err != nil {
		t.Fatal(err)
	}
	if len(tagsOut.Tags) != 2 {
		t.Fatalf("expected 2 tags, got %+v", tagsOut.Tags)
	}
	if !hasTag(tagsOut.Tags, "env", "prod") {
		t.Fatalf("expected env=prod tag, got %+v", tagsOut.Tags)
	}
	if !hasTag(tagsOut.Tags, "team", "core") {
		t.Fatalf("expected team=core tag, got %+v", tagsOut.Tags)
	}

	_, err = client.UntagResource(ctx, &dynamodb.UntagResourceInput{
		ResourceArn: desc.Table.TableArn,
		TagKeys:     []string{"team", "missing"},
	})
	if err != nil {
		t.Fatal(err)
	}

	tagsOut, err = client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: desc.Table.TableArn})
	if err != nil {
		t.Fatal(err)
	}
	if len(tagsOut.Tags) != 1 {
		t.Fatalf("expected 1 tag after untag, got %+v", tagsOut.Tags)
	}
	if !hasTag(tagsOut.Tags, "env", "prod") {
		t.Fatalf("expected env=prod to remain, got %+v", tagsOut.Tags)
	}
}

func TestTagResourceRejectsDuplicateTagKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.TagResource(ctx, &dynamodb.TagResourceInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/missing"),
		Tags: []types.Tag{
			{Key: aws.String("dup"), Value: aws.String("a")},
			{Key: aws.String("dup"), Value: aws.String("b")},
		},
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %q", apiErr.ErrorCode())
	}
}

func TestListTagsOfResourceMissingResourceReturnsResourceNotFound(t *testing.T) {
	testutils.SkipIfIntegration(t)

	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/does-not-exist")})
	if err == nil {
		t.Fatal("expected ResourceNotFoundException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ResourceNotFoundException" {
		t.Fatalf("expected ResourceNotFoundException, got %q", apiErr.ErrorCode())
	}
}

func hasTag(tags []types.Tag, key string, value string) bool {
	for _, tag := range tags {
		if tag.Key != nil && tag.Value != nil && *tag.Key == key && *tag.Value == value {
			return true
		}
	}
	return false
}
