package httpapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestScanParallelSegmentsPartitionFullResultSet(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("scanseg"),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, pk := range []string{"a", "b", "c", "d"} {
		for i := 1; i <= 3; i++ {
			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName: aws.String("scanseg"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: pk},
					"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	fullOut, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("scanseg")})
	if err != nil {
		t.Fatal(err)
	}
	full := map[string]struct{}{}
	for _, item := range fullOut.Items {
		full[itemKey(item)] = struct{}{}
	}

	segment0 := scanAllSegmentItems(t, client, "scanseg", 0, 2)
	segment1 := scanAllSegmentItems(t, client, "scanseg", 1, 2)

	for k := range segment0 {
		if _, exists := segment1[k]; exists {
			t.Fatalf("expected disjoint segment results, key %s appears in both", k)
		}
	}

	union := map[string]struct{}{}
	for k := range segment0 {
		union[k] = struct{}{}
	}
	for k := range segment1 {
		union[k] = struct{}{}
	}

	if len(union) != len(full) {
		t.Fatalf("expected union of segments to match full scan size (%d), got %d", len(full), len(union))
	}
	for k := range full {
		if _, ok := union[k]; !ok {
			t.Fatalf("expected full-scan key %s in segment union", k)
		}
	}
}

func TestScanParallelRequiresSegmentAndTotalSegmentsTogether(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("scansegval"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String("scansegval"),
		Segment:   aws.Int32(0),
	})
	if err == nil {
		t.Fatal("expected validation error when Segment is provided without TotalSegments")
	}

	_, err = client.Scan(ctx, &dynamodb.ScanInput{
		TableName:     aws.String("scansegval"),
		Segment:       aws.Int32(2),
		TotalSegments: aws.Int32(2),
	})
	if err == nil {
		t.Fatal("expected validation error when Segment is not less than TotalSegments")
	}
}

func scanAllSegmentItems(t *testing.T, client *dynamodb.Client, table string, segment, total int32) map[string]struct{} {
	t.Helper()
	ctx := context.Background()
	collected := map[string]struct{}{}
	var start map[string]types.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(table),
			Segment:           aws.Int32(segment),
			TotalSegments:     aws.Int32(total),
			ExclusiveStartKey: start,
			Limit:             aws.Int32(2),
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range out.Items {
			collected[itemKey(item)] = struct{}{}
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		start = out.LastEvaluatedKey
	}
	return collected
}

func itemKey(item map[string]types.AttributeValue) string {
	pk := item["pk"].(*types.AttributeValueMemberS).Value
	if rawSK, ok := item["sk"]; ok {
		sk := rawSK.(*types.AttributeValueMemberN).Value
		return pk + "#" + sk
	}
	return pk
}
