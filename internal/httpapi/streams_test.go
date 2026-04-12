package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	dstypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/middleware"
	"github.com/jdillenkofer/pinax/internal/mutation"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

func newStreamTestClients(t *testing.T, serverOpts ...ServerOption) (*dynamodb.Client, *dynamodbstreams.Client, func()) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	defaultOpts := []ServerOption{WithMutationHooks(mutation.DefaultHooks(store)...)}
	allOpts := append(defaultOpts, serverOpts...)
	srv := httptest.NewServer(NewServer(store, nil, allOpts...))

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	streams := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	cleanup := func() {
		srv.Close()
		_ = db.Close()
	}
	return ddb, streams, cleanup
}

func TestStreamsRoundTripAcrossMutations(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	key := []byte("streams-expiry-test-key")
	ddb, streams, cleanup := newStreamTestClients(t, WithStreamIteratorTTL(2*time.Millisecond), WithStreamIteratorSigningKey(key))
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("streams_rt"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbtypes.KeyTypeRange},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_rt")})
	if err != nil {
		t.Fatal(err)
	}
	if desc.Table == nil || desc.Table.LatestStreamArn == nil {
		t.Fatalf("expected latest stream arn, got %+v", desc.Table)
	}
	arn := *desc.Table.LatestStreamArn

	_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("streams_rt"),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":      &ddbtypes.AttributeValueMemberS{Value: "u#1"},
			"sk":      &ddbtypes.AttributeValueMemberS{Value: "v#1"},
			"payload": &ddbtypes.AttributeValueMemberS{Value: "first"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String("streams_rt"),
		Key:              map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "u#1"}, "sk": &ddbtypes.AttributeValueMemberS{Value: "v#1"}},
		UpdateExpression: aws.String("SET payload = :p"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":p": &ddbtypes.AttributeValueMemberS{Value: "second"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("streams_rt"),
		Key:       map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "u#1"}, "sk": &ddbtypes.AttributeValueMemberS{Value: "v#1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	listOut, err := streams.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{TableName: aws.String("streams_rt")})
	if err != nil {
		t.Fatal(err)
	}
	if len(listOut.Streams) != 1 {
		t.Fatalf("expected one stream, got %+v", listOut.Streams)
	}
	if listOut.Streams[0].StreamArn == nil || *listOut.Streams[0].StreamArn != arn {
		t.Fatalf("expected stream arn %q, got %+v", arn, listOut.Streams[0])
	}

	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	if streamDesc.StreamDescription == nil || len(streamDesc.StreamDescription.Shards) != 1 {
		t.Fatalf("expected single shard stream description, got %+v", streamDesc.StreamDescription)
	}
	shardID := *streamDesc.StreamDescription.Shards[0].ShardId

	itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(arn),
		ShardId:           aws.String(shardID),
		ShardIteratorType: dstypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatal(err)
	}
	recordsOut, err := streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: itOut.ShardIterator, Limit: aws.Int32(10)})
	if err != nil {
		t.Fatal(err)
	}
	if len(recordsOut.Records) != 3 {
		t.Fatalf("expected 3 stream records, got %d", len(recordsOut.Records))
	}
	if string(recordsOut.Records[0].EventName) != "INSERT" || string(recordsOut.Records[1].EventName) != "MODIFY" || string(recordsOut.Records[2].EventName) != "REMOVE" {
		t.Fatalf("unexpected stream event names: %+v", recordsOut.Records)
	}
	if recordsOut.Records[0].Dynamodb == nil || recordsOut.Records[0].Dynamodb.NewImage == nil || recordsOut.Records[0].Dynamodb.OldImage != nil {
		t.Fatalf("expected INSERT to include only NewImage, got %+v", recordsOut.Records[0].Dynamodb)
	}
	if recordsOut.Records[1].Dynamodb == nil || recordsOut.Records[1].Dynamodb.NewImage == nil || recordsOut.Records[1].Dynamodb.OldImage == nil {
		t.Fatalf("expected MODIFY to include both images, got %+v", recordsOut.Records[1].Dynamodb)
	}
	if recordsOut.Records[2].Dynamodb == nil || recordsOut.Records[2].Dynamodb.NewImage != nil || recordsOut.Records[2].Dynamodb.OldImage == nil {
		t.Fatalf("expected REMOVE to include only OldImage, got %+v", recordsOut.Records[2].Dynamodb)
	}
}

func TestStreamsLatestIteratorAndKeysOnlyView(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	ddb, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("streams_latest"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		KeySchema:   []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeKeysOnly,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_latest")})
	if err != nil {
		t.Fatal(err)
	}
	arn := *desc.Table.LatestStreamArn

	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(arn),
		ShardId:           streamDesc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: dstypes.ShardIteratorTypeLatest,
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String("streams_latest"),
		Item:      map[string]ddbtypes.AttributeValue{"pk": &ddbtypes.AttributeValueMemberS{Value: "a"}, "payload": &ddbtypes.AttributeValueMemberS{Value: "x"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: itOut.ShardIterator})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Records) != 1 {
		t.Fatalf("expected 1 record from LATEST iterator, got %d", len(out.Records))
	}
	if out.Records[0].Dynamodb == nil || out.Records[0].Dynamodb.Keys == nil {
		t.Fatalf("expected keys in KEYS_ONLY stream record, got %+v", out.Records[0].Dynamodb)
	}
	if out.Records[0].Dynamodb.NewImage != nil || out.Records[0].Dynamodb.OldImage != nil {
		t.Fatalf("expected no images in KEYS_ONLY stream, got %+v", out.Records[0].Dynamodb)
	}
}

func TestStreamsInvalidIteratorReturnsValidationException(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := streams.GetRecords(context.Background(), &dynamodbstreams.GetRecordsInput{ShardIterator: aws.String("not-a-token")})
	if err == nil {
		t.Fatal("expected ValidationException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %q", apiErr.ErrorCode())
	}
}

func TestStreamsExpiredIteratorReturnsExpiredIteratorException(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	key := []byte("streams-expiry-test-key")
	ddb, streams, cleanup := newStreamTestClients(t, WithStreamIteratorTTL(2*time.Millisecond), WithStreamIteratorSigningKey(key))
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("streams_expired"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_expired")})
	if err != nil {
		t.Fatal(err)
	}
	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.Table.LatestStreamArn})
	if err != nil {
		t.Fatal(err)
	}
	itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         desc.Table.LatestStreamArn,
		ShardId:           streamDesc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: dstypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := base64.RawURLEncoding.DecodeString(*itOut.ShardIterator)
	if err != nil {
		t.Fatal(err)
	}
	var envelope streamIteratorEnvelope
	if err := json.Unmarshal(decoded, &envelope); err != nil {
		t.Fatal(err)
	}
	envelope.Token.ExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	rawToken, err := json.Marshal(envelope.Token)
	if err != nil {
		t.Fatal(err)
	}
	sig := hmac.New(sha256.New, key)
	_, _ = sig.Write(rawToken)
	envelope.Sig = hex.EncodeToString(sig.Sum(nil))
	rawEnvelope, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	expiredIterator := base64.RawURLEncoding.EncodeToString(rawEnvelope)
	_, err = streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: aws.String(expiredIterator)})
	if err == nil {
		t.Fatal("expected ExpiredIteratorException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ExpiredIteratorException" {
		t.Fatalf("expected ExpiredIteratorException, got %q", apiErr.ErrorCode())
	}
}

func TestStreamsGetRecordsLimitOverMaxReturnsValidationException(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := streams.GetRecords(context.Background(), &dynamodbstreams.GetRecordsInput{ShardIterator: aws.String("not-a-token"), Limit: aws.Int32(1001)})
	if err == nil {
		t.Fatal("expected ValidationException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %q", apiErr.ErrorCode())
	}
}

func TestStreamsGetShardIteratorRejectsSequenceForLatest(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	ddb, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("streams_seq_validation"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_seq_validation")})
	if err != nil {
		t.Fatal(err)
	}
	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.Table.LatestStreamArn})
	if err != nil {
		t.Fatal(err)
	}

	_, err = streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         desc.Table.LatestStreamArn,
		ShardId:           streamDesc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: dstypes.ShardIteratorTypeLatest,
		SequenceNumber:    aws.String("1"),
	})
	if err == nil {
		t.Fatal("expected ValidationException")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %q", apiErr.ErrorCode())
	}
}

func TestStreamsIteratorTamperRejected(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	ddb, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("streams_tamper"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_tamper")})
	if err != nil {
		t.Fatal(err)
	}
	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.Table.LatestStreamArn})
	if err != nil {
		t.Fatal(err)
	}
	itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         desc.Table.LatestStreamArn,
		ShardId:           streamDesc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: dstypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatal(err)
	}
	tampered := *itOut.ShardIterator
	if strings.HasSuffix(tampered, "A") {
		tampered = tampered[:len(tampered)-1] + "B"
	} else {
		tampered = tampered[:len(tampered)-1] + "A"
	}

	_, err = streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: aws.String(tampered)})
	if err == nil {
		t.Fatal("expected ValidationException for tampered iterator")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "ValidationException" {
		t.Fatalf("expected ValidationException, got %q", apiErr.ErrorCode())
	}
}

func TestStreamsListStreamsNotCappedAtFirstPageOfTables(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	ddb, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	for i := 0; i < 105; i++ {
		_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName:            aws.String(fmt.Sprintf("streams_many_%03d", i)),
			AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
			KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
			BillingMode:          ddbtypes.BillingModePayPerRequest,
			StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeKeysOnly},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	var start *string
	for {
		out, err := streams.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{Limit: aws.Int32(25), ExclusiveStartStreamArn: start})
		if err != nil {
			t.Fatal(err)
		}
		count += len(out.Streams)
		if out.LastEvaluatedStreamArn == nil {
			break
		}
		start = out.LastEvaluatedStreamArn
	}
	if count != 105 {
		t.Fatalf("expected 105 streams, got %d", count)
	}
}

func TestStreamsGetRecordsRespectsOneMBResponseCap(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	ddb, streams, cleanup := newStreamTestClients(t)
	defer cleanup()

	_, err := ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("streams_size_cap"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeNewImage},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("x", 350000)
	for i := 0; i < 3; i++ {
		_, err := ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("streams_size_cap"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk":      &ddbtypes.AttributeValueMemberS{Value: fmt.Sprintf("k#%d", i)},
				"payload": &ddbtypes.AttributeValueMemberS{Value: payload},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	desc, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("streams_size_cap")})
	if err != nil {
		t.Fatal(err)
	}
	streamDesc, err := streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: desc.Table.LatestStreamArn})
	if err != nil {
		t.Fatal(err)
	}
	itOut, err := streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         desc.Table.LatestStreamArn,
		ShardId:           streamDesc.StreamDescription.Shards[0].ShardId,
		ShardIteratorType: dstypes.ShardIteratorTypeTrimHorizon,
	})
	if err != nil {
		t.Fatal(err)
	}
	out, err := streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: itOut.ShardIterator, Limit: aws.Int32(10)})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Records) >= 3 {
		t.Fatalf("expected response cap to truncate records, got %d", len(out.Records))
	}
}

func TestStreamsServiceSignatureAccepted(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	var h http.Handler = NewServer(store, nil)
	h = authentication.MakeSignatureMiddleware([]authentication.Credentials{{AccessKeyID: "test", SecretAccessKey: "test"}}, "eu-central-1", h)
	h = middleware.MakeRequestContextMiddleware(h)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	ddb := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	streams := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("streams_signed"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
		StreamSpecification:  &ddbtypes.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: ddbtypes.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := streams.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{TableName: aws.String("streams_signed")}); err != nil {
		t.Fatalf("expected signed dynamodbstreams request to succeed, got %v", err)
	}
}
