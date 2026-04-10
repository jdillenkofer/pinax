package httpapi

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization/lua"
	"github.com/jdillenkofer/pinax/internal/httpapi/middleware"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestResourcePolicyLifecycleAndRevisionSemantics(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_table"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_table")})
	if err != nil {
		t.Fatal(err)
	}
	arn := *desc.Table.TableArn

	policy1 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["dynamodb:GetItem"],"Resource":"` + arn + `"}]}`
	policy2 := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["dynamodb:PutItem"],"Resource":"` + arn + `"}]}`

	put1, err := client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(policy1)})
	if err != nil {
		t.Fatal(err)
	}
	if put1.RevisionId == nil || *put1.RevisionId == "" {
		t.Fatalf("expected revision id, got %+v", put1)
	}

	putSame, err := client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(policy1)})
	if err != nil {
		t.Fatal(err)
	}
	if putSame.RevisionId == nil || *putSame.RevisionId != *put1.RevisionId {
		t.Fatalf("expected idempotent revision id %q, got %+v", *put1.RevisionId, putSame)
	}

	get1, err := client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	if get1.Policy == nil || *get1.Policy != policy1 {
		t.Fatalf("unexpected policy payload: %+v", get1)
	}

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn:        aws.String(arn),
		Policy:             aws.String(policy2),
		ExpectedRevisionId: aws.String("wrong"),
	})
	assertAPIErrorCode(t, err, "PolicyNotFoundException")
	assertAPIErrorMessageContains(t, err, "nonexistent resource-based policy")

	put2, err := client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn:        aws.String(arn),
		Policy:             aws.String(policy2),
		ExpectedRevisionId: put1.RevisionId,
	})
	if err != nil {
		t.Fatal(err)
	}
	if put2.RevisionId == nil || *put2.RevisionId == *put1.RevisionId {
		t.Fatalf("expected new revision id after policy change, got %+v", put2)
	}

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn:        aws.String(arn),
		Policy:             aws.String(policy2),
		ExpectedRevisionId: aws.String("NO_POLICY"),
	})
	assertAPIErrorCode(t, err, "PolicyNotFoundException")
	assertAPIErrorMessageContains(t, err, "nonexistent resource-based policy")

	_, err = client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{ResourceArn: aws.String(arn), ExpectedRevisionId: aws.String("wrong")})
	assertAPIErrorCode(t, err, "PolicyNotFoundException")
	assertAPIErrorMessageContains(t, err, "nonexistent resource-based policy")

	del1, err := client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{ResourceArn: aws.String(arn), ExpectedRevisionId: put2.RevisionId})
	if err != nil {
		t.Fatal(err)
	}
	if del1.RevisionId == nil || *del1.RevisionId != *put2.RevisionId {
		t.Fatalf("expected deleted revision id %q, got %+v", *put2.RevisionId, del1)
	}

	del2, err := client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{ResourceArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	if del2.RevisionId == nil || *del2.RevisionId != "" {
		t.Fatalf("expected empty revision id for idempotent delete, got %+v", del2)
	}

	_, err = client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(arn)})
	assertAPIErrorCode(t, err, "PolicyNotFoundException")

	putNoPolicy, err := client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn:        aws.String(arn),
		Policy:             aws.String(policy1),
		ExpectedRevisionId: aws.String("NO_POLICY"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if putNoPolicy.RevisionId == nil || *putNoPolicy.RevisionId == "" {
		t.Fatalf("expected revision id with NO_POLICY, got %+v", putNoPolicy)
	}
}

func TestResourcePolicyValidationAndResourceNotFound(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	missingARN := "arn:aws:dynamodb:local:000000000000:table/missing-policy"
	_, err := client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(missingARN)})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: aws.String(missingARN),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/x"),
		Policy:      aws.String("not json"),
	})
	assertAPIErrorCode(t, err, "ValidationException")

	longPolicy := strings.Repeat(" ", 20481)
	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: aws.String("arn:aws:dynamodb:local:000000000000:table/x"),
		Policy:      aws.String(longPolicy),
	})
	assertAPIErrorCode(t, err, "ValidationException")
}

func TestResourcePolicyConfirmRemoveSelfResourceAccessEnforced(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_confirm"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_confirm")})
	if err != nil {
		t.Fatal(err)
	}
	arn := *desc.Table.TableArn
	denyPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":["dynamodb:PutResourcePolicy"],"Resource":"` + arn + `"}]}`

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(denyPolicy)})
	assertAPIErrorCode(t, err, "ValidationException")
	assertAPIErrorMessageContains(t, err, "ConfirmRemoveSelfResourceAccess")

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(denyPolicy), ConfirmRemoveSelfResourceAccess: true})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResourcePolicyConfirmRemoveSelfWithNotActionAndNotPrincipal(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_confirm_notaction"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_confirm_notaction")})
	if err != nil {
		t.Fatal(err)
	}
	arn := *desc.Table.TableArn
	denyByNotAction := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","NotAction":"dynamodb:GetItem","Resource":"` + arn + `"}]}`

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(denyByNotAction)})
	assertAPIErrorCode(t, err, "ValidationException")
	assertAPIErrorMessageContains(t, err, "ConfirmRemoveSelfResourceAccess")

	root := "arn:aws:iam::000000000000:root"
	notPrincipalExcludesSelf := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","NotPrincipal":{"AWS":["` + root + `"]},"Action":"dynamodb:PutResourcePolicy","Resource":"` + arn + `"}]}`
	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(arn), Policy: aws.String(notPrincipalExcludesSelf)})
	if err != nil {
		t.Fatalf("expected no confirm requirement when root excluded via NotPrincipal, got %v", err)
	}
}

func TestResourcePolicySupportsStreamARN(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_stream_table"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
		StreamSpecification:  &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_stream_table")})
	if err != nil {
		t.Fatal(err)
	}
	if desc.Table.LatestStreamArn == nil {
		t.Fatalf("expected stream arn, got %+v", desc.Table)
	}
	streamARN := *desc.Table.LatestStreamArn
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":["dynamodb:GetRecords"],"Resource":"` + streamARN + `"}]}`

	putOut, err := client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{ResourceArn: aws.String(streamARN), Policy: aws.String(policy)})
	if err != nil {
		t.Fatal(err)
	}
	if putOut.RevisionId == nil || *putOut.RevisionId == "" {
		t.Fatalf("expected revision id, got %+v", putOut)
	}

	getOut, err := client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(streamARN)})
	if err != nil {
		t.Fatal(err)
	}
	if getOut.Policy == nil || *getOut.Policy != policy {
		t.Fatalf("expected policy payload for stream arn, got %+v", getOut)
	}

	_, err = client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{ResourceArn: aws.String(streamARN), ExpectedRevisionId: putOut.RevisionId})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResourcePolicyOldStreamArnNotFoundAfterDisableAndReenable(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	client, cleanup := newTestClient(t)
	defer cleanup()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_stream_recycle"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
		StreamSpecification:  &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeKeysOnly},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc1, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_stream_recycle")})
	if err != nil {
		t.Fatal(err)
	}
	oldStreamARN := *desc1.Table.LatestStreamArn

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: aws.String(oldStreamARN),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"dynamodb:GetRecords","Resource":"` + oldStreamARN + `"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:           aws.String("policy_stream_recycle"),
		StreamSpecification: &types.StreamSpecification{StreamEnabled: aws.Bool(false)},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(oldStreamARN)})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	_, err = client.DeleteResourcePolicy(ctx, &dynamodb.DeleteResourcePolicyInput{ResourceArn: aws.String(oldStreamARN)})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:           aws.String("policy_stream_recycle"),
		StreamSpecification: &types.StreamSpecification{StreamEnabled: aws.Bool(true), StreamViewType: types.StreamViewTypeNewImage},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc2, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_stream_recycle")})
	if err != nil {
		t.Fatal(err)
	}
	if desc2.Table.LatestStreamArn == nil || *desc2.Table.LatestStreamArn == oldStreamARN {
		t.Fatalf("expected new stream arn after re-enable, got old=%q new=%+v", oldStreamARN, desc2.Table.LatestStreamArn)
	}

	_, err = client.GetResourcePolicy(ctx, &dynamodb.GetResourcePolicyInput{ResourceArn: aws.String(oldStreamARN)})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestResourcePolicyDoesNotAffectLuaAuthorization(t *testing.T) {
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

	authorizer, err := lua.NewLuaAuthorizer(`
function authorizeRequest(request)
  if request:isOperation("CreateTable") then
    return true
  end
  if request:isOperation("PutResourcePolicy") then
    return true
  end
  return request:isReadOnly()
end
`)
	if err != nil {
		t.Fatal(err)
	}

	var h http.Handler = NewServer(store, authorizer)
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
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:            aws.String("policy_lua"),
		AttributeDefinitions: []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS}},
		KeySchema:            []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}},
		BillingMode:          types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	desc, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("policy_lua")})
	if err != nil {
		t.Fatal(err)
	}
	arn := *desc.Table.TableArn

	_, err = client.PutResourcePolicy(ctx, &dynamodb.PutResourcePolicyInput{
		ResourceArn: aws.String(arn),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"dynamodb:PutItem","Resource":"` + arn + `"}]}`),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("policy_lua"), Item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "x"}}})
	if err == nil {
		t.Fatal("expected PutItem to be denied by lua authorizer")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "AccessDeniedException" {
		t.Fatalf("expected AccessDeniedException, got %q", apiErr.ErrorCode())
	}
}

func assertAPIErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s", code)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != code {
		t.Fatalf("expected %s, got %q", code, apiErr.ErrorCode())
	}
}

func assertAPIErrorMessageContains(t *testing.T, err error, fragment string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected API error, got %T: %v", err, err)
	}
	if !strings.Contains(apiErr.ErrorMessage(), fragment) {
		t.Fatalf("expected error message containing %q, got %q", fragment, apiErr.ErrorMessage())
	}
}
