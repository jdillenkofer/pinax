package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization/lua"
	"github.com/jdillenkofer/pinax/internal/httpapi/middleware"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestTableNamesAreScopedPerAccount(t *testing.T) {
	ctx := context.Background()
	serverURL := newSignedTestServer(t, []authentication.Credentials{
		{AccessKeyID: "akid-a", SecretAccessKey: "secret-a", AccountID: "111111111111"},
		{AccessKeyID: "akid-b", SecretAccessKey: "secret-b", AccountID: "222222222222"},
	})

	clientA := newDynamoClient(t, ctx, serverURL, "akid-a", "secret-a")
	clientB := newDynamoClient(t, ctx, serverURL, "akid-b", "secret-b")

	createSimpleTable(t, ctx, clientA, "shared")
	createSimpleTable(t, ctx, clientB, "shared")

	outA, err := clientA.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		t.Fatal(err)
	}
	if len(outA.TableNames) != 1 || outA.TableNames[0] != "shared" {
		t.Fatalf("unexpected account A tables: %#v", outA.TableNames)
	}

	descA, err := clientA.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("shared")})
	if err != nil {
		t.Fatal(err)
	}
	if descA.Table == nil || descA.Table.TableArn == nil || !strings.Contains(*descA.Table.TableArn, ":111111111111:table/shared") {
		t.Fatalf("unexpected account A table arn: %#v", descA.Table)
	}

	descB, err := clientB.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("shared")})
	if err != nil {
		t.Fatal(err)
	}
	if descB.Table == nil || descB.Table.TableArn == nil || !strings.Contains(*descB.Table.TableArn, ":222222222222:table/shared") {
		t.Fatalf("unexpected account B table arn: %#v", descB.Table)
	}
}

func TestCrossAccountTableARNIsRejected(t *testing.T) {
	ctx := context.Background()
	serverURL := newSignedTestServer(t, []authentication.Credentials{
		{AccessKeyID: "akid-a", SecretAccessKey: "secret-a", AccountID: "111111111111"},
		{AccessKeyID: "akid-b", SecretAccessKey: "secret-b", AccountID: "222222222222"},
	})

	clientA := newDynamoClient(t, ctx, serverURL, "akid-a", "secret-a")
	createSimpleTable(t, ctx, clientA, "shared")

	desc, err := clientA.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("shared")})
	if err != nil {
		t.Fatal(err)
	}
	badARN := strings.ReplaceAll(aws.ToString(desc.Table.TableArn), ":111111111111:", ":222222222222:")

	_, err = clientA.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{ResourceArn: aws.String(badARN)})
	if err == nil || !strings.Contains(err.Error(), "AccessDeniedException") {
		t.Fatalf("expected AccessDeniedException, got: %v", err)
	}
}

func TestUnsignedRequestRejectedWhenSignatureMiddlewareEnabled(t *testing.T) {
	serverURL := newSignedTestServer(t, []authentication.Credentials{{AccessKeyID: "akid-a", SecretAccessKey: "secret-a", AccountID: "111111111111"}})

	req, err := http.NewRequest(http.MethodPost, serverURL+"/", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, string(body))
	}
	if !strings.Contains(string(body), "AccessDeniedException") {
		t.Fatalf("expected AccessDeniedException, got body=%s", string(body))
	}
}

func newSignedTestServer(t *testing.T, creds []authentication.Credentials) string {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	authorizer, err := lua.NewLuaAuthorizer("function authorizeRequest(request) return true end")
	if err != nil {
		t.Fatal(err)
	}

	var h http.Handler = newTestServer(backend, authorizer)
	h = authentication.MakeSignatureMiddleware(creds, "eu-central-1", h)
	h = middleware.MakeRequestContextMiddleware(h)

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func newDynamoClient(t *testing.T, ctx context.Context, endpoint, accessKeyID, secret string) *dynamodb.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secret, "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func createSimpleTable(t *testing.T, ctx context.Context, client *dynamodb.Client, tableName string) {
	t.Helper()
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
}
