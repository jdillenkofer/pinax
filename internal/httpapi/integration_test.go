package httpapi

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
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
	testutils "github.com/jdillenkofer/pinax/internal/testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestIntegrationSignedRequestAllowed(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

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

	authorizer, err := lua.NewLuaAuthorizer(`
function authorizeRequest(request)
  return not request:isAnonymous()
end
`)
	if err != nil {
		t.Fatal(err)
	}

	var h http.Handler = newTestServer(backend, authorizer)
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
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("users"),
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

func TestIntegrationSignedRequestDeniedForWrongSecret(t *testing.T) {
	testutils.SkipIfNotIntegration(t)

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

	authorizer, err := lua.NewLuaAuthorizer(`
function authorizeRequest(request)
  return not request:isAnonymous()
end
`)
	if err != nil {
		t.Fatal(err)
	}

	var h http.Handler = newTestServer(backend, authorizer)
	h = authentication.MakeSignatureMiddleware([]authentication.Credentials{{AccessKeyID: "test", SecretAccessKey: "test"}}, "eu-central-1", h)
	h = middleware.MakeRequestContextMiddleware(h)

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("eu-central-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "wrong", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err == nil {
		t.Fatal("expected unauthorized error")
	}
}
