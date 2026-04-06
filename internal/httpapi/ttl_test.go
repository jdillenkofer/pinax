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
	"github.com/jdillenkofer/pinax/internal/store/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestTTLUpdateAndDescribe(t *testing.T) {
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
  return true
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
	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("test-ttl"),
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

	_, err = client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String("test-ttl"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			Enabled:       aws.Bool(true),
			AttributeName: aws.String("expires_at"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	descResp, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String("test-ttl"),
	})
	if err != nil {
		t.Fatal(err)
	}

	if descResp.TimeToLiveDescription == nil {
		t.Fatal("expected TimeToLiveDescription")
	}
	if descResp.TimeToLiveDescription.AttributeName == nil || *descResp.TimeToLiveDescription.AttributeName != "expires_at" {
		t.Errorf("expected attribute name expires_at, got %v", descResp.TimeToLiveDescription.AttributeName)
	}
	if descResp.TimeToLiveDescription.TimeToLiveStatus != types.TimeToLiveStatusEnabled {
		t.Errorf("expected status ENABLED, got %s", descResp.TimeToLiveDescription.TimeToLiveStatus)
	}
}
