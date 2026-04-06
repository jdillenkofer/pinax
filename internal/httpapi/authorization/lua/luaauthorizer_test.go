package lua

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	testutils "github.com/jdillenkofer/pinax/internal/testing"
)

func TestAuthorizeRequestHelpers(t *testing.T) {
	testutils.SkipIfIntegration(t)

	script := `
function authorizeRequest(request)
  return request:isOperation("PutItem")
    and request:accessKeyIdEquals("akid")
    and request:tableHasPrefix("dev_")
    and request.httpRequest:isMethod("POST")
    and request.httpRequest:hostHasSuffix("localhost")
end
`

	a, err := NewLuaAuthorizer(script)
	if err != nil {
		t.Fatal(err)
	}
	akid := "akid"
	table := "dev_users"
	authorized, err := a.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: "PutItem",
		Authorization: authorization.Authorization{
			AccessKeyID: &akid,
		},
		TableName: &table,
		HTTPRequest: authorization.HTTPRequest{
			Method: "POST",
			Path:   "/",
			Host:   "dynamodb.localhost",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("expected request to be authorized")
	}
}

func TestForwardedClientIP(t *testing.T) {
	testutils.SkipIfIntegration(t)

	script := `
function authorizeRequest(request)
  return request.httpRequest:clientIPInCIDR("10.0.0.0/8")
end
`

	a, err := NewLuaAuthorizerWithOptions(script, Options{
		TrustForwardedHeaders: true,
		TrustedProxyCIDRs:     []string{"127.0.0.1/32"},
	})
	if err != nil {
		t.Fatal(err)
	}

	authorized, err := a.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: "GetItem",
		HTTPRequest: authorization.HTTPRequest{
			Method:   "POST",
			Path:     "/",
			Host:     "localhost",
			RemoteIP: ptr("127.0.0.1"),
			Headers: map[string][]string{
				"X-Forwarded-For": {"10.1.2.3"},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !authorized {
		t.Fatal("expected forwarded client ip to be authorized")
	}
}

func ptr(s string) *string { return &s }
