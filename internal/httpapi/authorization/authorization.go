package authorization

import "context"

type Authorization struct {
	AccessKeyID *string
}

type HTTPRequest struct {
	Method      string
	Path        string
	Query       string
	QueryParams map[string][]string
	Headers     map[string][]string
	Host        string
	Proto       string
	RemoteAddr  string
	RemoteIP    *string
	ClientIP    *string
	Scheme      string
}

type Request struct {
	Operation     string
	Authorization Authorization
	TableName     *string
	HTTPRequest   HTTPRequest
}

type RequestAuthorizer interface {
	AuthorizeRequest(ctx context.Context, request *Request) (bool, error)
}
