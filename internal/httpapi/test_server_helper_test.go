package httpapi

import (
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	reposqlite "github.com/jdillenkofer/pinax/internal/repo/sqlite"
)

func newTestServer(backend *reposqlite.Backend, requestAuthorizer authorization.RequestAuthorizer, opts ...ServerOption) *Server {
	return NewServer(backend.DB(), reposqlite.NewFactory(backend), requestAuthorizer, opts...)
}
