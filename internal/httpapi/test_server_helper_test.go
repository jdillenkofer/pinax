package httpapi

import (
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	"github.com/jdillenkofer/pinax/internal/repo/sqlite"
)

func newTestServer(backend *sqlite.Backend, requestAuthorizer authorization.RequestAuthorizer, opts ...ServerOption) *Server {
	unitOfWork := sqlite.NewUnitOfWork(backend.DB(), sqlite.NewFactory(backend))
	return NewServer(unitOfWork, requestAuthorizer, opts...)
}
