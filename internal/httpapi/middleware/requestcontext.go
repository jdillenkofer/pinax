package middleware

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/logctx"
	"github.com/oklog/ulid/v2"
)

const requestIDHeader = "x-amz-request-id"

type requestIDContextKey struct{}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func MakeRequestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx := r.Context()
		requestID, ok := ctx.Value(authentication.RequestIDContextKey{}).(string)
		if !ok || requestID == "" {
			requestID = ulid.Make().String()
			ctx = context.WithValue(ctx, authentication.RequestIDContextKey{}, requestID)
		}
		if ip := getRemoteIP(r.RemoteAddr); ip != nil {
			ctx = context.WithValue(ctx, authentication.ClientIPContextKey{}, *ip)
		}
		ctx = context.WithValue(ctx, requestIDContextKey{}, requestID)

		w.Header().Set(requestIDHeader, requestID)
		wrappedWriter := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrappedWriter, r.WithContext(ctx))

		logctx.Logger(ctx).InfoContext(ctx, "Request completed", "method", r.Method, "host", r.Host, "path", r.URL.Path, "status", wrappedWriter.statusCode, "durationMs", time.Since(start).Milliseconds())
	})
}

func getRemoteIP(remoteAddr string) *string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil
	}
	ipString := ip.String()
	return &ipString
}
