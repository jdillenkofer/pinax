package logctx

import (
	"context"
	"log/slog"

	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
)

func RequestID(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	requestID, ok := ctx.Value(authentication.RequestIDContextKey{}).(string)
	if !ok || requestID == "" {
		return "", false
	}
	return requestID, true
}

func Logger(ctx context.Context) *slog.Logger {
	if requestID, ok := RequestID(ctx); ok {
		return slog.Default().With("request_id", requestID)
	}
	return slog.Default()
}
