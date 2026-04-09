package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jdillenkofer/pinax/internal/httpapi"
	"github.com/jdillenkofer/pinax/internal/httpapi/authentication"
	"github.com/jdillenkofer/pinax/internal/httpapi/authorization"
	authorizationLua "github.com/jdillenkofer/pinax/internal/httpapi/authorization/lua"
	"github.com/jdillenkofer/pinax/internal/httpapi/middleware"
	"github.com/jdillenkofer/pinax/internal/settings"
	"github.com/jdillenkofer/pinax/internal/store/sqlite"
	"github.com/jdillenkofer/pinax/internal/ttl"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultAuthorizationCode = `
function authorizeRequest(request)
  return true
end
`
	defaultAuthorizationCodeWithCredentials = `
function authorizeRequest(request)
  return not request:isAnonymous()
end
`

	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 2 * time.Minute
	maxHeaderBytes    = 1 << 20
)

func main() {
	logLevelVar := setupLogging()

	s, err := settings.LoadSettings(os.Args[1:])
	if err != nil {
		slog.Error("could not load settings", "err", err)
		os.Exit(1)
	}
	logLevelVar.Set(s.LogLevel())

	addr := fmt.Sprintf("%s:%d", s.BindAddress(), s.Port())
	dbPath := s.DBPath()

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		slog.Error("could not open sqlite", "path", dbPath, "err", err)
		os.Exit(1)
	}
	defer db.Close()

	store, err := sqlite.New(db)
	if err != nil {
		slog.Error("could not initialize storage", "err", err)
		os.Exit(1)
	}

	requestAuthorizer, err := loadRequestAuthorizer(s.AuthorizerPath(), len(s.Credentials()) > 0, s.TrustForwardedHeaders(), s.TrustedProxyCIDRs())
	if err != nil {
		slog.Error("could not create request authorizer", "err", err)
		os.Exit(1)
	}

	srv := httpapi.NewServer(store, requestAuthorizer, httpapi.WithPITRLatestRestorableLagMillis(s.PITRLatestRestorableLagMillis()))

	var rootHandler http.Handler = srv
	if s.AuthenticationEnabled() {
		authCreds := make([]authentication.Credentials, 0, len(s.Credentials()))
		for _, cred := range s.Credentials() {
			authCreds = append(authCreds, authentication.Credentials{AccessKeyID: cred.AccessKeyId, SecretAccessKey: cred.SecretAccessKey})
		}
		rootHandler = authentication.MakeSignatureMiddleware(authCreds, s.Region(), rootHandler)
	} else {
		slog.Warn("authentication disabled")
	}
	rootHandler = middleware.MakeRequestContextMiddleware(rootHandler)

	var sweeper *ttl.Sweeper
	if s.TTLSweeperEnabled() {
		sweeper = ttl.NewSweeper(store, s.TTLSweeperInterval())
		go sweeper.Start(context.Background())
		slog.Info("TTL sweeper started", "interval", s.TTLSweeperInterval())
	}

	httpServer := &http.Server{
		BaseContext:       func(net.Listener) context.Context { return context.Background() },
		Addr:              addr,
		Handler:           rootHandler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}

	slog.Info("listening", "addr", addr, "dbPath", dbPath)
	if err := httpServer.ListenAndServe(); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func setupLogging() *slog.LevelVar {
	logLevelVar := new(slog.LevelVar)
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{AddSource: true, Level: logLevelVar})
	slog.SetDefault(slog.New(handler))
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
	return logLevelVar
}

func loadRequestAuthorizer(path string, hasCredentials bool, trustForwardedHeaders bool, trustedProxyCIDRs []string) (authorization.RequestAuthorizer, error) {
	code, err := os.ReadFile(path)
	if err != nil {
		if hasCredentials {
			return authorizationLua.NewLuaAuthorizerWithOptions(defaultAuthorizationCodeWithCredentials, authorizationLua.Options{TrustForwardedHeaders: trustForwardedHeaders, TrustedProxyCIDRs: trustedProxyCIDRs})
		}
		return authorizationLua.NewLuaAuthorizerWithOptions(defaultAuthorizationCode, authorizationLua.Options{TrustForwardedHeaders: trustForwardedHeaders, TrustedProxyCIDRs: trustedProxyCIDRs})
	}
	return authorizationLua.NewLuaAuthorizerWithOptions(string(code), authorizationLua.Options{TrustForwardedHeaders: trustForwardedHeaders, TrustedProxyCIDRs: trustedProxyCIDRs})
}
