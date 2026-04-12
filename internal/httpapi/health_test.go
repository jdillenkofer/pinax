package httpapi

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestAPIServerDoesNotServeMonitoringEndpoints(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newTestServer(backend, nil))
	defer srv.Close()

	for _, path := range []string{"/health", "/metrics", "/healthz", "/ready", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 for %s, got %d", path, resp.StatusCode)
		}
	}
}

func TestMonitoringServerEndpoints(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := sqlite.New(db); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewMonitoringHandler(db))
	defer srv.Close()

	for _, path := range []string{"/health", "/metrics"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d", path, resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if len(body) == 0 {
			t.Fatalf("expected non-empty body for %s", path)
		}
		if path == "/metrics" {
			for _, metricName := range []string{
				"pinax_http_requests_total",
				"pinax_http_request_duration_seconds",
			} {
				if !strings.Contains(string(body), metricName) {
					t.Fatalf("expected metrics output to include %s", metricName)
				}
			}
		}
	}
}

func TestMonitoringHealthEndpointReturns503WhenDBUnavailable(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlite.New(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(NewMonitoringHandler(db))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}
