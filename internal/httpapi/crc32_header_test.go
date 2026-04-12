package httpapi

import (
	"database/sql"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func TestDynamoResponsesIncludeCRC32Header(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(newTestServer(backend, nil))
	t.Cleanup(srv.Close)

	body := `{"TableName":"crc_users","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"BillingMode":"PAY_PER_REQUEST"}`
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	assertCRC32HeaderMatchesBody(t, resp)
}

func TestDynamoErrorResponsesIncludeCRC32Header(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(newTestServer(backend, nil))
	t.Cleanup(srv.Close)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Amz-Target", "bad-target")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	assertCRC32HeaderMatchesBody(t, resp)
}

func assertCRC32HeaderMatchesBody(t *testing.T, resp *http.Response) {
	t.Helper()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	got := resp.Header.Get("X-Amz-Crc32")
	if got == "" {
		t.Fatal("missing X-Amz-Crc32 response header")
	}
	want := strconv.FormatUint(uint64(crc32.ChecksumIEEE(bodyBytes)), 10)
	if got != want {
		t.Fatalf("unexpected X-Amz-Crc32: got %s want %s", got, want)
	}
}
