package httpapi

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pinax/internal/repo/sqlite"

	_ "github.com/mattn/go-sqlite3"
)

func BenchmarkAPIPutItem(b *testing.B) {
	srv := newBenchmarkServer(b)
	setupBenchTableAndSeedItems(b, srv, 1)

	body := []byte(`{"TableName":"bench_items","Item":{"pk":{"S":"acct#1"},"sk":{"S":"item#bench"},"payload":{"S":"benchmark-payload"}}}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchOperation(b, srv, "PutItem", body)
	}
}

func BenchmarkAPIQuery(b *testing.B) {
	srv := newBenchmarkServer(b)
	setupBenchTableAndSeedItems(b, srv, 200)

	body := []byte(`{"TableName":"bench_items","KeyConditionExpression":"pk = :pk","ExpressionAttributeValues":{":pk":{"S":"acct#1"}},"Limit":100}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchOperation(b, srv, "Query", body)
	}
}

func BenchmarkAPITransactWriteItems(b *testing.B) {
	srv := newBenchmarkServer(b)
	setupBenchTableAndSeedItems(b, srv, 1)

	body := []byte(`{"TransactItems":[{"Put":{"TableName":"bench_items","Item":{"pk":{"S":"acct#1"},"sk":{"S":"txn#a"},"payload":{"S":"x"}}}},{"Put":{"TableName":"bench_items","Item":{"pk":{"S":"acct#1"},"sk":{"S":"txn#b"},"payload":{"S":"y"}}}}]}`)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchOperation(b, srv, "TransactWriteItems", body)
	}
}

func newBenchmarkServer(b *testing.B) *Server {
	b.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	backend, err := sqlite.New(db)
	if err != nil {
		b.Fatal(err)
	}

	return newTestServer(backend, nil)
}

func setupBenchTableAndSeedItems(b *testing.B, srv *Server, itemCount int) {
	b.Helper()

	createTableBody := []byte(`{"TableName":"bench_items","AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"S"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}],"BillingMode":"PAY_PER_REQUEST"}`)
	benchOperation(b, srv, "CreateTable", createTableBody)

	for i := 0; i < itemCount; i++ {
		putBody := []byte(fmt.Sprintf(`{"TableName":"bench_items","Item":{"pk":{"S":"acct#1"},"sk":{"S":"item#%04d"},"payload":{"S":"payload-%04d"}}}`, i, i))
		benchOperation(b, srv, "PutItem", putBody)
	}
}

func benchOperation(b *testing.B, srv *Server, operation string, body []byte) []byte {
	b.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+operation)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		b.Fatalf("%s failed: status=%d body=%s", operation, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}
