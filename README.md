# pinax

`pinax` is a local reimplementation of DynamoDB with an AWS-compatible JSON API.

## Current scope

- SQLite storage backend (`mattn/go-sqlite3`)
- Embedded SQL migrations (`golang-migrate` + `iofs`)
- DynamoDB-style target routing (`X-Amz-Target`)
- Implemented operations:
  - `CreateTable`, `DescribeTable`, `ListTables`, `DeleteTable`
  - `PutItem`, `GetItem`, `DeleteItem`, `UpdateItem`
  - `Query`, `Scan`
  - `BatchGetItem`, `BatchWriteItem`
- SigV4 authentication middleware
- Lua-based request authorization

## Run

```bash
go run ./cmd/pinax
```

Server defaults:

- bind address: `0.0.0.0`
- port: `8000`
- region: `eu-central-1`
- sqlite db: `./pinax.db`

## Settings

Settings are loaded from command-line flags and environment variables.
Environment values override only explicitly set fields, matching the merge model used in `pithos`.

### Core flags

- `-authenticationEnabled`
- `-region`
- `-bindAddress`
- `-port`
- `-dbPath`
- `-authorizerPath`
- `-trustForwardedHeaders`
- `-trustedProxyCIDRs`
- `-logLevel`

### Core environment variables

- `PINAX_AUTHENTICATION_ENABLED`
- `PINAX_REGION`
- `PINAX_BIND_ADDRESS`
- `PINAX_PORT`
- `PINAX_DB_PATH`
- `PINAX_AUTHORIZER_PATH`
- `PINAX_TRUST_FORWARDED_HEADERS`
- `PINAX_TRUSTED_PROXY_CIDRS`
- `PINAX_LOG_LEVEL`
- credentials:
  - `PINAX_CREDENTIALS_0_ACCESS_KEY_ID`
  - `PINAX_CREDENTIALS_0_SECRET_ACCESS_KEY`
  - `PINAX_CREDENTIALS_1_ACCESS_KEY_ID`
  - ...

## Authorization script

If no authorizer file is found:

- with credentials configured: only authenticated requests are allowed
- without credentials configured: all requests are allowed

Example authorizer (`authorizer.lua`):

```lua
function authorizeRequest(request)
  return request:hasAccessKeyId()
    and request:isOperationIn({"GetItem", "Query", "Scan"})
    and request:tableHasPrefix("dev_")
end
```

## Testing

Unit tests:

```bash
go test ./...
```

Integration tests (mirrors `pithos` style flag):

```bash
go test ./... -integration
```

Run conformance checks against DynamoDB Local (requires local container on `http://localhost:8000` or custom endpoint):

```bash
PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT=http://localhost:8000 go test ./internal/httpapi -integration -run Conformance
```

Run the optional stress conformance profile (larger pagination and concurrent writes):

```bash
PINAX_CONFORMANCE_DDB_LOCAL_ENDPOINT=http://localhost:8000 PINAX_CONFORMANCE_STRESS=1 go test ./internal/httpapi -integration -run ConformanceStress
```

Known conformance differences are tracked in `internal/httpapi/testdata/conformance_known_differences.json`.
When extending conformance coverage, prefer fixing parity gaps first and only add entries to this file when the difference is intentional or externally constrained.
Stale or unknown entries are treated as test failures to keep the registry current.

## Optimistic locking

`pinax` follows the DynamoDB optimistic locking pattern using standard condition and update expressions.
It does not provide a custom versioning API and does not auto-manage version attributes.

Recommended pattern:

- create: `ConditionExpression` with `attribute_not_exists(pk)` and set `version` to `1`
- update: `ConditionExpression` with `version = :expected` and bump version in the same write
- delete: `ConditionExpression` with `version = :expected`

Single-item optimistic update example:

```text
UpdateExpression:    SET #v = #v + :one, #payload = :payload
ConditionExpression: #v = :expected
ExpressionAttributeNames:
  #v -> version
  #payload -> payload
ExpressionAttributeValues:
  :one -> {"N":"1"}
  :expected -> {"N":"7"}
  :payload -> {"S":"next"}
```

Failure semantics:

- stale single-item writes return `ConditionalCheckFailedException`
- stale transactional writes return `TransactionCanceledException` with ordered cancellation reasons

Transactional optimistic update example:

```text
TransactWriteItems:
  - Update:
      TableName: users
      Key: { pk: {"S":"u#1"} }
      UpdateExpression: SET #v = #v + :one, #state = :next
      ConditionExpression: #v = :expected
      ExpressionAttributeNames:
        #v -> version
        #state -> state
      ExpressionAttributeValues:
        :one -> {"N":"1"}
        :expected -> {"N":"12"}
        :next -> {"S":"active"}
```

Application retry guidance:

- on stale write errors, re-read the item, re-apply business logic, and retry with the new expected version
- do not rely on implicit version management; keep version writes explicit in expressions
