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
