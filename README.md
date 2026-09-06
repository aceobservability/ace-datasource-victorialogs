# ace-datasource-victorialogs

Compile-time VictoriaLogs datasource module for [Ace](https://github.com/aceobservability/ace).

Ace keeps the datasource contract and registry in
`github.com/aceobservability/ace/backend/pkg/datasource`. This module implements
that `Client` (plus live tail, field names/values, and connection test). Ace
registers the factory at `init` and injects its SSRF-safe HTTP client — this
module does not import Ace `internal/` packages and does not construct an
unpolicy'd client.

## Contract

| Surface | Package |
| --- | --- |
| Query / result types | `github.com/aceobservability/ace/backend/pkg/datasource` |
| Registry type key | `victorialogs` (`Type`) |
| Factory | `New(url string, httpClient *http.Client)` |

`httpClient` is required. Ace passes `ssrf.DatasourceClient` wrapped with stored
datasource credentials. Live tail uses a clone of that client with `Timeout` 0.

## Tests

```
go test ./...
```

Query, stream, labels, and connection tests speak to an `httptest` fixture. No
live VictoriaLogs is required.
