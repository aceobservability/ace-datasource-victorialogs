package victorialogs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

func TestNew_requiresHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := New("http://localhost:9428", nil)
	if err == nil {
		t.Fatal("expected error for nil http client")
	}
	if client != nil {
		t.Fatal("expected nil client when http client is missing")
	}
	if !strings.Contains(err.Error(), "http client is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_streamClientTimeoutZero(t *testing.T) {
	t.Parallel()

	httpClient := &http.Client{Timeout: 30 * time.Second}
	client, err := New("http://localhost:9428", httpClient)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client.HTTPClient().Timeout != 30*time.Second {
		t.Fatalf("HTTPClient timeout = %s", client.HTTPClient().Timeout)
	}
	if client.StreamHTTPClient().Timeout != 0 {
		t.Fatalf("StreamHTTPClient timeout = %s, want 0", client.StreamHTTPClient().Timeout)
	}
}

func TestQueryLabelsAndTestConnection_againstFixtureHTTP(t *testing.T) {
	t.Parallel()

	var sawQuery, sawHealth, sawFieldNames, sawFieldValues bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/select/logsql/query":
			sawQuery = true
			if got := r.URL.Query().Get("query"); got != "*" {
				t.Errorf("query=%q, want *", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"_msg":"boom","_time":"2026-02-08T12:00:00Z","service":"api","level":"error"}`+"\n")
		case r.URL.Path == "/health":
			sawHealth = true
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "OK")
		case r.URL.Path == "/select/logsql/field_names":
			sawFieldNames = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[{"value":"level"},{"value":"service"}]}`)
		case r.URL.Path == "/select/logsql/field_values":
			sawFieldValues = true
			if got := r.URL.Query().Get("field"); got != "service" {
				t.Errorf("field=%q, want service", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[{"value":"api"},{"value":"web"}]}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Unix(1600000000, 0).Add(-time.Hour)
	end := time.Unix(1600000000, 0)
	result, err := client.Query(ctx, "*", start, end, time.Minute, 10)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("Query status=%q error=%q", result.Status, result.Error)
	}
	if result.ResultType != "logs" {
		t.Fatalf("ResultType=%q, want logs", result.ResultType)
	}
	if result.Data == nil || len(result.Data.Logs) != 1 || result.Data.Logs[0].Line != "boom" {
		t.Fatalf("unexpected logs %+v", result.Data)
	}
	if result.Data.Logs[0].Labels["service"] != "api" {
		t.Fatalf("labels=%v", result.Data.Logs[0].Labels)
	}
	if result.Data.Logs[0].Level != "error" {
		t.Fatalf("level=%q", result.Data.Logs[0].Level)
	}
	if !sawQuery {
		t.Fatal("expected fixture to receive /select/logsql/query")
	}

	labels, err := client.Labels(ctx)
	if err != nil {
		t.Fatalf("Labels: %v", err)
	}
	if len(labels) != 2 || labels[0] != "level" || labels[1] != "service" {
		t.Fatalf("Labels=%v", labels)
	}
	if !sawFieldNames {
		t.Fatal("expected Labels to hit /select/logsql/field_names")
	}

	values, err := client.LabelValues(ctx, "service")
	if err != nil {
		t.Fatalf("LabelValues: %v", err)
	}
	if len(values) != 2 || values[0] != "api" {
		t.Fatalf("LabelValues=%v", values)
	}
	if !sawFieldValues {
		t.Fatal("expected LabelValues to hit /select/logsql/field_values")
	}

	if err := client.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if !sawHealth {
		t.Fatal("expected TestConnection to hit /health")
	}
}

func TestTestConnection_usesHealthThenFieldNamesFallback(t *testing.T) {
	t.Parallel()

	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Path == "/select/logsql/field_names" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"values":[]}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.TestConnection(ctx); err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if len(paths) < 2 || paths[0] != "/health" {
		t.Fatalf("paths=%v, want /health then field_names", paths)
	}
}

func TestStream_againstFixtureHTTP(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/select/logsql/tail" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("method=%s, want POST", r.Method)
		}
		_ = r.ParseForm()
		if got := r.Form.Get("query"); got != `service:api` {
			t.Errorf("query=%q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"_msg":"hello","_time":"2026-02-08T12:00:00Z","service":"api"}`+"\n")
	}))
	t.Cleanup(srv.Close)

	client, err := New(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var gotLine string
	err = client.Stream(ctx, `service:api`, time.Time{}, 1, func(entry datasource.LogEntry) error {
		gotLine = entry.Line
		cancel()
		return nil
	})
	if err != nil && ctx.Err() == nil {
		t.Fatalf("Stream: %v", err)
	}
	if gotLine != "hello" {
		t.Fatalf("line=%q, want hello", gotLine)
	}
}

func TestParseVictoriaLogsLine(t *testing.T) {
	t.Parallel()

	entry, ok := parseVictoriaLogsLine(`{"_msg":"boom","_time":"2026-02-08T12:00:00Z","service":"api","level":"error"}`)
	if !ok {
		t.Fatal("expected line to parse")
	}

	if entry.Line != "boom" {
		t.Fatalf("expected line to be boom, got %q", entry.Line)
	}
	if entry.Timestamp != "2026-02-08T12:00:00Z" {
		t.Fatalf("expected timestamp to match, got %q", entry.Timestamp)
	}
	if entry.Labels["service"] != "api" {
		t.Fatalf("expected service label api, got %q", entry.Labels["service"])
	}
	if entry.Level != "error" {
		t.Fatalf("expected level error, got %q", entry.Level)
	}
}

func TestParseVictoriaLogsLineInvalid(t *testing.T) {
	t.Parallel()

	if _, ok := parseVictoriaLogsLine(`not-json`); ok {
		t.Fatal("expected invalid line to fail parsing")
	}
}
