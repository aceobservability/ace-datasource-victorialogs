package victorialogs

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aceobservability/ace/backend/pkg/datasource"
)

// Type is the RegisterDatasource key Ace uses for this module.
const Type = "victorialogs"

// Client queries VictoriaLogs using LogsQL.
type Client struct {
	url          string
	httpClient   *http.Client
	streamClient *http.Client
}

// New constructs a VictoriaLogs datasource client.
// httpClient is required so Ace can inject DatasourceClient (dial/redirect policy + auth).
func New(baseURL string, httpClient *http.Client) (*Client, error) {
	if httpClient == nil {
		return nil, fmt.Errorf("http client is required")
	}
	stream := *httpClient
	stream.Timeout = 0
	return &Client{
		url:          baseURL,
		httpClient:   httpClient,
		streamClient: &stream,
	}, nil
}

// HTTPClient returns the injected HTTP client. Ace SSRF tests inspect policy wiring.
func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

// StreamHTTPClient returns the long-lived tail client (Timeout 0, same transport).
func (c *Client) StreamHTTPClient() *http.Client {
	return c.streamClient
}

type victoriaLogsFieldNamesResponse struct {
	Values []struct {
		Value string `json:"value"`
	} `json:"values"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

type victoriaLogsFieldValuesResponse struct {
	Values []struct {
		Value string `json:"value"`
	} `json:"values"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (c *Client) Labels(ctx context.Context) ([]string, error) {
	params := url.Values{}
	params.Set("query", "*")

	reqURL := fmt.Sprintf("%s/select/logsql/field_names?%s", c.url, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create field names request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Victoria Logs field names: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read field names response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("victoria logs field names request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var fieldResp victoriaLogsFieldNamesResponse
	if err := json.Unmarshal(body, &fieldResp); err != nil {
		return nil, fmt.Errorf("failed to parse field names response: %w", err)
	}

	if fieldResp.Status == "error" {
		return nil, fmt.Errorf("victoria logs field names request failed: %s", fieldResp.Error)
	}

	labels := make([]string, 0, len(fieldResp.Values))
	for _, value := range fieldResp.Values {
		trimmed := strings.TrimSpace(value.Value)
		if trimmed == "" {
			continue
		}
		labels = append(labels, trimmed)
	}

	sort.Strings(labels)

	return labels, nil
}

func (c *Client) LabelValues(ctx context.Context, labelName string) ([]string, error) {
	trimmedLabel := strings.TrimSpace(labelName)
	if trimmedLabel == "" {
		return nil, fmt.Errorf("field name is required")
	}

	params := url.Values{}
	params.Set("query", "*")
	params.Set("field", trimmedLabel)
	params.Set("limit", "1000")

	reqURL := fmt.Sprintf("%s/select/logsql/field_values?%s", c.url, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create field values request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Victoria Logs field values: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read field values response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("victoria logs field values request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var valuesResp victoriaLogsFieldValuesResponse
	if err := json.Unmarshal(body, &valuesResp); err != nil {
		return nil, fmt.Errorf("failed to parse field values response: %w", err)
	}

	if valuesResp.Status == "error" {
		return nil, fmt.Errorf("victoria logs field values request failed: %s", valuesResp.Error)
	}

	valuesSet := make(map[string]struct{}, len(valuesResp.Values))
	for _, value := range valuesResp.Values {
		trimmed := strings.TrimSpace(value.Value)
		if trimmed == "" {
			continue
		}
		valuesSet[trimmed] = struct{}{}
	}

	values := make([]string, 0, len(valuesSet))
	for value := range valuesSet {
		values = append(values, value)
	}

	sort.Strings(values)

	return values, nil
}

func (c *Client) Query(ctx context.Context, query string, start, end time.Time, step time.Duration, limit int) (*datasource.QueryResult, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("start", start.UTC().Format(time.RFC3339))
	params.Set("end", end.UTC().Format(time.RFC3339))
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	} else {
		params.Set("limit", "1000")
	}

	reqURL := fmt.Sprintf("%s/select/logsql/query?%s", c.url, params.Encode())
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query Victoria Logs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &datasource.QueryResult{
			Status:     "error",
			Error:      fmt.Sprintf("Victoria Logs returned status %d", resp.StatusCode),
			ResultType: "logs",
		}, nil
	}

	logs := []datasource.LogEntry{}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	for scanner.Scan() {
		entry, ok := parseVictoriaLogsLine(scanner.Text())
		if !ok {
			continue
		}

		logs = append(logs, entry)
	}

	return &datasource.QueryResult{
		Status:     "success",
		ResultType: "logs",
		Data: &datasource.QueryData{
			ResultType: "streams",
			Logs:       logs,
		},
	}, nil
}

func (c *Client) Stream(ctx context.Context, query string, start time.Time, limit int, onLog datasource.LogStreamCallback) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query is required")
	}
	if onLog == nil {
		return fmt.Errorf("stream callback is required")
	}
	_ = limit

	params := url.Values{}
	params.Set("query", query)
	params.Set("offset", "1s")
	params.Set("refresh_interval", "1s")

	if !start.IsZero() {
		startOffset := time.Since(start)
		if startOffset < time.Second {
			startOffset = time.Second
		}
		params.Set("start_offset", startOffset.Round(time.Second).String())
	}

	reqURL := fmt.Sprintf("%s/select/logsql/tail", c.url)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, strings.NewReader(params.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create tail request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.streamClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to stream Victoria Logs tail: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("victoria logs tail request failed with status %d", resp.StatusCode)
		}
		return fmt.Errorf("victoria logs tail request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	for scanner.Scan() {
		entry, ok := parseVictoriaLogsLine(scanner.Text())
		if !ok {
			continue
		}

		if err := onLog(entry); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to read Victoria Logs tail response: %w", err)
	}

	return nil
}

func parseVictoriaLogsLine(line string) (datasource.LogEntry, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return datasource.LogEntry{}, false
	}

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return datasource.LogEntry{}, false
	}

	entry := datasource.LogEntry{
		Labels: make(map[string]string),
	}

	if msg, ok := raw["_msg"].(string); ok {
		entry.Line = msg
	}
	if t, ok := raw["_time"].(string); ok {
		entry.Timestamp = t
	}

	for k, v := range raw {
		if k == "_msg" || k == "_time" {
			continue
		}
		if str, ok := v.(string); ok {
			entry.Labels[k] = str
		}
	}

	entry.Level = detectLogLevel(entry.Labels, entry.Line)

	return entry, true
}

var (
	_ datasource.Client            = (*Client)(nil)
	_ datasource.StreamClient      = (*Client)(nil)
	_ datasource.LabelsClient      = (*Client)(nil)
	_ datasource.LabelValuesClient = (*Client)(nil)
)
