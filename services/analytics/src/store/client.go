package store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a stdlib-only ClickHouse HTTP interface client.
type Client struct {
	baseURL  string
	user     string
	password string
	database string
	http     *http.Client
}

// Config holds ClickHouse HTTP connection settings.
type Config struct {
	URL      string // e.g. http://clickhouse:8123
	User     string
	Password string
	Database string
	Timeout  time.Duration
}

// NewClient constructs a ClickHouse HTTP client. Database must be non-empty.
func NewClient(cfg Config) (*Client, error) {
	raw := strings.TrimSpace(cfg.URL)
	if raw == "" {
		return nil, fmt.Errorf("clickhouse: URL required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("clickhouse: URL scheme must be http or https")
	}
	if u.Host == "" {
		return nil, fmt.Errorf("clickhouse: URL host required")
	}
	db := strings.TrimSpace(cfg.Database)
	if db == "" {
		db = "analytics"
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	base := strings.TrimRight(u.Scheme+"://"+u.Host, "/")
	return &Client{
		baseURL:  base,
		user:     cfg.User,
		password: cfg.Password,
		database: db,
		http:     &http.Client{Timeout: timeout},
	}, nil
}

// Database returns the configured database name.
func (c *Client) Database() string { return c.database }

// Ping runs SELECT 1.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Query(ctx, "SELECT 1")
	return err
}

// Exec runs a statement that returns no rows.
func (c *Client) Exec(ctx context.Context, query string, params map[string]string) error {
	_, err := c.do(ctx, query, params, false)
	return err
}

// Query runs a SELECT and returns TabSeparated rows (split on \t).
func (c *Client) Query(ctx context.Context, query string, params ...map[string]string) ([][]string, error) {
	var p map[string]string
	if len(params) > 0 {
		p = params[0]
	}
	body, err := c.do(ctx, query, p, true)
	if err != nil {
		return nil, err
	}
	body = strings.TrimSuffix(body, "\n")
	if body == "" {
		return nil, nil
	}
	lines := strings.Split(body, "\n")
	out := make([][]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, strings.Split(line, "\t"))
	}
	return out, nil
}

// QueryCell returns the first cell of the first row.
func (c *Client) QueryCell(ctx context.Context, query string, params map[string]string) (string, error) {
	rows, err := c.Query(ctx, query, params)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 || len(rows[0]) == 0 {
		return "", fmt.Errorf("clickhouse: empty result")
	}
	return rows[0][0], nil
}

func (c *Client) do(ctx context.Context, query string, params map[string]string, expectBody bool) (string, error) {
	q := url.Values{}
	q.Set("database", c.database)
	if expectBody {
		q.Set("default_format", "TabSeparated")
	}
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	endpoint := c.baseURL + "/?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(query))
	if err != nil {
		return "", err
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("clickhouse: request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return "", fmt.Errorf("clickhouse: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("clickhouse: status %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	return string(raw), nil
}

// QuoteIdent validates and backticks a ClickHouse identifier (letters/digits/underscore).
func QuoteIdent(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty identifier")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return "", fmt.Errorf("unsafe identifier %q", name)
	}
	return "`" + name + "`", nil
}
