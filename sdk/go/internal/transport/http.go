package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// HTTPTransport is a simple HTTP client for SwarmKeyDb REST fallback.
type HTTPTransport struct {
	BaseURL  string
	Password string
	Client   *http.Client
}

// NewHTTPTransport creates a new HTTP transport.
func NewHTTPTransport(baseURL, password string, timeout time.Duration) *HTTPTransport {
	return &HTTPTransport{
		BaseURL:  strings.TrimRight(baseURL, "/"),
		Password: password,
		Client:   &http.Client{Timeout: timeout},
	}
}

type httpCmdRequest struct {
	Cmd  string   `json:"cmd"`
	Args []string `json:"args"`
}

type httpCmdResponse struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error,omitempty"`
}

// Do sends a command via HTTP POST /cmd.
func (t *HTTPTransport) Do(ctx context.Context, cmd string, args []string) (*Response, error) {
	payload := httpCmdRequest{Cmd: cmd, Args: args}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		t.BaseURL+"/cmd", strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.Password != "" {
		req.Header.Set("Authorization", "Bearer "+t.Password)
	}
	resp, err := t.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var r httpCmdResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("http: bad response: %w", err)
	}
	return &Response{Result: r.Result, Error: r.Error}, nil
}
