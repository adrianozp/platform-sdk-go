package queue

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotConfigured = errors.New("queue: PLATFORM_QUEUE_URL and PLATFORM_QUEUE_TOKEN are not set")
	ErrUnauthorized  = errors.New("queue: the publish token was refused")
	ErrForbidden     = errors.New("queue: this service is not a publisher of that queue")
	ErrNotFound      = errors.New("queue: no such queue in this environment")
	ErrTooLarge      = errors.New("queue: the message is larger than the queue allows")
	ErrInvalid       = errors.New("queue: the gateway refused the request")
)

var retryWaits = []time.Duration{100 * time.Millisecond, 400 * time.Millisecond, 1600 * time.Millisecond}

type Client struct {
	url   string
	token string
	http  *http.Client
}

type publishOptions struct {
	contentType string
	key         string
	delay       time.Duration
}

type Option func(*publishOptions)

func WithContentType(contentType string) Option {
	return func(o *publishOptions) { o.contentType = contentType }
}
func WithIdempotencyKey(key string) Option { return func(o *publishOptions) { o.key = key } }
func WithDelay(delay time.Duration) Option { return func(o *publishOptions) { o.delay = delay } }

func New(url, token string, client *http.Client) *Client {
	return &Client{url: strings.TrimRight(url, "/"), token: token, http: client}
}

func FromEnv() (*Client, error) {
	url, token := os.Getenv("PLATFORM_QUEUE_URL"), os.Getenv("PLATFORM_QUEUE_TOKEN")
	if url == "" || token == "" {
		return nil, ErrNotConfigured
	}
	return New(url, token, &http.Client{Timeout: 10 * time.Second}), nil
}

func (c *Client) PublishJSON(ctx context.Context, queue string, v any, opts ...Option) (string, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return c.Publish(ctx, queue, body, append([]Option{WithContentType("application/json")}, opts...)...)
}

func (c *Client) Publish(ctx context.Context, queue string, body []byte, opts ...Option) (string, error) {
	o := publishOptions{contentType: "application/octet-stream", key: newKey()}
	for _, opt := range opts {
		opt(&o)
	}
	var err error
	for attempt := 0; attempt <= len(retryWaits); attempt++ {
		id, retry, sendErr := c.send(ctx, queue, body, o)
		if !retry {
			return id, sendErr
		}
		err = sendErr
		if waitErr := wait(ctx, attempt); waitErr != nil {
			return "", waitErr
		}
	}
	return "", err
}

func wait(ctx context.Context, attempt int) error {
	if attempt >= len(retryWaits) {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(retryWaits[attempt]):
		return nil
	}
}

func (c *Client) send(ctx context.Context, queue string, body []byte, o publishOptions) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/v1/queues/"+queue+"/messages", bytes.NewReader(body))
	if err != nil {
		return "", false, err
	}
	setPublishHeaders(req.Header, c.token, o)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", ctx.Err() == nil, err
	}
	defer resp.Body.Close()
	return decode(resp)
}

func setPublishHeaders(h http.Header, token string, o publishOptions) {
	h.Set("Authorization", "Bearer "+token)
	h.Set("Content-Type", o.contentType)
	h.Set("Idempotency-Key", o.key)
	if o.delay > 0 {
		h.Set("Platform-Delay", strconv.Itoa(int(o.delay.Seconds())))
	}
}

func decode(resp *http.Response) (string, bool, error) {
	if resp.StatusCode == http.StatusAccepted {
		var body struct {
			MessageID string `json:"message_id"`
		}
		err := json.NewDecoder(resp.Body).Decode(&body)
		return body.MessageID, false, err
	}
	if resp.StatusCode >= 500 {
		return "", true, fmt.Errorf("queue: gateway answered %d", resp.StatusCode)
	}
	return "", false, statusError(resp.StatusCode)
}

func statusError(status int) error {
	known := map[int]error{401: ErrUnauthorized, 403: ErrForbidden, 404: ErrNotFound, 413: ErrTooLarge}
	if err, ok := known[status]; ok {
		return err
	}
	return fmt.Errorf("%w (%d)", ErrInvalid, status)
}

func newKey() string {
	raw := make([]byte, 16)
	rand.Read(raw)
	return hex.EncodeToString(raw)
}
