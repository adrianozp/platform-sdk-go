package queuetest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adrianozp/platform-sdk-go/queue"
)

type Published struct {
	Queue          string
	Body           []byte
	ContentType    string
	IdempotencyKey string
	Delay          string
}

type Gateway struct {
	*httptest.Server
	mu        sync.Mutex
	published []Published
}

func NewGateway() *Gateway {
	g := &Gateway{}
	g.Server = httptest.NewServer(http.HandlerFunc(g.handle))
	return g
}

func (g *Gateway) handle(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/queues/"), "/messages")
	body, _ := io.ReadAll(r.Body)
	g.mu.Lock()
	g.published = append(g.published, Published{
		Queue: name, Body: body, ContentType: r.Header.Get("Content-Type"),
		IdempotencyKey: r.Header.Get("Idempotency-Key"), Delay: r.Header.Get("Platform-Delay"),
	})
	id := "m" + strconv.Itoa(len(g.published))
	g.mu.Unlock()
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"message_id": id})
}

func (g *Gateway) Client() *queue.Client {
	return queue.New(g.URL, "test-token", g.Server.Client())
}

func (g *Gateway) Published() []Published {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Published{}, g.published...)
}

type DeliveryOption func(*http.Request)

func WithAttempt(n int) DeliveryOption {
	return func(r *http.Request) { r.Header.Set("Platform-Attempt", strconv.Itoa(n)) }
}

func Delivery(signingKey string, body []byte, opts ...DeliveryOption) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/hooks", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Platform-Message-Id", "m1")
	r.Header.Set("Platform-Attempt", "1")
	r.Header.Set("Platform-Queue", "test")
	r.Header.Set("Platform-Published-At", time.Now().UTC().Format(time.RFC3339))
	r.Header.Set("Platform-Signature", queue.Sign(signingKey, time.Now(), body))
	for _, opt := range opts {
		opt(r)
	}
	return r
}
