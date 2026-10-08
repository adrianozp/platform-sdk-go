package queue_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrianozp/platform-sdk-go/queue"
	"github.com/adrianozp/platform-sdk-go/queue/queuetest"
)

func Test_PublishJSON_SendsBodyHeadersAndOptions(t *testing.T) {
	g := queuetest.NewGateway()
	defer g.Close()

	id, err := g.Client().PublishJSON(context.Background(), "place-import", map[string]int{"city": 7},
		queue.WithIdempotencyKey("city-7"), queue.WithDelay(time.Minute))

	got := g.Published()
	if err != nil || id != "m1" || len(got) != 1 {
		t.Fatalf("id=%q err=%v published=%d", id, err, len(got))
	}
	p := got[0]
	if p.Queue != "place-import" || string(p.Body) != `{"city":7}` || p.ContentType != "application/json" || p.IdempotencyKey != "city-7" || p.Delay != "60" {
		t.Fatalf("unexpected publish %+v", p)
	}
}

func flakyGateway(t *testing.T, failures int32) (*httptest.Server, *atomic.Int32, chan string) {
	var calls atomic.Int32
	keys := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys <- r.Header.Get("Idempotency-Key")
		if calls.Add(1) <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"message_id":"m9"}`))
	}))
	t.Cleanup(server.Close)
	return server, &calls, keys
}

func Test_Publish_RetriesServerErrorsWithTheSameKey(t *testing.T) {
	server, calls, keys := flakyGateway(t, 2)

	id, err := queue.New(server.URL, "tok", server.Client()).Publish(context.Background(), "q", []byte("x"))

	if err != nil || id != "m9" || calls.Load() != 3 || len(keys) != 3 {
		t.Fatalf("id=%q err=%v calls=%d", id, err, calls.Load())
	}
	first, second, third := <-keys, <-keys, <-keys
	if first == "" || first != second || second != third {
		t.Fatalf("keys differ: %q %q %q", first, second, third)
	}
}

func Test_Publish_MapsRefusalsWithoutRetrying(t *testing.T) {
	cases := map[int]error{401: queue.ErrUnauthorized, 403: queue.ErrForbidden, 404: queue.ErrNotFound, 413: queue.ErrTooLarge}
	for status, want := range cases {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
		}))
		_, err := queue.New(server.URL, "tok", server.Client()).Publish(context.Background(), "q", []byte("x"))
		server.Close()
		if !errors.Is(err, want) || calls.Load() != 1 {
			t.Fatalf("status %d: err=%v calls=%d", status, err, calls.Load())
		}
	}
}

func Test_FromEnv_NeedsURLAndToken(t *testing.T) {
	t.Setenv("PLATFORM_QUEUE_URL", "")
	if _, err := queue.FromEnv(); !errors.Is(err, queue.ErrNotConfigured) {
		t.Fatalf("err=%v", err)
	}
}
