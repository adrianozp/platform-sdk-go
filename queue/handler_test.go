package queue_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adrianozp/platform-sdk-go/queue"
	"github.com/adrianozp/platform-sdk-go/queue/queuetest"
)

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func Test_Handler_PassesTheMessageAndAcks(t *testing.T) {
	var got queue.Message
	h := queue.Handler("key", func(_ context.Context, m queue.Message) error { got = m; return nil })

	rec := serve(h, queuetest.Delivery("key", []byte(`{"city":7}`), queuetest.WithAttempt(3)))

	var job struct{ City int }
	if rec.Code != http.StatusOK || got.ID != "m1" || got.Attempt != 3 || got.Queue != "test" || got.DecodeJSON(&job) != nil || job.City != 7 {
		t.Fatalf("code=%d message=%+v", rec.Code, got)
	}
}

func Test_Handler_MapsErrorsToTheGatewayOutcomes(t *testing.T) {
	cases := map[int]error{
		http.StatusUnprocessableEntity: queue.Permanent(errors.New("bad payload")),
		http.StatusServiceUnavailable:  queue.RetryAfter(30*time.Second, errors.New("rate limited")),
		http.StatusInternalServerError: errors.New("database down"),
	}
	for want, returned := range cases {
		h := queue.Handler("key", func(context.Context, queue.Message) error { return returned })
		rec := serve(h, queuetest.Delivery("key", []byte("x")))
		if rec.Code != want {
			t.Fatalf("want %d got %d", want, rec.Code)
		}
	}
	h := queue.Handler("key", func(context.Context, queue.Message) error { return queue.RetryAfter(30*time.Second, errors.New("x")) })
	if got := serve(h, queuetest.Delivery("key", []byte("x"))).Header().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After=%q", got)
	}
}

func Test_Handler_RefusesBadSignatures(t *testing.T) {
	called := false
	h := queue.Handler("key", func(context.Context, queue.Message) error { called = true; return nil })
	tampered := queuetest.Delivery("key", []byte("x"))
	tampered.Header.Set("Platform-Signature", queue.Sign("key", time.Now(), []byte("other")))
	expired := queuetest.Delivery("key", []byte("x"))
	expired.Header.Set("Platform-Signature", queue.Sign("key", time.Now().Add(-10*time.Minute), []byte("x")))

	for _, r := range []*http.Request{queuetest.Delivery("wrong", []byte("x")), tampered, expired} {
		if rec := serve(h, r); rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	}
	if called {
		t.Fatal("handler ran for a bad signature")
	}
}
