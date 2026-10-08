package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

const maxBody = 1 << 20

type Message struct {
	ID          string
	Attempt     int
	Queue       string
	PublishedAt time.Time
	Header      http.Header
	Body        []byte
}

func (m Message) DecodeJSON(v any) error {
	return json.Unmarshal(m.Body, v)
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

type retryAfterError struct {
	after time.Duration
	err   error
}

func (e retryAfterError) Error() string { return e.err.Error() }
func (e retryAfterError) Unwrap() error { return e.err }

func Permanent(err error) error { return permanentError{err: err} }

func RetryAfter(after time.Duration, err error) error { return retryAfterError{after: after, err: err} }

func Handler(signingKey string, handle func(ctx context.Context, m Message) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
		if err != nil {
			http.Error(w, "unreadable body", http.StatusBadRequest)
			return
		}
		if Verify(r.Header.Get("Platform-Signature"), signingKey, body, time.Now()) != nil {
			http.Error(w, "bad signature", http.StatusUnauthorized)
			return
		}
		respond(w, handle(r.Context(), message(r, body)))
	})
}

func message(r *http.Request, body []byte) Message {
	attempt, _ := strconv.Atoi(r.Header.Get("Platform-Attempt"))
	published, _ := time.Parse(time.RFC3339, r.Header.Get("Platform-Published-At"))
	return Message{
		ID: r.Header.Get("Platform-Message-Id"), Attempt: attempt, Queue: r.Header.Get("Platform-Queue"),
		PublishedAt: published, Header: r.Header, Body: body,
	}
}

func respond(w http.ResponseWriter, err error) {
	var permanent permanentError
	var later retryAfterError
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.As(err, &permanent):
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case errors.As(err, &later):
		w.Header().Set("Retry-After", strconv.Itoa(int(later.after.Seconds())))
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
