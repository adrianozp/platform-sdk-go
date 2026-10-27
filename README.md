# kalzone-sdk-go

Go client for the queues of [Kalzone](https://platform.kalz.one), the internal
developer platform. Kalzone hands every deployed
service `PLATFORM_QUEUE_URL`, `PLATFORM_QUEUE_TOKEN` and `PLATFORM_QUEUE_SIGNING_KEY`;
this module uses them. Standard library only.

```sh
go get github.com/klzn-labs/kalzone-sdk-go/queue
```

## Publish

```go
q, err := queue.FromEnv()
id, err := q.PublishJSON(ctx, "place-import", job,
	queue.WithIdempotencyKey(job.CityID),
	queue.WithDelay(time.Minute))
```

Network errors and 5xx answers are retried three times with the same idempotency
key, so a retried publish is stored once. A refusal comes back as
`queue.ErrUnauthorized`, `ErrForbidden`, `ErrNotFound` or `ErrTooLarge`.

## Receive

The gateway POSTs each message to the path the queue names on your service.

```go
h := queue.Handler(os.Getenv("PLATFORM_QUEUE_SIGNING_KEY"), func(ctx context.Context, m queue.Message) error {
	var job ImportJob
	if err := m.DecodeJSON(&job); err != nil {
		return queue.Permanent(err)
	}
	return process(ctx, job)
})
mux.Handle("POST /hooks/place-import", h)
```

| Handler returns | Response | The gateway |
|---|---|---|
| `nil` | 200 | acks the message |
| `queue.Permanent(err)` | 422 | moves it to the dead letters |
| `queue.RetryAfter(d, err)` | 503 + `Retry-After` | retries after `d` (capped by the queue's backoff max) |
| any other error | 500 | retries with the queue's backoff |

Delivery is at least once: the same message can arrive twice, so handlers must be
idempotent. `m.ID` is stable across retries and `m.Attempt` counts from 1.

## Cron targets

A Kalzone cron POSTs to a path on your service on its schedule, signed the same
way as a queue delivery, so `queue.Handler` serves it too:

```go
mux.Handle("POST /hooks/refresh", queue.Handler(os.Getenv("PLATFORM_QUEUE_SIGNING_KEY"), func(ctx context.Context, m queue.Message) error {
	var job RefreshJob
	if err := m.DecodeJSON(&job); err != nil {
		return queue.Permanent(err)
	}
	return refresh(ctx, job, m.Header.Get("Platform-Run-Id"))
}))
```

The body is the cron's JSON payload (`{}` when it has none). A cron call has no
message ID or queue name; read `Platform-Cron`, `Platform-Run-Id` (stable across
retries of one run), `Platform-Scheduled-At` and `Platform-Attempt` from
`m.Header`. Answering `nil` ends the run as succeeded, `queue.Permanent` fails it
at once, and any other error or `queue.RetryAfter` is retried within the cron's
timeout.

## Verifying deliveries without this module

Each delivery carries `Platform-Signature: t=<unix seconds>,v1=<hex>`, where the hex
is HMAC-SHA256 of `"<t>.<body>"` keyed with `PLATFORM_QUEUE_SIGNING_KEY`. Refuse a
mismatch, and a `t` more than five minutes from now.

## Testing

`queuetest.NewGateway()` is a fake gateway that records what was published;
`queuetest.Delivery(key, body)` builds a signed request for your handler.
