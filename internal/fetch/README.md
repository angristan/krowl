# fetch

Fetcher goroutine pool. Pulls domains from the frontier heap, respects per-domain rate limits and robots.txt, performs HTTP fetches with connection pooling and DNS caching, and sends results downstream.

```
  frontier.PopReady()
        │
        ▼ domain
  ┌───────────────────────────────────────────┐
  │              fetch.Pool                   │
  │                                            │
  │  worker 0   worker 1   ...   worker 99    │
  │     │          │                 │         │
  │     ▼          ▼                 ▼         │
  │  CanFetch? ─► IsAllowed? ─► HTTP GET      │
  │  (rate)       (robots)       │             │
  │                              ▼             │
  │                    ┌──────────────────┐    │
  │                    │  httptrace       │    │
  │                    │  DNS ► Connect ► │    │
  │                    │  TLS ► TTFB ►    │    │
  │                    │  ReadBody        │    │
  │                    └────────┬─────────┘    │
  │                             │              │
  │                      fetch.Result          │
  │                    (url, body, status,      │
  │                     headers, timings)       │
  └─────────────────────┬─────────────────────┘
                        │
                        ▼
                  results channel
               (to parse + WARC)
```

## Key constants

| Constant | Value | Description |
|----------|-------|-------------|
| `FetchTimeout` | 10s | Per-request timeout |
| `MaxBodySize` | 1 MB | Body truncation limit |
| `MaxRedirects` | 5 | Maximum redirect chain length |
| `MaxRetries` | 1 | Retry transient errors once |

## Features

- **Connection pooling** — 1000 max idle connections, 100 per host
- **DNS caching** — In-process cache to avoid hammering resolvers
- **httptrace instrumentation** — Per-phase timing (DNS, connect, TLS, TTFB) exported as Prometheus histograms
- **Content-type filtering** — Only processes HTML responses
- **Retry logic** — Retries transient errors (5xx, timeouts, connection resets) with backoff

## API

- `NewPool(dm, fr, results, userAgent, workers)` — Create pool with tuned HTTP transport
- `Run(ctx)` — Start all workers; blocks until context cancelled

## Key types

- `Result` — Fetch output: URL, domain, depth, body, status, headers, request headers (for WARC writing)
- `Pool` — Manages N fetcher goroutines

## Dependencies

- `internal/domain`
- `internal/frontier`
- `internal/metrics`
