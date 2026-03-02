# fetch

Fetcher goroutine pool. Pulls domains from the frontier heap, respects per-domain rate limits and robots.txt, performs HTTP fetches via a gowarc WARC-writing HTTP client, and sends results to parsers.

WARC recording is transparent: gowarc wraps every TCP connection with `TeeReader`/`MultiWriter` at the transport layer, so every request/response pair is captured without any explicit WARC logic in this package.

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
  │                    │  gowarc client   │    │
  │                    │  (WARC recording │    │
  │                    │   at TCP layer)  │    │
  │                    │                  │    │
  │                    │  httptrace:      │    │
  │                    │  Connect ► TTFB  │    │
  │                    │  ► ReadBody      │    │
  │                    └────────┬─────────┘    │
  │                             │              │
  │                      fetch.Result          │
  │                    (url, body, status)      │
  └─────────────────────┬─────────────────────┘
                        │
                        ▼
                  fetchResults channel
                   (to parsers)
```

## Key constants

| Constant | Value | Description |
|----------|-------|-------------|
| `FetchTimeout` | 10s | Per-request timeout |
| `MaxBodySize` | 1 MB | Body truncation limit |
| `MaxRedirects` | 5 | Maximum redirect chain length |
| `MaxRetries` | 1 | Retry transient errors once |

## Features

- **gowarc WARC recording** — WARC capture at the transport layer (TeeReader/MultiWriter on TCP connections). No manual WARC logic needed.
- **httptrace instrumentation** — Per-phase timing (connect, TTFB) exported as Prometheus histograms. Note: DNS and TLS callbacks don't fire because gowarc does DNS/TLS in its custom dialer.
- **TLS metrics via context** — `TLSInfo` struct stashed in request context; the patched `DialTLSContext` extracts TLS version/cipher from utls via reflection.
- **Content-type filtering** — Only processes HTML responses
- **Retry logic** — Retries transient errors (5xx, timeouts, connection resets)

## API

- `NewPool(dm, fr, results, client, userAgent, workers)` — Create pool with a gowarc `*warc.CustomHTTPClient`
- `Run(ctx)` — Start all workers; blocks until context cancelled

## Key types

- `Result` — Fetch output: URL, domain, depth, body, status
- `Pool` — Manages N fetcher goroutines
- `TLSInfo` — TLS version + cipher extracted by the patched gowarc dialer (via context)

## Dependencies

- `internal/domain`
- `internal/frontier`
- `internal/metrics`
- `github.com/internetarchive/gowarc`
