# parse

Parser goroutine pool. Extracts links from fetched HTML pages, runs dedup, and either enqueues URLs locally or forwards them to the owning node via the inbox sender.

```
  fetch.Result (from channel)
        │
        ▼
  ┌─────────────────────────────────────────┐
  │             parse.Pool                  │
  │                                          │
  │  HTML body ──► x/net/html tokenizer     │
  │                    │                     │
  │                    ▼                     │
  │              extract <a href>            │
  │              resolve against base URL    │
  │                    │                     │
  │                    ▼                     │
  │              urlnorm.Normalize()         │
  │                    │                     │
  │                    ▼                     │
  │              dedup.IsNew(url)?           │
  │              ┌─────┴──────┐              │
  │             yes           no             │
  │              │          (skip)           │
  │              ▼                           │
  │        ring.Owner(domain)               │
  │        ┌─────┴──────┐                   │
  │       mine        other                 │
  │        │            │                   │
  │        ▼            ▼                   │
  │   dm.Enqueue   inbox.Forward            │
  │   (local)      (Redis LPUSH)            │
  │                                          │
  │  body hash ──► soft-404 detection       │
  │  (skip after 3 identical hashes/domain) │
  └─────────────────────────────────────────┘
```

## Features

- **Link extraction** — Parses HTML with `golang.org/x/net/html`, resolves relative URLs against base
- **URL normalization** — Via `urlnorm` package before dedup
- **Soft-404 detection** — Per-domain body hash counting; skips pages after 3 identical content hashes
- **Depth tracking** — Child URLs get `depth = parent.Depth + 1`
- **Cross-shard routing** — URLs owned by other nodes (per consistent hash ring) are forwarded via inbox

## API

- `NewPool(results, dedup, dm, sender, ring, myID, workers)` — Create parser pool
- `Run(ctx)` — Start workers; blocks until results channel closes

## Dependencies

- `internal/dedup`
- `internal/domain`
- `internal/fetch` (for `Result` type)
- `internal/inbox`
- `internal/metrics`
- `internal/ring`
- `internal/urlnorm`
- `golang.org/x/net/html`
