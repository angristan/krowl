# domain

Per-domain crawl state management: robots.txt caching, adaptive rate limiting, error tracking, per-domain URL queues, and sitemap discovery.

```
                    Enqueue(domain, url, depth)
                           │
                           ▼
                  ┌─── maxFrontier? ───┐
                  │ (global atomic cap) │
                  └────────┬───────────┘
                      under cap
                           │
  ┌────────────────────────┼──────────────────────────┐
  │                  Manager                          │
  │                                                    │
  │  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
  │  │ a.com   │  │ b.com   │  │ c.com   │  ...      │
  │  │┌───────┐│  │┌───────┐│  │┌───────┐│           │
  │  ││ State ││  ││ State ││  ││ State ││           │
  │  ││robots ││  ││robots ││  ││robots ││           │
  │  ││delay  ││  ││delay  ││  ││delay  ││           │
  │  ││backoff││  ││backoff││  ││backoff││           │
  │  ││Queue[]││  ││Queue[]││  ││Queue[]││           │
  │  │└───────┘│  │└───────┘│  │└───────┘│           │
  │  └────┬────┘  └────┬────┘  └────┬────┘           │
  │       └────────────┼────────────┘                 │
  │                    ▼                               │
  │           frontier.Push(domain, nextFetch)         │
  └────────────────────────────────────────────────────┘
                       │
          CanFetch? ◄──┘   RecordFetch(latency)
          │                      │
          ▼                      ▼
     wait / fetch         delay = latency * 5
                          (clamped 250ms..30s)
```

## Key types

- `State` — Per-domain state: robots.txt rules, crawl delay, backoff timers, health counters, URL queue
- `QueueItem` — URL + crawl depth (hops from seed)
- `Manager` — Thread-safe manager for all domain states

## Key constants

| Constant | Value | Description |
|----------|-------|-------------|
| `DefaultCrawlDelay` | 1s | Initial delay between requests to a domain |
| `MinCrawlDelay` | 250ms | Floor for adaptive delay |
| `MaxCrawlDelay` | 30s | Ceiling for adaptive delay |
| `AdaptiveMultiplier` | 5 | Delay = latency * 5 |
| `MaxQueuePerDomain` | 1K | Per-domain URL queue cap (forces domain diversity) |
| `DefaultMaxFrontier` | 50M | Global URL queue cap (disk-backed, no memory concern) |
| `MaxURLLength` | 2048 | Reject URLs longer than this |
| `MaxCrawlDepth` | 25 | Maximum hops from seed |
| `MaxConsecutiveDead` | 10 | Give up on a domain after this many consecutive errors |

## Rate limiting

Adaptive crawl delay based on response latency EMA: `delay = max(MinCrawlDelay, latency * 5)`. A 200ms response yields a 1s delay. Exponential backoff kicks in after 10 consecutive errors.

## API

- `NewManager(userAgent, maxFrontier)` — Create manager with global frontier cap
- `Enqueue(domain, url, depth)` / `Dequeue(domain)` — Per-domain FIFO with backpressure
- `EnqueueBatch(items, depth)` — Batch enqueue in a single bbolt txn (used for seed loading)
- `EnqueueBatchWithDepth(items)` — Batch enqueue with per-item depth (used by inbox consumer)
- `IsAllowed(domain, path)` — robots.txt check (fetches and caches on miss, triggers sitemap discovery)
- `CanFetch(domain)` / `RecordFetch(domain, latency)` — Rate limiting
- `RecordRateLimit(domain, retryAfter)` — Handle 429 responses (doubles crawl delay, sets backoff)
- `RecordError(domain)` — Error tracking with exponential backoff
- `SaveAllState()` / `IterMeta()` — Persist/restore domain state via bbolt metadata

## Dependencies

- `internal/frontier`
- `internal/urlqueue`
- `internal/sitemap`
- `temoto/robotstxt`
