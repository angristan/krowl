# dedup

Two-tier URL deduplication: in-memory bloom filter (fast reject) followed by Pebble on SSD (exact, persistent).

```
  URL ──► IsNew(url)
              │
              ▼
    ┌──────────────────┐
    │  Tier 0: Bloom   │  (RAM, ~120MB)
    │  TestAndAdd(url)  │
    └────────┬─────────┘
             │
        probably seen?
        ┌────┴────┐
        no        yes
        │         │
        ▼         ▼
    DEFINITELY  ┌──────────────────┐
    NEW ────►   │  Tier 1: Pebble  │  (SSD, exact)
    mark in     │  Has(url)?       │
    both tiers  └────────┬─────────┘
                    ┌────┴────┐
                    no        yes
                    │         │
                    ▼         ▼
                   NEW     DUPLICATE
                (mark in    (skip)
                 Pebble)
```

A URL goes through:
1. **Bloom filter** — if negative, URL is definitely new (fast path)
2. **Pebble lookup** — if bloom says "maybe seen", Pebble provides the definitive answer

This avoids hitting disk for the majority of URLs while keeping zero false negatives.

## API

- `New(pebblePath, expectedURLs)` — Open Pebble DB and create bloom filter
- `IsNew(rawURL)` — Returns true if URL hasn't been seen; atomically marks it in both tiers
- `MarkSeen(rawURL)` — Force-mark a URL (used by inbox consumer for cross-shard URLs)
- `Metrics()` — Expose underlying Pebble metrics for Prometheus
- `Close()` — Close Pebble database

## Dependencies

- `internal/bloom`
- `internal/metrics`
- `cockroachdb/pebble`
