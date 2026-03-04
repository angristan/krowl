# dedup

Bloom filter backed by Pebble for persistence. The bloom filter is authoritative during crawling (zero Pebble reads). Pebble is write-only — it persists URL hashes so the bloom can be rebuilt on restart via `WarmBloom()`.

```
  URL ──► IsNew(url)
              │
              ▼
    ┌──────────────────┐
    │  Bloom filter     │  (RAM, ~120MB)
    │  Test(key)        │
    └────────┬─────────┘
             │
        ┌────┴────┐
        no        yes (seen)
        │         │
        ▼         ▼
    NEW: Add to  DUPLICATE
    bloom + write (skip)
    to Pebble
    (NoSync)
```

Pebble writes use `NoSync` — the WAL still records them but doesn't fsync per write. On crash, at most the last WAL segment (~64MB) is lost; those URLs simply get re-crawled once.

On startup, `WarmBloom()` scans all Pebble keys back into the bloom filter.

## API

- `New(pebblePath, expectedURLs)` — Open Pebble DB and create bloom filter
- `WarmBloom()` — Rebuild bloom from Pebble (call before crawling)
- `IsNew(rawURL)` — Returns true if URL hasn't been seen; marks it in bloom + Pebble
- `MarkSeen(rawURL)` — Force-mark a URL (used by inbox consumer for cross-shard URLs)
- `Metrics()` — Expose underlying Pebble metrics for Prometheus
- `Close()` — Close Pebble database

## Dependencies

- `internal/bloom`
- `internal/metrics`
- `cockroachdb/pebble`
