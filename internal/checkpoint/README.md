# checkpoint

Atomically persists and restores the crawler frontier (per-domain URL queues) to survive restarts.

Frontier state is gob-encoded as `map[string][]domain.QueueItem` and written via temp-file + atomic rename to prevent corruption.

```
  domain.Manager                           disk
  ┌──────────────┐    Snapshot()    ┌────────────────┐
  │ domain queues │───────────────►│  frontier.ckpt  │
  │ a.com: [urls] │   gob encode   │  (temp + rename)│
  │ b.com: [urls] │   + atomic     │                 │
  │ ...           │   write        │                 │
  └──────────────┘                 └────────────────┘
                                          │
  ┌──────────────┐    Load()              │
  │ domain queues │◄──────────────────────┘
  │ (restored)    │   gob decode
  └──────────────┘
```

## API

- `Save(path, queues)` — Atomic write of frontier snapshot to disk
- `Load(path)` — Read checkpoint; returns empty map if file doesn't exist

## Usage

The crawler calls `Save` periodically (default every 30s) and on `SIGTERM`. On startup, `Load` restores the frontier so crawling resumes where it left off.

## Dependencies

- `internal/domain` (for `QueueItem` type)
