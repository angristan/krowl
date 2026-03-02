# frontier

Min-heap priority queue of domains ordered by next-allowed-fetch time. Replaces O(n) domain scan with O(log n) heap operations.

```
  Push(domain, nextFetch)
         │
         ▼
  ┌──────────────────────────────────┐
  │  Min-Heap (by NextFetch time)    │
  │                                  │
  │  ┌─────┐  ┌─────┐  ┌─────┐     │
  │  │a.com│  │c.com│  │b.com│     │  O(log n) push/pop
  │  │ t=1 │  │ t=3 │  │ t=5 │     │
  │  └──┬──┘  └─────┘  └─────┘     │
  │     │                            │
  │     │  index map: domain → pos   │  O(1) lookup
  │     │  (for update-in-place)     │
  └─────┼────────────────────────────┘
        │
        ▼ PopReady()
  domain (if t <= now)
  or wait duration
```

Domains are pushed onto the heap when URLs are enqueued. The fetcher pool pops the earliest-ready domain, fetches a URL, and re-pushes the domain with an updated next-fetch time based on adaptive crawl delay.

## API

- `New()` — Create empty frontier
- `Push(domain, nextFetch)` — Add or update domain (only updates if new time is earlier)
- `PopReady()` — Pop earliest domain if its time has passed; returns wait duration otherwise
- `Peek()` — Inspect top of heap without popping
- `Remove(domain)` — Remove a domain from the heap
- `Len()` / `Contains(domain)`

## Key types

- `Entry` — Domain name + NextFetch timestamp
- `Frontier` — Thread-safe heap with O(1) domain lookup via index map

## Dependencies

None (leaf package).
