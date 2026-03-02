# bloom

Thread-safe in-memory bloom filter for fast probabilistic URL deduplication (Tier 0 in the dedup pipeline).

```
  URL string
      │
      ▼
  FNV-128a hash
      │
      ├── h1 ──┐
      └── h2 ──┤
               ▼
  ┌──────────────────────────────────┐
  │  bit array ([]uint64)           │
  │  ░░█░░░█░░░░█░░░░░░█░░░█░░░░░  │
  │                                  │
  │  k positions = h1 + i*h2        │
  │  (Kirsch-Mitzenmacker scheme)    │
  └──────────────────────────────────┘
      │
      ▼
  all bits set? ──yes──► "probably seen"
      │
      no
      ▼
  "definitely new"
```

Uses Kirsch-Mitzenmacker double hashing over FNV-128a. Sized at initialization for a target false positive rate — e.g. 100M items at 1% FP uses ~120 MB of RAM.

## API

- `New(expectedItems, fpRate)` — Create a bloom filter sized for expected cardinality
- `Add(key)` — Insert a key
- `Test(key)` — Probabilistic membership test (no false negatives)
- `TestAndAdd(key)` — Atomic check-and-insert; returns true if probably already present
- `SizeBytes()` — Memory footprint of the bit array

## Dependencies

None (leaf package).
