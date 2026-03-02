# inbox

Cross-shard URL forwarding via Redis. When a parser discovers a URL that belongs to a different node (per consistent hashing), the inbox sender pushes it to that node's Redis inbox. The inbox consumer on the receiving node polls its local Redis, dedup-checks the URLs, and enqueues them into the domain manager.

```
  Node 0 (parser)              Node 1 (inbox)              Node 2 (inbox)
  ┌──────────────┐            ┌──────────────┐            ┌──────────────┐
  │ discovered   │            │              │            │              │
  │ url for      │            │              │            │              │
  │ node 1       │            │              │            │              │
  │    │         │            │              │            │              │
  │    ▼         │            │              │            │              │
  │  Sender      │            │   Consumer   │            │   Consumer   │
  │  ring.Owner()│            │   LPOP batch │            │   LPOP batch │
  │  ──► node 1  │            │   (500/50ms) │            │   (500/50ms) │
  │    │         │            │      │       │            │      │       │
  └────┼─────────┘            └──────┼───────┘            └──────┼───────┘
       │                             │                           │
       │  LPUSH "1\turl"             │                           │
       └──────────────────►    Redis inbox                       │
                               (local)                           │
                                     │                           │
                                     ▼                           │
                               dedup ► enqueue                   │
                               into domain.Manager               │
```

## Wire format

```
depth\turl
```

## Components

### Sender
- Maintains Redis connections to all peer nodes
- `Forward(ctx, url, depth)` — Sends URL to the owning node's inbox via `LPUSH`
- `UpdatePeers(nodes)` — Refreshes Redis connections on topology change (from Consul watcher)

### Consumer
- Polls local Redis inbox in batches (500 items, 50ms interval)
- Dedup-checks URLs before enqueuing
- `Run(ctx)` — Blocking loop until context cancelled

## API

- `NewSender(ring, myID, dedup)` / `Forward(ctx, url, depth)`
- `NewConsumer(localRedis, dedup, domainManager)` / `Run(ctx)`

## Dependencies

- `internal/dedup`
- `internal/domain`
- `internal/metrics`
- `internal/ring`
- `redis/go-redis`
