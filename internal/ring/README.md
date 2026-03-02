# ring

Consistent hashing for domain-to-node assignment. The ring is rebuilt dynamically from the Consul service catalog when topology changes.

```
  SetNodes([node0, node1, node2])
         │
         ▼
  ┌──────────────────────────────────────────────┐
  │              Consistent Hash Ring             │
  │                                                │
  │     0 ─────── node0_v0 ─── node2_v1 ──────   │
  │    /                                      \   │
  │   │   128 virtual nodes per physical node  │  │
  │   │                                        │  │
  │    \                                      /   │
  │     ── node1_v0 ─── node0_v1 ─── ... ────    │
  │                                                │
  │  Owner("example.com")                         │
  │    = FNV-64a("example.com")                   │
  │    = find next vnode clockwise                │
  │    = node1                                    │
  └────────────────────────────────────────────────┘
```

Uses 128 virtual nodes per physical node (FNV-64a hashing) for even distribution.

## API

- `New(vnodes)` — Create ring (default 128 vnodes per node)
- `SetNodes(nodes)` — Replace entire ring (called on topology change from Consul watcher)
- `Owner(domain)` — Return the node ID that owns a domain
- `Nodes()` / `NodeCount()` / `GetNode(id)` — Query ring state
- `HashDomain(domain)` — Exported FNV-64a hash

## Key types

- `Node` — ID + VPC address + Redis address
- `Ring` — Thread-safe consistent hash ring

## Dependencies

None (leaf package).
