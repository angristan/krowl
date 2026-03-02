# grafana/dashboards

Grafana dashboard definitions provisioned to the master node.

## Data flow

```
  ┌──────────────┐     scrape      ┌──────────────┐    query     ┌─────────┐
  │   Crawlers   │◄────────────────│  Prometheus  │◄─────────────│ Grafana │
  │  :9090       │   /metrics      │  :9090       │              │  :3000  │
  ├──────────────┤                 │              │              │         │
  │node_exporter │◄────────────────│  Consul SD   │              │ krowl   │
  │  :9100       │                 │  discovers   │──────────────►│.json   │
  ├──────────────┤                 │  all targets │              │         │
  │redis_exporter│◄────────────────│              │              │         │
  │  :9121       │                 │  alerts.yml  │              │         │
  ├──────────────┤                 │  (14 rules)  │              │         │
  │  JuiceFS     │◄────────────────│              │              │         │
  │  :9567       │                 └──────────────┘              └─────────┘
  └──────────────┘                    master node                 master node
    worker nodes
```

## krowl.json

The main operational dashboard with the following rows:

| Row | Panels |
|-----|--------|
| **Overview** | Pages/sec, URLs/sec, Active Domains, Frontier Size, Fetch Errors/sec, Cluster Nodes, JuiceFS Disk Usage, WARC Written |
| **HTTP Fetch Latency** | Duration percentiles, request phase latency (DNS, connect, TLS, TTFB) p50/p95 |
| **HTTP Responses** | Status codes, response size distribution, errors by reason |
| **Content & Links** | Content types, links per page, redirects |
| **URL Pipeline** | Flow rates (discovered, deduped, enqueued, forwarded), robots/dedup/sitemaps |
| **Dedup Internals** | Operations breakdown, bloom filter false positive rate |
| **DNS Cache** | Hit ratio, cache size, evictions |
| **WARC Storage** | Throughput, records/rotations, file size |
| **Inbox Cross-Shard** | Forward/receive rate, queue depth, batch size |
| **Frontier & Crawl Status** | Frontier size, domain counts |
| **Go Runtime** | Goroutines, heap usage, GC pauses |
| **Process** | CPU, RSS, file descriptors |
| **Network I/O** | Receive/transmit rate (Mbps), TCP connections |
| **Network I/O (Stacked)** | Same metrics with stacked area charts |
| **Disk I/O** | Read/write rate, disk space % |
| **JuiceFS** | Object storage latency, FUSE read/write, disk usage |
| **Pebble** | Disk usage, L0 files/sublevels, read amplification, compaction debt |
| **Redis** | Memory, connected clients, ops/sec, pool connections |

## Template variables

- `$instance` — populated from `krowl_pages_fetched_total` (port `:9090`), used to filter crawler-specific panels. Infrastructure panels (node_exporter, redis_exporter, JuiceFS) do **not** use this filter due to port mismatch.

## Deployment

The dashboard is provisioned from `/var/lib/grafana/dashboards/` on the master node. Grafana auto-reloads every 30 seconds.

```
scp monitoring/grafana/dashboards/krowl.json krowl-master:/var/lib/grafana/dashboards/krowl.json
```
