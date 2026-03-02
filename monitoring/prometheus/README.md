# prometheus

Prometheus alerting rules for the krowl crawler cluster.

## Alert flow

```
  Crawler metrics         Prometheus              Alert evaluation
  ┌────────────┐         ┌───────────┐           ┌───────────────┐
  │krowl_*     │──scrape─►│           │──evaluate─►│ alerts.yml   │
  │node_*      │  15s     │  TSDB     │  15s      │ (17 rules)   │
  │redis_*     │         │  7d ret.  │           │              │
  │juicefs_*   │         │           │           │ CrawlStalled │
  └────────────┘         └───────────┘           │ HighErrors   │
   3 workers                master                │ BloomSat.    │
                                                  │ WARCErrors   │
                                                  │ NodeDown     │
                                                  │ ...          │
                                                  └──────┬────────┘
                                                         │
                                                         ▼
                                                    (alertmanager)
```

## alerts.yml

17 alerting rules across the following categories:

### Crawl health
- **CrawlStalled** — No pages fetched in 5 minutes
- **HighFetchErrorRate** — Error rate exceeds 25%
- **FrontierStarvation** — Frontier empty for 3 minutes

### Dedup
- **BloomSaturation** — Bloom filter false positive rate > 15% (warning)
- **BloomSaturationCritical** — False positive rate > 30% (critical)

### HTTP latency
- **HighFetchLatencyP95** — p95 fetch latency > 10 seconds
- **HighTLSLatencyP95** — p95 TLS handshake > 2 seconds

### WARC
- **WARCWriteErrors** — Any write errors indicate potential data loss

### Inbox
- **InboxForwardErrors** — Cross-shard forwarding failures
- **InboxQueueBacklog** — Inbox queue > 100K items

### Infrastructure
- **NodeDown** — Crawler node unreachable
- **HighMemory** — RSS > 3.5 GB
- **HighGoroutineCount** — More than 10K goroutines
- **HighOpenFDs** — File descriptor usage > 80%

### CoreDNS
- **CoreDNSDown** — CoreDNS process not running
- **CoreDNSHighLatency** — DNS resolution latency too high
- **CoreDNSLowCacheHitRate** — DNS cache hit rate below threshold

## Deployment

The alerts file is base64-encoded into the master cloud-init script and written to `/etc/prometheus/alerts.yml` on provisioning. To update manually:

```
scp monitoring/prometheus/alerts.yml krowl-master:/etc/prometheus/alerts.yml
ssh krowl-master "kill -HUP \$(pgrep prometheus)"
```
