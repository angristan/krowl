# metrics

Centralized Prometheus metrics registry. Single source of truth for all `krowl_*` metrics (~70+ metrics).

```
  ┌─────────────────────────────────────────────────────────┐
  │                   metrics.Register()                     │
  │                                                          │
  │  fetch.Pool ──► krowl_pages_fetched_total                │
  │                 krowl_fetch_duration_seconds              │
  │                 krowl_dns_duration_seconds                │
  │                 krowl_response_size_bytes                 │
  │                                                          │
  │  parse.Pool ──► krowl_urls_discovered_total               │
  │                 krowl_urls_deduped_total                  │
  │                 krowl_links_per_page                      │
  │                                                          │
  │  dedup ──────► krowl_dedup_bloom_hits_total               │
  │                krowl_dedup_pebble_hits_total              │
  │                                                          │
  │  warc ───────► krowl_warc_bytes_written_total             │
  │                krowl_warc_records_written_total           │
  │                                                          │
  │  inbox ──────► krowl_inbox_forwarded_total                │
  │                krowl_inbox_queue_size                     │
  │                                                          │
  │  pebble ─────► krowl_pebble_disk_usage_bytes             │
  │  redis ──────► krowl_redis_pool_hits_total                │
  │  frontier ───► krowl_frontier_size                        │
  │                                                          │
  └──────────────────────────┬──────────────────────────────┘
                             │
                             ▼
                     /metrics endpoint
                    (Prometheus scrape)
```

## Categories

| Category | Example metrics |
|----------|----------------|
| **Fetch/HTTP** | `pages_fetched_total`, `fetch_errors_total`, `fetch_duration_seconds`, `dns_duration_seconds`, `tls_duration_seconds`, `ttfb_duration_seconds`, `response_size_bytes` |
| **DNS Cache** | `dns_cache_hits_total`, `dns_cache_misses_total`, `dns_cache_size`, `dns_cache_evictions_total` |
| **Parse/Links** | `urls_discovered_total`, `urls_deduped_total`, `urls_forwarded_total`, `links_per_page`, `sitemap_urls_discovered_total` |
| **Dedup** | `dedup_bloom_hits_total`, `dedup_bloom_false_positives_total`, `dedup_pebble_hits_total`, `dedup_new_urls_total` |
| **WARC** | `warc_bytes_written_total`, `warc_records_written_total`, `warc_file_rotations_total`, `warc_write_errors_total` |
| **Inbox** | `inbox_forwarded_total`, `inbox_received_total`, `inbox_queue_size`, `inbox_batch_size` |
| **Pebble** | `pebble_disk_usage_bytes`, `pebble_l0_files`, `pebble_l0_sublevels`, `pebble_read_amp`, `pebble_compaction_debt_bytes` |
| **Redis pool** | `redis_pool_hits_total`, `redis_pool_misses_total`, `redis_pool_timeouts_total`, `redis_pool_total_conns` |
| **Frontier** | `frontier_size`, `frontier_domains`, `active_domains`, `tracked_domains`, `topology_nodes` |

## API

- `Register()` — Register all metrics with the default Prometheus registry (call once at init)
- `StatusBucket(code)` — Map HTTP status code to bucket label (`"2xx"`, `"3xx"`, etc.)

## Dependencies

- `prometheus/client_golang`
