# metrics

Centralized Prometheus metrics registry. Single source of truth for all `krowl_*` metrics.

```
  ┌─────────────────────────────────────────────────────────┐
  │                   metrics.Register()                     │
  │                                                          │
  │  fetch.Pool ──► krowl_pages_fetched_total                │
  │                 krowl_fetch_duration_seconds              │
  │                 krowl_connect_duration_seconds            │
  │                 krowl_tls_version_total                   │
  │                 krowl_response_size_bytes                 │
  │                                                          │
  │  parse.Pool ──► krowl_urls_discovered_total               │
  │                 krowl_urls_deduped_total                  │
  │                 krowl_links_per_page                      │
  │                                                          │
  │  dedup ──────► krowl_dedup_bloom_hits_total               │
  │                krowl_dedup_pebble_hits_total              │
  │                                                          │
  │  gowarc ─────► krowl_warc_data_bytes_total               │
  │                krowl_warc_inflight_writers                │
  │                krowl_warc_write_errors_total              │
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
| **Fetch/HTTP** | `pages_fetched_total`, `fetch_errors_total`, `fetch_duration_seconds`, `connect_duration_seconds`, `ttfb_duration_seconds`, `response_size_bytes`, `tls_version_total`, `tls_cipher_total`, `ip_version_total`, `response_encoding_total` |
| **Parse/Links** | `urls_discovered_total`, `urls_deduped_total`, `urls_forwarded_total`, `links_per_page`, `sitemap_urls_discovered_total` |
| **Dedup** | `dedup_lookups_total`, `dedup_bloom_hits_total`, `dedup_new_urls_total` |
| **WARC** | `warc_data_bytes_total` (from gowarc DataTotal), `warc_inflight_writers` (from gowarc WaitGroup), `warc_write_errors_total` (from gowarc ErrChan) |
| **Inbox** | `inbox_forwarded_total`, `inbox_received_total`, `inbox_queue_size`, `inbox_batch_size` |
| **Pebble (dedup)** | `pebble_disk_usage_bytes`, `pebble_l0_files`, `pebble_l0_sublevels`, `pebble_read_amp`, `pebble_compaction_debt_bytes` |
| **bbolt (URL queue)** | `bbolt_disk_size_bytes`, `bbolt_free_pages`, `bbolt_pending_pages`, `bbolt_free_alloc_bytes`, `bbolt_open_readers` |
| **Redis pool** | `redis_pool_hits_total`, `redis_pool_misses_total`, `redis_pool_timeouts_total`, `redis_pool_total_conns` |
| **Frontier** | `frontier_size`, `frontier_domains`, `active_domains`, `tracked_domains`, `topology_nodes`, `parse_active_workers`, `chan_parse_len` |

## API

- `Register()` — Register all metrics with the default Prometheus registry (call once at init)
- `StatusBucket(code)` — Map HTTP status code to bucket label (`"2xx"`, `"3xx"`, etc.)

## Dependencies

- `prometheus/client_golang`
