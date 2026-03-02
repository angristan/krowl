# sitemap

Fetches and parses XML sitemaps (including sitemap index files) to discover seed URLs for domains.

```
  robots.txt parsed
        │
        ├── Sitemap: hints ──┐
        │                    │
        ▼                    ▼
  GET /sitemap.xml    GET hinted URLs
        │                    │
        ▼                    ▼
  ┌──────────────────────────────────────┐
  │  XML parse                           │
  │                                      │
  │  <sitemapindex>?                     │
  │  ┌─────┴──────┐                     │
  │  yes          no                    │
  │  │            │                     │
  │  ▼            ▼                     │
  │  recurse    extract <url><loc>      │
  │  (max       up to 10K URLs          │
  │   depth 2)                          │
  └──────────────┬───────────────────────┘
                 │
                 ▼
           []string URLs
           (enqueued into domain.Manager)
```

Triggered automatically when a domain's robots.txt is first fetched. Checks `/sitemap.xml` plus any `Sitemap:` directives from robots.txt.

## Limits

| Constant | Value | Description |
|----------|-------|-------------|
| `MaxSitemapSize` | 10 MB | Maximum sitemap file size |
| `MaxURLsPerSite` | 10K | Cap on URLs extracted per domain |
| `MaxIndexDepth` | 2 | Maximum sitemap index recursion depth |

## API

- `NewFetcher(userAgent)` — Create fetcher with configured HTTP client
- `FetchURLs(domain, robotsHints)` — Fetch and parse sitemaps; return up to 10K URLs

## Dependencies

None (leaf package).
