# urlnorm

Aggressive URL normalization for deduplication. Ensures that two URLs returning the same content normalize to the same string.

```
  Input: "HTTP://WWW.Example.COM:80/path/?utm_source=x&b=2&a=1#frag"
                    │
                    ▼
         ┌─────────────────────┐
         │  1. lowercase scheme │  http
         │  2. lowercase host   │  www.example.com
         │  3. strip www.       │  example.com
         │  4. strip port :80   │  example.com
         │  5. strip fragment   │  (remove #frag)
         │  6. strip utm_source │  (tracking param)
         │  7. sort query params│  ?a=1&b=2
         │  8. strip trailing / │
         └─────────┬───────────┘
                   ▼
  Output: "http://example.com/path?a=1&b=2"
```

## Normalizations applied

- Lowercase scheme and host
- Strip `www.` prefix
- Remove default ports (`:80`, `:443`)
- Remove trailing slashes
- Remove fragment (`#...`)
- Sort query parameters alphabetically
- Strip ~30 tracking query parameters (UTM, fbclid, gclid, msclkid, HubSpot, Mailchimp, etc.)

## API

- `Normalize(rawURL)` — Full URL normalization
- `NormalizeDomain(rawURL)` — Extract and normalize just the hostname

## Dependencies

None (leaf package).
