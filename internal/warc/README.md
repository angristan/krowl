# warc

Writes WARC 1.1 files with gzip compression and size-based rotation to JuiceFS-mounted shared storage.

```
  fetch.Result
       │
       ▼
  WriteResult()
       │
       ├──► WARC request record
       │    ┌─────────────────────────────┐
       │    │ WARC/1.1                    │
       │    │ WARC-Type: request          │
       │    │ WARC-Record-ID: <uuid>      │
       │    │ WARC-Target-URI: https://...│
       │    │                             │
       │    │ GET /path HTTP/1.1          │
       │    │ Host: example.com           │
       │    └─────────────────────────────┘
       │
       └──► WARC response record
            ┌─────────────────────────────┐
            │ WARC/1.1                    │
            │ WARC-Type: response         │
            │ WARC-Concurrent-To: <uuid>  │
            │                             │
            │ HTTP/1.1 200 OK             │
            │ Content-Type: text/html     │
            │                             │
            │ <html>...</html>            │
            └─────────────────────────────┘
                    │
                    ▼
            gzip compressed stream
                    │
          ┌─────────┴──────────┐
          │  rotate at 1 GB    │
          │  KROWL-{ts}-node   │
          │  {id}-{seq}.warc.gz│
          └────────────────────┘
               JuiceFS /mnt/jfs/warcs/
```

Each fetch produces a paired WARC request + response record linked via `WARC-Concurrent-To`. Files rotate at 1 GB and include a `warcinfo` record at the start of each file.

## File naming

```
KROWL-{timestamp}-node{id}-{seqnum}.warc.gz
```

## Key constants

| Constant | Value | Description |
|----------|-------|-------------|
| `MaxFileSize` | 1 GB | Rotation threshold |
| `WARCVersion` | `WARC/1.1` | WARC spec version |
| `DefaultPrefix` | `KROWL` | Filename prefix |

## API

- `NewWriter(dir, nodeID, software)` — Create writer, open first file, write `warcinfo` record
- `WriteResult(result)` — Write paired request + response records
- `Close()` — Flush and close current file

## Dependencies

- `internal/fetch` (for `Result` type)
- `internal/metrics`
- `google/uuid`
