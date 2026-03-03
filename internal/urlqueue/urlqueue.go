// Package urlqueue implements a Pebble-backed per-domain URL queue.
//
// Key schema:
//
//	<domain>\x00<seq_be64>
//
// Value schema:
//
//	<depth_varint><url_bytes>
//
// All URLs for a domain are stored contiguously in sorted order.
// Enqueue appends with an incrementing sequence number (FIFO).
// Dequeue seeks to the first key for a domain and deletes it.
//
// Lightweight in-memory maps track per-domain counts and sequence
// numbers. These are rebuilt from a single Pebble scan on startup.
package urlqueue

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// Item is a URL with its crawl depth.
type Item struct {
	URL   string
	Depth int
}

// Queue is a Pebble-backed per-domain URL queue.
type Queue struct {
	db *pebble.DB

	mu     sync.Mutex
	counts map[string]int    // domain -> queue length
	seqs   map[string]uint64 // domain -> next sequence number

	total atomic.Int64 // global count of queued URLs
}

// Open creates or opens a Pebble-backed URL queue at the given path.
// If the database already contains data, in-memory counters are rebuilt
// from a full scan.
func Open(path string) (*Queue, error) {
	cache := pebble.NewCache(256 * 1024 * 1024) // 256MB block cache
	defer cache.Unref()

	db, err := pebble.Open(path, &pebble.Options{
		Cache:                       cache,
		MemTableSize:                64 << 20, // 64MB
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    func() int { return 2 },
		DisableWAL:                  true,
	})
	if err != nil {
		return nil, err
	}

	q := &Queue{
		db:     db,
		counts: make(map[string]int),
		seqs:   make(map[string]uint64),
	}

	if err := q.rebuild(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return q, nil
}

// rebuild scans all keys to restore in-memory counters and sequence numbers.
func (q *Queue) rebuild() error {
	iter, err := q.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		domain, seq := decodeKey(iter.Key())
		q.counts[domain]++
		if seq+1 > q.seqs[domain] {
			q.seqs[domain] = seq + 1
		}
	}

	var total int64
	for _, c := range q.counts {
		total += int64(c)
	}
	q.total.Store(total)

	return iter.Error()
}

// Enqueue adds a URL to a domain's queue. Returns false if the
// per-domain cap or global cap would be exceeded.
func (q *Queue) Enqueue(domain, rawURL string, depth, maxPerDomain int, maxTotal int64) bool {
	q.mu.Lock()
	if maxPerDomain > 0 && q.counts[domain] >= maxPerDomain {
		q.mu.Unlock()
		return false
	}
	if maxTotal > 0 && q.total.Load() >= maxTotal {
		q.mu.Unlock()
		return false
	}

	seq := q.seqs[domain]
	q.seqs[domain] = seq + 1
	q.counts[domain]++
	q.total.Add(1)
	q.mu.Unlock()

	key := encodeKey(domain, seq)
	val := encodeValue(depth, rawURL)
	_ = q.db.Set(key, val, pebble.NoSync)
	return true
}

// Dequeue pops the next URL from a domain's queue.
// Returns the item and true, or a zero item and false if empty.
func (q *Queue) Dequeue(domain string) (Item, bool) {
	prefix := []byte(domain + "\x00")

	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return Item{}, false
	}
	defer func() { _ = iter.Close() }()

	if !iter.First() {
		return Item{}, false
	}

	// Copy key before deleting (iterator keys are only valid until next call)
	key := make([]byte, len(iter.Key()))
	copy(key, iter.Key())

	depth, url := decodeValue(iter.Value())
	item := Item{URL: url, Depth: depth}

	_ = q.db.Delete(key, pebble.NoSync)

	q.mu.Lock()
	q.counts[domain]--
	if q.counts[domain] <= 0 {
		delete(q.counts, domain)
		// Keep seqs[domain] so future enqueues don't reuse sequence numbers
	}
	q.total.Add(-1)
	q.mu.Unlock()

	return item, true
}

// QueueLen returns the number of URLs queued for a domain.
func (q *Queue) QueueLen(domain string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.counts[domain]
}

// TotalLen returns the total number of queued URLs across all domains.
func (q *Queue) TotalLen() int64 {
	return q.total.Load()
}

// HasURLs returns true if the domain has any queued URLs.
func (q *Queue) HasURLs(domain string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.counts[domain] > 0
}

// ActiveDomains returns all domains that have queued URLs.
func (q *Queue) ActiveDomains() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.counts))
	for d := range q.counts {
		out = append(out, d)
	}
	return out
}

// DomainCount returns the number of domains with queued URLs.
func (q *Queue) DomainCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.counts)
}

// DropDomain removes all queued URLs for a domain.
func (q *Queue) DropDomain(domain string) {
	prefix := []byte(domain + "\x00")

	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return
	}

	batch := q.db.NewBatch()
	var count int
	for iter.First(); iter.Valid(); iter.Next() {
		_ = batch.Delete(iter.Key(), nil)
		count++
	}
	_ = iter.Close()

	if count > 0 {
		_ = batch.Commit(pebble.NoSync)
		q.mu.Lock()
		q.total.Add(-int64(q.counts[domain]))
		delete(q.counts, domain)
		q.mu.Unlock()
	} else {
		_ = batch.Close()
	}
}

// Metrics returns the underlying Pebble metrics.
func (q *Queue) Metrics() *pebble.Metrics {
	return q.db.Metrics()
}

// Close closes the underlying Pebble database.
func (q *Queue) Close() error {
	return q.db.Close()
}

// --- encoding helpers ---

func encodeKey(domain string, seq uint64) []byte {
	// domain + \x00 + 8-byte big-endian sequence
	key := make([]byte, len(domain)+1+8)
	copy(key, domain)
	key[len(domain)] = 0x00
	binary.BigEndian.PutUint64(key[len(domain)+1:], seq)
	return key
}

func decodeKey(key []byte) (domain string, seq uint64) {
	// Find the \x00 separator
	for i := len(key) - 9; i >= 0; i-- {
		if key[i] == 0x00 {
			domain = string(key[:i])
			seq = binary.BigEndian.Uint64(key[i+1:])
			return
		}
	}
	return string(key), 0
}

func encodeValue(depth int, url string) []byte {
	// varint(depth) + url bytes
	buf := make([]byte, binary.MaxVarintLen64+len(url))
	n := binary.PutVarint(buf, int64(depth))
	copy(buf[n:], url)
	return buf[:n+len(url)]
}

func decodeValue(val []byte) (depth int, url string) {
	d, n := binary.Varint(val)
	return int(d), string(val[n:])
}

// --- Domain metadata storage ---
//
// Domain state is stored with key prefix \x01 to separate from URL queue
// keys (which start with printable domain chars >= 0x20).
//
// Key:   \x01<domain>
// Value: caller-defined opaque bytes (typically gob-encoded struct)

const metaPrefix = 0x01

func metaKey(domain string) []byte {
	key := make([]byte, 1+len(domain))
	key[0] = metaPrefix
	copy(key[1:], domain)
	return key
}

// SaveMeta stores opaque metadata for a domain.
func (q *Queue) SaveMeta(domain string, data []byte) {
	_ = q.db.Set(metaKey(domain), data, pebble.NoSync)
}

// LoadMeta retrieves metadata for a domain. Returns nil if not found.
func (q *Queue) LoadMeta(domain string) []byte {
	val, closer, err := q.db.Get(metaKey(domain))
	if err != nil {
		return nil
	}
	// Copy before closing (val is only valid until closer.Close)
	out := make([]byte, len(val))
	copy(out, val)
	_ = closer.Close()
	return out
}

// IterMeta calls fn for every stored domain metadata entry.
// Used at startup to restore domain state.
func (q *Queue) IterMeta(fn func(domain string, data []byte)) {
	prefix := []byte{metaPrefix}
	iter, err := q.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: []byte{metaPrefix + 1},
	})
	if err != nil {
		return
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		domain := string(iter.Key()[1:]) // strip \x01 prefix
		// Copy value since iterator reuses buffers
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())
		fn(domain, val)
	}
}

// DeleteMeta removes metadata for a domain.
func (q *Queue) DeleteMeta(domain string) {
	_ = q.db.Delete(metaKey(domain), pebble.NoSync)
}

// prefixUpperBound returns the immediate successor prefix for
// Pebble range-limited iteration. E.g., "foo\x00" → "foo\x01".
func prefixUpperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper
		}
	}
	return nil // prefix was all 0xFF — no upper bound
}
