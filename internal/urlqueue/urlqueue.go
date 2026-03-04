// Package urlqueue implements a bbolt-backed per-domain URL queue.
//
// bbolt structure:
//
//	bucket "urls"
//	├── sub-bucket "example.com"    (per-domain bucket)
//	│   ├── <seq_be64> → <depth_varint><url_bytes>
//	│   └── ...
//	└── sub-bucket "other.org"
//	    └── ...
//	bucket "meta"
//	├── "example.com" → 46-byte domain state
//	└── ...
//
// Per-domain sub-buckets give O(1) first-key seek for Dequeue and
// O(1) bucket delete for DropDomain (no tombstone churn).
//
// Lightweight in-memory maps track per-domain counts and sequence
// numbers. These are rebuilt from a single scan on startup.
package urlqueue

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	urlsBucket = []byte("urls")
	metaBucket = []byte("meta")
)

// Item is a URL with its crawl depth.
type Item struct {
	URL   string
	Depth int
}

// Queue is a bbolt-backed per-domain URL queue.
type Queue struct {
	db *bolt.DB

	mu     sync.Mutex
	counts map[string]int    // domain -> queue length
	seqs   map[string]uint64 // domain -> next sequence number

	total atomic.Int64 // global count of queued URLs
}

// Open creates or opens a bbolt-backed URL queue at the given file path.
// If the database already contains data, in-memory counters are rebuilt
// from a full scan.
func Open(path string) (*Queue, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{
		FreelistType: bolt.FreelistMapType,
	})
	if err == nil {
		// Reduce batch delay from default 10ms to 1ms. Still amortizes
		// concurrent callers during crawling, but doesn't waste time
		// when a single goroutine is the only caller.
		db.MaxBatchDelay = 1 * time.Millisecond
	}
	if err != nil {
		return nil, err
	}

	// Create top-level buckets if they don't exist.
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(urlsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	})
	if err != nil {
		_ = db.Close()
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

// rebuild scans all sub-buckets to restore in-memory counters and sequence numbers.
func (q *Queue) rebuild() error {
	return q.db.View(func(tx *bolt.Tx) error {
		urls := tx.Bucket(urlsBucket)
		return urls.ForEachBucket(func(domainKey []byte) error {
			domain := string(domainKey)
			sub := urls.Bucket(domainKey)

			var count int
			var maxSeq uint64

			err := sub.ForEach(func(k, _ []byte) error {
				count++
				seq := binary.BigEndian.Uint64(k)
				if seq+1 > maxSeq {
					maxSeq = seq + 1
				}
				return nil
			})
			if err != nil {
				return err
			}

			if count > 0 {
				q.counts[domain] = count
				q.seqs[domain] = maxSeq
				q.total.Add(int64(count))
			}
			return nil
		})
	})
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

	key := encodeSeq(seq)
	val := encodeValue(depth, rawURL)
	domainBytes := []byte(domain)

	err := q.db.Batch(func(tx *bolt.Tx) error {
		sub, err := tx.Bucket(urlsBucket).CreateBucketIfNotExists(domainBytes)
		if err != nil {
			return err
		}
		return sub.Put(key, val)
	})
	if err != nil {
		// Rollback in-memory counters on write failure.
		q.mu.Lock()
		q.counts[domain]--
		if q.counts[domain] <= 0 {
			delete(q.counts, domain)
		}
		q.total.Add(-1)
		q.mu.Unlock()
		return false
	}
	return true
}

// EnqueueBatch adds multiple URLs in a single bbolt transaction (one fsync).
// The caller provides a function that receives an enqueue callback. The callback
// returns false if the per-domain or global cap is exceeded. Used for seed loading
// where single-threaded Batch() would waste 10ms per call waiting for concurrency.
func (q *Queue) EnqueueBatch(maxPerDomain int, maxTotal int64, fn func(enqueue func(domain, rawURL string, depth int) bool)) error {
	// Collect items under lock, then write all at once.
	type item struct {
		domain string
		seq    uint64
		key    []byte
		val    []byte
	}
	var items []item

	q.mu.Lock()
	fn(func(domain, rawURL string, depth int) bool {
		if maxPerDomain > 0 && q.counts[domain] >= maxPerDomain {
			return false
		}
		if maxTotal > 0 && q.total.Load() >= maxTotal {
			return false
		}
		seq := q.seqs[domain]
		q.seqs[domain] = seq + 1
		q.counts[domain]++
		q.total.Add(1)
		items = append(items, item{
			domain: domain,
			seq:    seq,
			key:    encodeSeq(seq),
			val:    encodeValue(depth, rawURL),
		})
		return true
	})
	q.mu.Unlock()

	if len(items) == 0 {
		return nil
	}

	err := q.db.Update(func(tx *bolt.Tx) error {
		urls := tx.Bucket(urlsBucket)
		for i := range items {
			sub, err := urls.CreateBucketIfNotExists([]byte(items[i].domain))
			if err != nil {
				return err
			}
			if err := sub.Put(items[i].key, items[i].val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Rollback in-memory counters.
		q.mu.Lock()
		for _, it := range items {
			q.counts[it.domain]--
			if q.counts[it.domain] <= 0 {
				delete(q.counts, it.domain)
			}
			q.total.Add(-1)
		}
		q.mu.Unlock()
	}
	return err
}

// Dequeue pops the next URL from a domain's queue.
// Returns the item and true, or a zero item and false if empty.
func (q *Queue) Dequeue(domain string) (Item, bool) {
	var item Item
	var found bool
	domainBytes := []byte(domain)

	err := q.db.Batch(func(tx *bolt.Tx) error {
		sub := tx.Bucket(urlsBucket).Bucket(domainBytes)
		if sub == nil {
			return nil
		}
		c := sub.Cursor()
		k, v := c.First()
		if k == nil {
			return nil
		}
		depth, url := decodeValue(v)
		item = Item{URL: url, Depth: depth}
		found = true
		return c.Delete()
	})
	if err != nil {
		return Item{}, false
	}

	if found {
		q.mu.Lock()
		q.counts[domain]--
		if q.counts[domain] <= 0 {
			delete(q.counts, domain)
		}
		q.total.Add(-1)
		q.mu.Unlock()
	}

	return item, found
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
	domainBytes := []byte(domain)
	err := q.db.Update(func(tx *bolt.Tx) error {
		urls := tx.Bucket(urlsBucket)
		if urls.Bucket(domainBytes) == nil {
			return nil
		}
		return urls.DeleteBucket(domainBytes)
	})
	if err != nil {
		return
	}
	q.mu.Lock()
	q.total.Add(-int64(q.counts[domain]))
	delete(q.counts, domain)
	q.mu.Unlock()
}

// Stats returns the underlying bbolt database stats.
func (q *Queue) Stats() bolt.Stats {
	return q.db.Stats()
}

// DBPath returns the file path of the underlying bbolt database.
func (q *Queue) DBPath() string {
	return q.db.Path()
}

// Close closes the underlying bbolt database.
func (q *Queue) Close() error {
	return q.db.Close()
}

// --- encoding helpers ---

func encodeSeq(seq uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, seq)
	return key
}

func encodeValue(depth int, url string) []byte {
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
// Domain state is stored in a separate "meta" bucket.
// Key:   <domain>
// Value: caller-defined opaque bytes (typically binary-encoded struct)

// SaveMeta stores opaque metadata for a domain.
func (q *Queue) SaveMeta(domain string, data []byte) {
	_ = q.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).Put([]byte(domain), data)
	})
}

// SaveMetaBatch writes metadata for many domains in a single transaction.
// The caller provides a function that receives a put callback.
func (q *Queue) SaveMetaBatch(fn func(put func(domain string, data []byte))) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		var firstErr error
		fn(func(domain string, data []byte) {
			if firstErr != nil {
				return
			}
			if err := b.Put([]byte(domain), data); err != nil {
				firstErr = err
			}
		})
		return firstErr
	})
}

// LoadMeta retrieves metadata for a domain. Returns nil if not found.
func (q *Queue) LoadMeta(domain string) []byte {
	var out []byte
	_ = q.db.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(metaBucket).Get([]byte(domain))
		if val != nil {
			out = make([]byte, len(val))
			copy(out, val)
		}
		return nil
	})
	return out
}

// IterMeta calls fn for every stored domain metadata entry.
// Used at startup to restore domain state.
func (q *Queue) IterMeta(fn func(domain string, data []byte)) {
	_ = q.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).ForEach(func(k, v []byte) error {
			// Copy value since bbolt tx buffers are only valid within tx.
			val := make([]byte, len(v))
			copy(val, v)
			fn(string(k), val)
			return nil
		})
	})
}

// DeleteMeta removes metadata for a domain.
func (q *Queue) DeleteMeta(domain string) {
	_ = q.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metaBucket).Delete([]byte(domain))
	})
}

