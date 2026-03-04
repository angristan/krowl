// Package inbox handles cross-shard URL forwarding via Redis LPUSH/LPOP.
// Each node has a local Redis with an "inbox" list that peers push to.
package inbox

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stanislas/krowl/internal/dedup"
	"github.com/stanislas/krowl/internal/domain"
	m "github.com/stanislas/krowl/internal/metrics"
	"github.com/stanislas/krowl/internal/ring"
)

const (
	inboxKey     = "inbox"
	batchSize    = 500
	pollInterval = 50 * time.Millisecond
	numConsumers = 8 // parallel drain goroutines
)

// Sender forwards cross-shard URLs to the owning node's Redis inbox.
type Sender struct {
	peers map[int]*redis.Client // node ID -> Redis client
	ring  *ring.Ring
	myID  int
	dedup *dedup.Dedup
}

// NewSender creates a sender that can forward URLs to peer nodes.
func NewSender(r *ring.Ring, myID int, d *dedup.Dedup) *Sender {
	return &Sender{
		peers: make(map[int]*redis.Client),
		ring:  r,
		myID:  myID,
		dedup: d,
	}
}

// UpdatePeers refreshes the Redis connections to peer nodes.
// Called when the Consul topology changes.
func (s *Sender) UpdatePeers(nodes []ring.Node) {
	newPeers := make(map[int]*redis.Client, len(nodes))
	for _, n := range nodes {
		if n.ID == s.myID {
			continue
		}
		// Reuse existing connection if available
		if existing, ok := s.peers[n.ID]; ok && existing != nil {
			newPeers[n.ID] = existing
		} else {
			newPeers[n.ID] = redis.NewClient(&redis.Options{
				Addr:     n.RedisAddr,
				PoolSize: 32,
			})
		}
	}
	// Close removed peers
	for id, client := range s.peers {
		if _, ok := newPeers[id]; !ok {
			_ = client.Close()
		}
	}
	s.peers = newPeers
}

// Forward sends a URL to the owning node's inbox.
// The URL has already passed local dedup. The depth is encoded in the
// wire format as "depth\turl".
func (s *Sender) Forward(ctx context.Context, rawURL string, depth int) error {
	d := domain.ExtractDomain(rawURL)
	owner := s.ring.Owner(d)

	if owner == s.myID || owner == -1 {
		return nil // shouldn't happen, caller should check
	}

	client, ok := s.peers[owner]
	if !ok {
		return nil // node not available, skip
	}

	wire := fmt.Sprintf("%d\t%s", depth, rawURL)
	err := client.LPush(ctx, inboxKey, wire).Err()
	if err != nil {
		m.InboxForwardErrors.Inc()
		return err
	}
	m.InboxForwardedTotal.Inc()
	return nil
}

// Close closes all peer connections.
func (s *Sender) Close() {
	for _, client := range s.peers {
		_ = client.Close()
	}
}

// Consumer drains the local inbox and feeds URLs into the domain manager.
type Consumer struct {
	rdb     *redis.Client // local Redis
	dedup   *dedup.Dedup
	domains *domain.Manager
}

// NewConsumer creates an inbox consumer that pops URLs from local Redis.
func NewConsumer(localRedis *redis.Client, d *dedup.Dedup, dm *domain.Manager) *Consumer {
	return &Consumer{
		rdb:     localRedis,
		dedup:   d,
		domains: dm,
	}
}

// Run starts the consumer loop. Blocks until context is cancelled.
// Spawns numConsumers parallel goroutines, each popping batches from Redis.
func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.drain(ctx)
		}()
	}
	wg.Wait()
}

func (c *Consumer) drain(ctx context.Context) {
	type newURL struct {
		Domain string
		URL    string
		depth  int
	}
	buf := make([]newURL, 0, batchSize)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		urls, err := c.rdb.LPopCount(ctx, inboxKey, batchSize).Result()
		if err == nil && len(urls) > 0 {
			m.InboxReceivedTotal.Add(float64(len(urls)))
			m.InboxBatchSize.Observe(float64(len(urls)))
		}
		if err != nil || len(urls) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		// Phase 1: bloom-filter all URLs (fast, in-memory).
		// Collect only genuinely new ones.
		buf = buf[:0]
		for _, wire := range urls {
			rawURL, depth := decodeWire(wire)
			if c.dedup.IsNew(rawURL) {
				buf = append(buf, newURL{
					Domain: domain.ExtractDomain(rawURL),
					URL:    rawURL,
					depth:  depth,
				})
			}
		}

		if len(buf) == 0 {
			continue
		}

		// Phase 2: batch-enqueue new URLs (single bbolt txn).
		items := make([]struct{ Domain, URL string; Depth int }, len(buf))
		for i, u := range buf {
			items[i] = struct{ Domain, URL string; Depth int }{u.Domain, u.URL, u.depth}
		}
		c.domains.EnqueueBatchWithDepth(items)
	}
}

// decodeWire parses the "depth\turl" wire format.
// Falls back to depth 0 for plain URL strings (backward compat).
func decodeWire(wire string) (string, int) {
	if idx := strings.IndexByte(wire, '\t'); idx > 0 {
		depth, err := strconv.Atoi(wire[:idx])
		if err == nil {
			return wire[idx+1:], depth
		}
	}
	return wire, 0
}
