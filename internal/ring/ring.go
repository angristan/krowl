// Package ring implements consistent hashing for domain-to-node assignment.
// The ring is built dynamically from Consul's service catalog.
package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const DefaultVnodes = 128

type Node struct {
	ID        int
	Addr      string // VPC IP:port for Redis LPUSH
	RedisAddr string
}

type Ring struct {
	mu     sync.RWMutex
	vnodes int
	ring   []uint64
	owners map[uint64]int // hash position -> node ID
	nodes  map[int]Node   // node ID -> node info
	sorted bool
}

func New(vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVnodes
	}
	return &Ring{
		vnodes: vnodes,
		owners: make(map[uint64]int),
		nodes:  make(map[int]Node),
	}
}

// SetNodes replaces the entire ring with a new set of nodes.
// Called when Consul reports a topology change.
func (r *Ring) SetNodes(nodes []Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ring = r.ring[:0]
	r.owners = make(map[uint64]int, len(nodes)*r.vnodes)
	r.nodes = make(map[int]Node, len(nodes))

	for _, n := range nodes {
		r.nodes[n.ID] = n
		for i := 0; i < r.vnodes; i++ {
			h := hash(fmt.Sprintf("node-%d-vnode-%d", n.ID, i))
			r.ring = append(r.ring, h)
			r.owners[h] = n.ID
		}
	}

	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
	r.sorted = true
}

// Owner returns the node ID that owns the given domain.
func (r *Ring) Owner(domain string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return -1
	}

	h := hash(domain)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	return r.owners[r.ring[idx]]
}

// GetNode returns the node info for the given node ID.
func (r *Ring) GetNode(id int) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[id]
	return n, ok
}

// Nodes returns all current nodes.
func (r *Ring) Nodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	return out
}

// NodeCount returns the number of nodes in the ring.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

// HashDomain is exported so other packages use the same hash function.
func HashDomain(domain string) uint64 {
	return hash(domain)
}
