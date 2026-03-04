package ring

import (
	"fmt"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	t.Run("positive vnodes", func(t *testing.T) {
		r := New(64)
		if r.vnodes != 64 {
			t.Fatalf("expected vnodes=64, got %d", r.vnodes)
		}
		if r.owners == nil {
			t.Fatal("owners map should be initialized")
		}
		if r.nodes == nil {
			t.Fatal("nodes map should be initialized")
		}
	})

	t.Run("zero vnodes defaults", func(t *testing.T) {
		r := New(0)
		if r.vnodes != DefaultVnodes {
			t.Fatalf("expected vnodes=%d, got %d", DefaultVnodes, r.vnodes)
		}
	})

	t.Run("negative vnodes defaults", func(t *testing.T) {
		r := New(-5)
		if r.vnodes != DefaultVnodes {
			t.Fatalf("expected vnodes=%d, got %d", DefaultVnodes, r.vnodes)
		}
	})
}

func makeNodes(n int) []Node {
	nodes := make([]Node, n)
	for i := range nodes {
		nodes[i] = Node{
			ID:        i + 1,
			RedisAddr: fmt.Sprintf("10.0.0.%d:6380", i+1),
		}
	}
	return nodes
}

func TestSetNodes(t *testing.T) {
	t.Run("populates ring", func(t *testing.T) {
		r := New(32)
		nodes := makeNodes(3)
		r.SetNodes(nodes)

		if r.NodeCount() != 3 {
			t.Fatalf("expected 3 nodes, got %d", r.NodeCount())
		}
		// 3 nodes * 32 vnodes = 96 ring positions
		if len(r.ring) != 3*32 {
			t.Fatalf("expected %d ring entries, got %d", 3*32, len(r.ring))
		}
	})

	t.Run("ring is sorted", func(t *testing.T) {
		r := New(64)
		r.SetNodes(makeNodes(5))
		for i := 1; i < len(r.ring); i++ {
			if r.ring[i] < r.ring[i-1] {
				t.Fatalf("ring not sorted at index %d: %d < %d", i, r.ring[i], r.ring[i-1])
			}
		}
	})

	t.Run("empty node list clears ring", func(t *testing.T) {
		r := New(32)
		r.SetNodes(makeNodes(3))
		if r.NodeCount() != 3 {
			t.Fatal("expected 3 nodes before clear")
		}
		r.SetNodes(nil)
		if r.NodeCount() != 0 {
			t.Fatalf("expected 0 nodes after clear, got %d", r.NodeCount())
		}
		if len(r.ring) != 0 {
			t.Fatalf("expected empty ring after clear, got %d entries", len(r.ring))
		}
	})
}

func TestOwnerEmpty(t *testing.T) {
	r := New(32)
	if id := r.Owner("example.com"); id != -1 {
		t.Fatalf("expected -1 for empty ring, got %d", id)
	}
}

func TestOwnerConsistency(t *testing.T) {
	r := New(128)
	r.SetNodes(makeNodes(5))

	domains := []string{
		"example.com",
		"google.com",
		"github.com",
		"stackoverflow.com",
		"reddit.com",
		"wikipedia.org",
		"amazon.com",
		"twitter.com",
		"facebook.com",
		"linkedin.com",
	}

	// Record owner for each domain
	owners := make(map[string]int, len(domains))
	for _, d := range domains {
		owners[d] = r.Owner(d)
	}

	// Call again many times — must be stable
	for i := 0; i < 100; i++ {
		for _, d := range domains {
			if got := r.Owner(d); got != owners[d] {
				t.Fatalf("Owner(%q) changed from %d to %d on iteration %d", d, owners[d], got, i)
			}
		}
	}
}

func TestOwnerDistribution(t *testing.T) {
	r := New(128)
	nodes := makeNodes(4)
	r.SetNodes(nodes)

	counts := make(map[int]int) // nodeID -> count
	numDomains := 10000
	for i := 0; i < numDomains; i++ {
		d := fmt.Sprintf("domain-%d.example.com", i)
		owner := r.Owner(d)
		counts[owner]++
	}

	// With 4 nodes and 128 vnodes, we expect roughly 2500 per node.
	// Allow wide tolerance: each node should get at least 5% of total.
	minExpected := numDomains * 5 / 100
	for _, n := range nodes {
		c := counts[n.ID]
		if c < minExpected {
			t.Errorf("node %d got only %d domains (min expected %d out of %d)", n.ID, c, minExpected, numDomains)
		}
	}

	// Verify all domains are assigned to one of the known node IDs
	for id := range counts {
		found := false
		for _, n := range nodes {
			if n.ID == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("domain assigned to unknown node ID %d", id)
		}
	}
}

func TestOwnerAfterTopologyChange(t *testing.T) {
	r := New(128)

	// Initial topology: 3 nodes
	r.SetNodes(makeNodes(3))

	domains := make([]string, 200)
	for i := range domains {
		domains[i] = fmt.Sprintf("site-%d.example.com", i)
	}

	ownersBefore := make(map[string]int, len(domains))
	for _, d := range domains {
		ownersBefore[d] = r.Owner(d)
	}

	// Change topology: 5 nodes (IDs 1-5, so nodes 1-3 overlap)
	r.SetNodes(makeNodes(5))

	changed := 0
	for _, d := range domains {
		after := r.Owner(d)
		if after != ownersBefore[d] {
			changed++
		}
		// Every domain must still map to a valid node (1-5)
		if after < 1 || after > 5 {
			t.Fatalf("Owner(%q) returned invalid node %d after topology change", d, after)
		}
	}

	// At least some domains should remap when adding nodes
	if changed == 0 {
		t.Error("expected at least some domains to remap after adding 2 nodes, but none did")
	}
	t.Logf("topology change: %d/%d domains remapped", changed, len(domains))
}

func TestGetNode(t *testing.T) {
	r := New(32)
	nodes := makeNodes(3)
	r.SetNodes(nodes)

	for _, n := range nodes {
		got, ok := r.GetNode(n.ID)
		if !ok {
			t.Fatalf("GetNode(%d) returned ok=false", n.ID)
		}
		if got.ID != n.ID || got.RedisAddr != n.RedisAddr {
			t.Fatalf("GetNode(%d) = %+v, want %+v", n.ID, got, n)
		}
	}

	// Non-existent node
	_, ok := r.GetNode(999)
	if ok {
		t.Fatal("GetNode(999) should return ok=false for non-existent node")
	}
}

func TestNodeCount(t *testing.T) {
	r := New(32)

	if r.NodeCount() != 0 {
		t.Fatalf("expected 0 nodes initially, got %d", r.NodeCount())
	}

	r.SetNodes(makeNodes(3))
	if r.NodeCount() != 3 {
		t.Fatalf("expected 3 nodes, got %d", r.NodeCount())
	}

	r.SetNodes(makeNodes(7))
	if r.NodeCount() != 7 {
		t.Fatalf("expected 7 nodes after replacement, got %d", r.NodeCount())
	}

	r.SetNodes(nil)
	if r.NodeCount() != 0 {
		t.Fatalf("expected 0 nodes after clearing, got %d", r.NodeCount())
	}
}

func TestNodes(t *testing.T) {
	r := New(32)
	r.SetNodes(makeNodes(4))

	all := r.Nodes()
	if len(all) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(all))
	}

	ids := make(map[int]bool)
	for _, n := range all {
		ids[n.ID] = true
	}
	for i := 1; i <= 4; i++ {
		if !ids[i] {
			t.Errorf("node ID %d not found in Nodes()", i)
		}
	}
}

func TestHashDomain(t *testing.T) {
	// Deterministic
	h1 := HashDomain("example.com")
	h2 := HashDomain("example.com")
	if h1 != h2 {
		t.Fatal("HashDomain not deterministic")
	}

	// Different domains produce different hashes (probabilistically)
	h3 := HashDomain("other.com")
	if h1 == h3 {
		t.Fatal("different domains produced the same hash")
	}
}

func TestConcurrency(t *testing.T) {
	r := New(64)
	r.SetNodes(makeNodes(3))

	var wg sync.WaitGroup
	const readers = 8
	const writers = 2
	const iterations = 5000

	// Readers: call Owner() concurrently
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				d := fmt.Sprintf("domain-%d-%d.com", id, j)
				owner := r.Owner(d)
				// Owner should be -1 (empty ring) or a valid positive node ID
				if owner != -1 && owner < 1 {
					t.Errorf("invalid owner %d for %s", owner, d)
				}
			}
		}(i)
	}

	// Writers: call SetNodes() concurrently
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations/10; j++ {
				n := (j % 5) + 1 // 1 to 5 nodes
				r.SetNodes(makeNodes(n))
			}
		}(i)
	}

	wg.Wait()

	// If we get here without a data race or panic, the test passes.
	// Verify the ring is still usable.
	owner := r.Owner("final-check.com")
	if owner == -1 {
		t.Log("ring ended up empty (unlikely but possible)")
	}
}

func TestOwnerSingleNode(t *testing.T) {
	r := New(32)
	r.SetNodes(makeNodes(1))

	// Every domain should map to node 1
	for i := 0; i < 100; i++ {
		d := fmt.Sprintf("domain-%d.com", i)
		if owner := r.Owner(d); owner != 1 {
			t.Fatalf("Owner(%q) = %d, expected 1 with single node", d, owner)
		}
	}
}

func TestSetNodesReplacesFully(t *testing.T) {
	r := New(32)

	// Set initial nodes 1-3
	r.SetNodes(makeNodes(3))
	if _, ok := r.GetNode(1); !ok {
		t.Fatal("node 1 should exist")
	}

	// Replace with completely different nodes (IDs 10, 20)
	r.SetNodes([]Node{
		{ID: 10, RedisAddr: "10.0.0.10:6380"},
		{ID: 20, RedisAddr: "10.0.0.20:6380"},
	})

	// Old nodes should be gone
	if _, ok := r.GetNode(1); ok {
		t.Fatal("node 1 should not exist after replacement")
	}
	if _, ok := r.GetNode(2); ok {
		t.Fatal("node 2 should not exist after replacement")
	}

	// New nodes should be present
	if _, ok := r.GetNode(10); !ok {
		t.Fatal("node 10 should exist")
	}
	if _, ok := r.GetNode(20); !ok {
		t.Fatal("node 20 should exist")
	}

	if r.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", r.NodeCount())
	}

	// Owner should return one of the new node IDs
	owner := r.Owner("test.com")
	if owner != 10 && owner != 20 {
		t.Fatalf("Owner returned %d, expected 10 or 20", owner)
	}
}

func TestDefaultVnodesConstant(t *testing.T) {
	if DefaultVnodes != 128 {
		t.Fatalf("DefaultVnodes = %d, expected 128", DefaultVnodes)
	}
}
