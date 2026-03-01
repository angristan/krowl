package checkpoint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stanislas/krowl/internal/domain"
)

func TestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier.ckpt")

	queues := map[string][]domain.QueueItem{
		"example.com": {
			{URL: "https://example.com/a", Depth: 0},
			{URL: "https://example.com/b", Depth: 1},
		},
		"golang.org": {
			{URL: "https://golang.org/doc", Depth: 2},
		},
		"empty.com":      {},
		"bigqueue.local": make([]domain.QueueItem, 1000),
	}
	for i := range queues["bigqueue.local"] {
		queues["bigqueue.local"][i] = domain.QueueItem{
			URL:   "https://bigqueue.local/page/" + string(rune('A'+i%26)),
			Depth: i % 10,
		}
	}

	if err := Save(path, queues); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// File should exist
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("checkpoint file not found: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Verify all domains and items match
	for d, wantItems := range queues {
		gotItems, ok := got[d]
		if !ok {
			t.Errorf("domain %q missing after roundtrip", d)
			continue
		}
		if len(gotItems) != len(wantItems) {
			t.Errorf("domain %q: got %d items, want %d", d, len(gotItems), len(wantItems))
			continue
		}
		for i, want := range wantItems {
			if gotItems[i].URL != want.URL || gotItems[i].Depth != want.Depth {
				t.Errorf("domain %q item[%d]: got %+v, want %+v", d, i, gotItems[i], want)
			}
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	got, err := Load("/nonexistent/path/frontier.ckpt")
	if err != nil {
		t.Fatalf("Load on missing file should not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %d entries", len(got))
	}
}

func TestSaveAtomicity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier.ckpt")

	// Write initial checkpoint
	initial := map[string][]domain.QueueItem{
		"a.com": {{URL: "https://a.com/1", Depth: 0}},
	}
	if err := Save(path, initial); err != nil {
		t.Fatalf("Save initial: %v", err)
	}

	// Write second checkpoint
	updated := map[string][]domain.QueueItem{
		"b.com": {
			{URL: "https://b.com/2", Depth: 1},
			{URL: "https://b.com/3", Depth: 2},
		},
	}
	if err := Save(path, updated); err != nil {
		t.Fatalf("Save updated: %v", err)
	}

	// Should have the updated data, not the initial
	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := got["a.com"]; ok {
		t.Error("initial data should have been replaced")
	}
	if items, ok := got["b.com"]; !ok || len(items) != 2 {
		t.Errorf("expected b.com with 2 items, got %v", got)
	}

	// No temp files should remain
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "frontier.ckpt" {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}
}

func TestEmptyCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier.ckpt")

	// Save empty map
	if err := Save(path, map[string][]domain.QueueItem{}); err != nil {
		t.Fatalf("Save empty: %v", err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %d entries", len(got))
	}
}
