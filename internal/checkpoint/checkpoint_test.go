package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "frontier.ckpt")

	queues := map[string][]string{
		"example.com":    {"https://example.com/a", "https://example.com/b"},
		"golang.org":     {"https://golang.org/doc"},
		"empty.com":      {},
		"bigqueue.local": make([]string, 1000),
	}
	for i := range queues["bigqueue.local"] {
		queues["bigqueue.local"][i] = "https://bigqueue.local/page/" + string(rune('A'+i%26))
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

	// Verify all domains and URLs match
	for domain, wantURLs := range queues {
		gotURLs, ok := got[domain]
		if !ok {
			t.Errorf("domain %q missing after roundtrip", domain)
			continue
		}
		if len(gotURLs) != len(wantURLs) {
			t.Errorf("domain %q: got %d URLs, want %d", domain, len(gotURLs), len(wantURLs))
			continue
		}
		for i, u := range wantURLs {
			if gotURLs[i] != u {
				t.Errorf("domain %q url[%d]: got %q, want %q", domain, i, gotURLs[i], u)
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
	initial := map[string][]string{"a.com": {"https://a.com/1"}}
	if err := Save(path, initial); err != nil {
		t.Fatalf("Save initial: %v", err)
	}

	// Write second checkpoint
	updated := map[string][]string{"b.com": {"https://b.com/2", "https://b.com/3"}}
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
	if urls, ok := got["b.com"]; !ok || len(urls) != 2 {
		t.Errorf("expected b.com with 2 URLs, got %v", got)
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
	if err := Save(path, map[string][]string{}); err != nil {
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
