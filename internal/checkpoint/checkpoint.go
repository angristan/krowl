// Package checkpoint persists and restores the crawler frontier
// (per-domain URL queues) so that a restart doesn't lose queued work.
//
// Format: gob-encoded map[string][]string (domain → URLs).
// Writes are atomic: encode to a temp file, then rename.
package checkpoint

import (
	"encoding/gob"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Save atomically writes the frontier queues to path.
func Save(path string, queues map[string][]string) error {
	start := time.Now()

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".ckpt-*.tmp")
	if err != nil {
		return fmt.Errorf("checkpoint: create temp: %w", err)
	}
	tmpName := tmp.Name()

	// Clean up temp file on any error
	ok := false
	defer func() {
		if !ok {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()

	enc := gob.NewEncoder(tmp)
	if err := enc.Encode(queues); err != nil {
		return fmt.Errorf("checkpoint: encode: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("checkpoint: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("checkpoint: close: %w", err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}

	ok = true

	// Count total URLs for logging
	total := 0
	for _, urls := range queues {
		total += len(urls)
	}

	slog.Info("checkpoint saved",
		"path", path,
		"domains", len(queues),
		"urls", total,
		"duration", time.Since(start).Round(time.Millisecond),
	)
	return nil
}

// Load reads a frontier checkpoint from path.
// Returns an empty map (not an error) if the file does not exist.
func Load(path string) (map[string][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string][]string), nil
		}
		return nil, fmt.Errorf("checkpoint: open: %w", err)
	}
	defer f.Close()

	var queues map[string][]string
	if err := gob.NewDecoder(f).Decode(&queues); err != nil {
		return nil, fmt.Errorf("checkpoint: decode: %w", err)
	}

	total := 0
	for _, urls := range queues {
		total += len(urls)
	}
	slog.Info("checkpoint loaded",
		"path", path,
		"domains", len(queues),
		"urls", total,
	)
	return queues, nil
}
