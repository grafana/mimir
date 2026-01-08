// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	cacheExtJSON  = ".json"
	cacheExtJSONL = ".jsonl"
)

// FileCache provides simple file-based caching for development iteration.
type FileCache struct {
	enabled bool
	dir     string
}

// NewFileCache creates a new FileCache.
// Creates the cache directory if caching is enabled.
func NewFileCache(enabled bool, dir string) (*FileCache, error) {
	if enabled {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory: %w", err)
		}
	}
	return &FileCache{enabled: enabled, dir: dir}, nil
}

// Set writes data to cache file. No-op if cache is disabled.
func (c *FileCache) Set(key string, data any) error {
	if !c.enabled {
		return nil
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	filename := key + cacheExtJSON
	path := filepath.Join(c.dir, filename)
	if err := os.WriteFile(path, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	return nil
}

// Get reads cached data from file into target.
// Returns (false, nil) if cache is disabled or file doesn't exist.
// Returns (true, error) if file exists but reading/unmarshaling fails.
// Returns (true, nil) on success.
func (c *FileCache) Get(key string, target any) (bool, error) {
	if !c.enabled {
		return false, nil
	}

	filename := key + cacheExtJSON
	path := filepath.Join(c.dir, filename)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return true, fmt.Errorf("failed to read cache file: %w", err)
	}

	if err := json.Unmarshal(data, target); err != nil {
		return true, fmt.Errorf("failed to unmarshal cache data: %w", err)
	}

	return true, nil
}

// StreamWrite creates a new streaming cache writer.
// Returns nil if cache is disabled.
func (c *FileCache) StreamWrite(key string) (*cacheEntryWriter, error) {
	if !c.enabled {
		return nil, nil
	}

	filename := key + cacheExtJSONL
	path := filepath.Join(c.dir, filename)
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache file: %w", err)
	}

	buf := bufio.NewWriter(file)
	return &cacheEntryWriter{
		file: file,
		buf:  buf,
		enc:  json.NewEncoder(buf),
	}, nil
}

// StreamRead reads cached entries in streaming fashion, calling handler for each line.
// Returns true if cache hit (file exists and was read successfully), false otherwise.
func (c *FileCache) StreamRead(key string, handler func(line []byte) error) bool {
	if !c.enabled {
		return false
	}

	filename := key + cacheExtJSONL
	path := filepath.Join(c.dir, filename)
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Increase buffer size for potentially large JSON lines.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		if err := handler(scanner.Bytes()); err != nil {
			return false
		}
	}

	return scanner.Err() == nil
}

// cacheEntryWriter writes entries to a cache file in JSONL format (one JSON object per line).
type cacheEntryWriter struct {
	file *os.File
	buf  *bufio.Writer
	enc  *json.Encoder
}

// Write appends an entry to the cache file as a JSON line.
func (w *cacheEntryWriter) Write(entry any) error {
	return w.enc.Encode(entry)
}

// Close flushes the buffer and closes the cache entry writer.
func (w *cacheEntryWriter) Close() error {
	if err := w.buf.Flush(); err != nil {
		w.file.Close()
		return err
	}
	return w.file.Close()
}

// buildKey creates a safe cache key from the given parts (without extension).
// Long parts are hashed to keep filenames reasonable.
func buildKey(parts ...string) string {
	var safeParts []string
	for _, part := range parts {
		// Replace unsafe characters.
		safe := strings.NewReplacer("/", "_", ":", "_", " ", "_").Replace(part)
		// Hash long parts to keep filename reasonable.
		if len(safe) > 50 {
			hash := sha256.Sum256([]byte(part))
			safe = hex.EncodeToString(hash[:8])
		}
		safeParts = append(safeParts, safe)
	}
	return strings.Join(safeParts, "-")
}
