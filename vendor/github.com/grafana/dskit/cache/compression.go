// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/golang/snappy"

	"github.com/grafana/dskit/internal/slices"
)

const (
	// CompressionSnappy is the value of the snappy compression.
	CompressionSnappy = "snappy"
)

var (
	supportedCompressions     = []string{CompressionSnappy}
	errUnsupportedCompression = errors.New("unsupported compression")

	_ Cache = (*SnappyCache)(nil)
)

type CompressionConfig struct {
	Compression string `yaml:"compression"`
}

// RegisterFlagsWithPrefix registers flags with provided prefix.
func (cfg *CompressionConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Compression, prefix+"compression", "", fmt.Sprintf("Enable cache compression, if not empty. Supported values are: %s.", strings.Join(supportedCompressions, ", ")))
}

func (cfg *CompressionConfig) Validate() error {
	if cfg.Compression != "" && !slices.Contains(supportedCompressions, cfg.Compression) {
		return errUnsupportedCompression
	}

	return nil
}

func NewCompression(cfg CompressionConfig, next Cache, logger log.Logger) Cache {
	switch cfg.Compression {
	case CompressionSnappy:
		return NewSnappy(next, logger)
	default:
		// No compression.
		return next
	}
}

type SnappyCache struct {
	next   Cache
	logger log.Logger
}

// NewSnappy makes a new snappy encoding cache wrapper.
func NewSnappy(next Cache, logger log.Logger) *SnappyCache {
	return &SnappyCache{
		next:   next,
		logger: logger,
	}
}

// SetAsync implements Cache.
func (s *SnappyCache) SetAsync(key string, value []byte, ttl time.Duration) {
	s.next.SetAsync(key, snappy.Encode(nil, value), ttl)
}

// SetMultiAsync implements Cache.
func (s *SnappyCache) SetMultiAsync(data map[string][]byte, ttl time.Duration) {
	encoded := make(map[string][]byte, len(data))
	for key, value := range data {
		encoded[key] = snappy.Encode(nil, value)
	}

	s.next.SetMultiAsync(encoded, ttl)
}

func (s *SnappyCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.next.Set(ctx, key, snappy.Encode(nil, value), ttl)
}

func (s *SnappyCache) Add(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return s.next.Add(ctx, key, snappy.Encode(nil, value), ttl)
}

// GetMulti implements Cache.
func (s *SnappyCache) GetMulti(ctx context.Context, keys []string, opts ...Option) map[string][]byte {
	found := s.next.GetMulti(ctx, keys, opts...)
	decoded := make(map[string][]byte, len(found))

	for key, encodedValue := range found {
		decodedValue, err := snappy.Decode(nil, encodedValue)
		if err != nil {
			level.Error(s.logger).Log("msg", "failed to decode cache entry", "err", err)
			continue
		}

		decoded[key] = decodedValue
	}

	return decoded
}

// Stop implements Cache.
func (s *SnappyCache) Stop() {
	s.next.Stop()
}

// Name implements Cache.
func (s *SnappyCache) Name() string {
	return s.next.Name()
}

// Delete implements Cache.
func (s *SnappyCache) Delete(ctx context.Context, key string) error {
	return s.next.Delete(ctx, key)
}
