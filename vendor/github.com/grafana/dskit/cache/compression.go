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

	"github.com/grafana/dskit/util/stringsutil"
)

const (
	// CompressionSnappy is the value of the snappy compression.
	CompressionSnappy = "snappy"
)

var (
	supportedCompressions     = []string{CompressionSnappy}
	errUnsupportedCompression = errors.New("unsupported compression")
)

type CompressionConfig struct {
	Compression string `yaml:"compression"`
}

// RegisterFlagsWithPrefix registers flags with provided prefix.
func (cfg *CompressionConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Compression, prefix+"compression", "", fmt.Sprintf("Enable cache compression, if not empty. Supported values are: %s.", strings.Join(supportedCompressions, ", ")))
}

func (cfg *CompressionConfig) Validate() error {
	if cfg.Compression != "" && !stringsutil.SliceContains(supportedCompressions, cfg.Compression) {
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

type snappyCache struct {
	next   Cache
	logger log.Logger
}

// NewSnappy makes a new snappy encoding cache wrapper.
func NewSnappy(next Cache, logger log.Logger) Cache {
	return &snappyCache{
		next:   next,
		logger: logger,
	}
}

// Store implements Cache.
func (s *snappyCache) Store(ctx context.Context, data map[string][]byte, ttl time.Duration) {
	encoded := make(map[string][]byte, len(data))
	for key, value := range data {
		encoded[key] = snappy.Encode(nil, value)
	}

	s.next.Store(ctx, encoded, ttl)
}

// Fetch implements Cache.
func (s *snappyCache) Fetch(ctx context.Context, keys []string) map[string][]byte {
	found := s.next.Fetch(ctx, keys)
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

func (s *snappyCache) Name() string {
	return s.next.Name()
}
