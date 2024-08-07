// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/header.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"flag"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"

	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

const (
	DefaultIndexHeaderLazyLoadingEnabled     = true
	DefaultIndexHeaderLazyLoadingIdleTimeout = 60 * time.Minute
)

// NotFoundRangeErr is an error returned by PostingsOffset when there is no posting for given name and value pairs.
var NotFoundRangeErr = errors.New("range not found") //nolint:revive

var (
	errInvalidIndexHeaderLazyLoadingConcurrency = errors.New("invalid index-header lazy loading max concurrency; must be non-negative")
)

// Reader is an interface allowing to read essential, minimal number of index fields from the small portion of index file called header.
type Reader interface {
	// Close should be called when this instance of Reader will no longer be used.
	// It is illegal to call Close multiple times.
	Close() error

	// IndexVersion returns version of index.
	IndexVersion(context.Context) (int, error)

	// PostingsOffset returns start and end offsets of postings for given name and value.
	// The Start is inclusive and is the byte offset of the number_of_entries field of a posting list.
	// The End is exclusive and is typically the byte offset of the CRC32 field.
	// The End might be bigger than the actual posting ending, but not larger than the whole index file.
	// NotFoundRangeErr is returned when no index can be found for given name and value.
	PostingsOffset(ctx context.Context, name string, value string) (index.Range, error)

	// LookupSymbol returns string based on given reference.
	// Error is return if the symbol can't be found.
	LookupSymbol(ctx context.Context, o uint32) (string, error)

	SymbolsReader(ctx context.Context) (streamindex.SymbolsReader, error)

	// LabelValuesOffsets returns all label values and the offsets for their posting lists for given label name or error.
	// The returned label values are sorted lexicographically (which the same as sorted by posting offset).
	// The ranges of each posting list are the same as returned by PostingsOffset.
	// If no values are found for label name, or label name does not exists,
	// then empty slice is returned and no error.
	// If non-empty prefix is provided, only posting lists starting with the prefix are returned.
	// If non-nil filter is provided, then only posting lists for which filter returns true are returned.
	LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error)

	// LabelNames returns all label names in sorted order.
	LabelNames(ctx context.Context) ([]string, error)
}

type Config struct {
	MaxIdleFileHandles         uint `yaml:"max_idle_file_handles" category:"advanced"`
	EagerLoadingStartupEnabled bool `yaml:"eager_loading_startup_enabled" category:"experimental"`

	// Controls whether index-header lazy loading is enabled.
	LazyLoadingEnabled     bool          `yaml:"lazy_loading_enabled" category:"advanced"`
	LazyLoadingIdleTimeout time.Duration `yaml:"lazy_loading_idle_timeout" category:"advanced"`

	// Maximum index-headers loaded into store-gateway concurrently
	LazyLoadingConcurrency             int           `yaml:"lazy_loading_concurrency" category:"advanced"`
	LazyLoadingConcurrencyQueueTimeout time.Duration `yaml:"lazy_loading_concurrency_queue_timeout" category:"advanced"`

	VerifyOnLoad bool `yaml:"verify_on_load" category:"advanced"`

	// EagerLoadingPersistInterval is injected for testing purposes only.
	EagerLoadingPersistInterval time.Duration `yaml:"-" doc:"hidden"`
}

func (cfg *Config) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.UintVar(&cfg.MaxIdleFileHandles, prefix+"max-idle-file-handles", 1, "Maximum number of idle file handles the store-gateway keeps open for each index-header file.")
	f.BoolVar(&cfg.LazyLoadingEnabled, prefix+"lazy-loading-enabled", DefaultIndexHeaderLazyLoadingEnabled, "If enabled, store-gateway will lazy load an index-header only once required by a query.")
	f.DurationVar(&cfg.LazyLoadingIdleTimeout, prefix+"lazy-loading-idle-timeout", DefaultIndexHeaderLazyLoadingIdleTimeout, "If index-header lazy loading is enabled and this setting is > 0, the store-gateway will offload unused index-headers after 'idle timeout' inactivity.")
	f.IntVar(&cfg.LazyLoadingConcurrency, prefix+"lazy-loading-concurrency", 4, "Maximum number of concurrent index header loads across all tenants. If set to 0, concurrency is unlimited.")
	f.DurationVar(&cfg.LazyLoadingConcurrencyQueueTimeout, prefix+"lazy-loading-concurrency-queue-timeout", 5*time.Second, "Timeout for the queue of index header loads. If the queue is full and the timeout is reached, the load will return an error. 0 means no timeout and the load will wait indefinitely.")
	f.BoolVar(&cfg.EagerLoadingStartupEnabled, prefix+"eager-loading-startup-enabled", true, "If enabled, store-gateway will periodically persist block IDs of lazy loaded index-headers and load them eagerly during startup. Ignored if index-header lazy loading is disabled.")
	f.DurationVar(&cfg.EagerLoadingPersistInterval, prefix+"eager-loading-persist-interval", time.Minute, "Interval at which the store-gateway persists block IDs of lazy loaded index-headers. Ignored if index-header eager loading is disabled.")
	f.BoolVar(&cfg.VerifyOnLoad, prefix+"verify-on-load", false, "If true, verify the checksum of index headers upon loading them (either on startup or lazily when lazy loading is enabled). Setting to true helps detect disk corruption at the cost of slowing down index header loading.")
}

func (cfg *Config) Validate() error {
	if cfg.LazyLoadingConcurrency < 0 {
		return errInvalidIndexHeaderLazyLoadingConcurrency
	}
	return nil
}
