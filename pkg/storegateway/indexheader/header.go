// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/header.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"flag"
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

// NotFoundRangeErr is an error returned by PostingsOffset when there is no posting for given name and value pairs.
var NotFoundRangeErr = errors.New("range not found") //nolint:revive

// Reader is an interface allowing to read essential, minimal number of index fields from the small portion of index file called header.
type Reader interface {
	io.Closer

	// IndexVersion returns version of index.
	IndexVersion() (int, error)

	// PostingsOffset returns start and end offsets of postings for given name and value.
	// The end offset might be bigger than the actual posting ending, but not larger than the whole index file.
	// NotFoundRangeErr is returned when no index can be found for given name and value.
	// TODO(bwplotka): Move to PostingsOffsets(name string, value ...string) []index.Range and benchmark.
	PostingsOffset(name string, value string) (index.Range, error)

	// LookupSymbol returns string based on given reference.
	// Error is return if the symbol can't be found.
	LookupSymbol(o uint32) (string, error)

	// LabelValues returns all label values for given label name or error.
	// If no values are found for label name, or label name does not exists,
	// then empty string is returned and no error.
	// If non-nil filter is provided, then only values for which filter returns true are returned.
	LabelValues(name string, filter func(string) bool) ([]string, error)

	// LabelNames returns all label names in sorted order.
	LabelNames() ([]string, error)
}

type Config struct {
	MapPopulateEnabled  bool `yaml:"map_populate_enabled" category:"experimental"`
	StreamReaderEnabled bool `yaml:"stream_reader_enabled" category:"experimental"`
	FileHandlePoolSize  uint `yaml:"file_handle_pool_size" category:"experimental"`
}

func (cfg *Config) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.MapPopulateEnabled, prefix+"map-populate-enabled", false, "If enabled, the store-gateway will attempt to pre-populate the file system cache when memory-mapping index-header files.")
	f.BoolVar(&cfg.StreamReaderEnabled, prefix+"stream-reader-enabled", false, "If enabled, the store-gateway will use an experimental streaming reader to load and parse index-header files.")
	f.UintVar(&cfg.FileHandlePoolSize, prefix+"file-handle-pool-size", 1, "Max number of file handles the store-gateway will keep open for each index-header file when using the streaming reader.")
}
