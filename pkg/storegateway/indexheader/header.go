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

	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

// NotFoundRangeErr is an error returned by PostingsOffset when there is no posting for given name and value pairs.
var NotFoundRangeErr = errors.New("range not found") //nolint:revive

// Reader is an interface allowing to read essential, minimal number of index fields from the small portion of index file called header.
type Reader interface {
	io.Closer

	// IndexVersion returns version of index.
	IndexVersion() (int, error)

	// PostingsOffset returns start and end offsets of postings for given name and value.
	// The Start is inclusive and is the byte offset of the number_of_entries field of a posting list.
	// The End is exclusive and is typically the byte offset of the CRC32 field.
	// The End might be bigger than the actual posting ending, but not larger than the whole index file.
	// NotFoundRangeErr is returned when no index can be found for given name and value.
	PostingsOffset(name string, value string) (index.Range, error)

	// LookupSymbol returns string based on given reference.
	// Error is return if the symbol can't be found.
	LookupSymbol(o uint32) (string, error)

	// LabelValues returns all label values for given label name or error.
	// If no values are found for label name, or label name does not exists,
	// then empty slice is returned and no error.
	// If non-empty prefix is provided, only values starting with the prefix are returned.
	// If non-nil filter is provided, then only values for which filter returns true are returned.
	LabelValues(name string, prefix string, filter func(string) bool) ([]string, error)

	// LabelValuesOffsets returns all label values and the offsets for their posting lists for given label name or error.
	// The ranges of each posting list are the same as returned by PostingsOffset.
	// If no values are found for label name, or label name does not exists,
	// then empty slice is returned and no error.
	// If non-empty prefix is provided, only posting lists starting with the prefix are returned.
	// If non-nil filter is provided, then only posting lists for which filter returns true are returned.
	LabelValuesOffsets(name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error)

	// LabelNames returns all label names in sorted order.
	LabelNames() ([]string, error)
}

type Config struct {
	MaxIdleFileHandles uint `yaml:"max_idle_file_handles" category:"advanced"`
}

func (cfg *Config) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.UintVar(&cfg.MaxIdleFileHandles, prefix+"max-idle-file-handles", 1, "Maximum number of idle file handles the store-gateway keeps open for each index-header file.")
}
