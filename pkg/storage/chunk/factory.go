// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/encoding/factory.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"fmt"
)

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
type Encoding byte

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

const (
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	// IMPORTANT: for backward compatibility reasons we need to keep the value hardcoded.
	PrometheusXorChunk Encoding = 4
	// PrometheusHistogramChunk is a wrapper around Prometheus histogram-encoded chunk.
	// IMPORTANT: for backward compatibility reasons we need to keep the value hardcoded.
	PrometheusHistogramChunk Encoding = 5
)

type encoding struct {
	Name string
	New  func() EncodedChunk
}

var encodings = map[Encoding]encoding{
	PrometheusXorChunk: {
		Name: "PrometheusXorChunk",
		New: func() EncodedChunk {
			return newPrometheusXorChunk()
		},
	},
	PrometheusHistogramChunk: {
		Name: "PrometheusHistogramChunk",
		New: func() EncodedChunk {
			return newPrometheusHistogramChunk()
		},
	},
}

// NewForEncoding allows configuring what chunk type you want
func NewForEncoding(encoding Encoding) (EncodedChunk, error) {
	enc, ok := encodings[encoding]
	if !ok {
		return nil, fmt.Errorf("unknown chunk encoding: %v", encoding)
	}

	return enc.New(), nil
}
