// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
)

// Encoding defines which encoding we are using, delta, doubledelta, or varbit
type Encoding byte

// Config configures the behaviour of chunk encoding
type Config struct{}

var (
	// DefaultEncoding exported for use in unit tests elsewhere
	DefaultEncoding      = Bigchunk
	bigchunkSizeCapBytes = 0
)

// RegisterFlags registers configuration settings.
func (Config) RegisterFlags(f *flag.FlagSet) {
	f.Var(&DefaultEncoding, "ingester.chunk-encoding", "Encoding version to use for chunks.")
	f.IntVar(&bigchunkSizeCapBytes, "store.bigchunk-size-cap-bytes", bigchunkSizeCapBytes, "When using bigchunk encoding, start a new bigchunk if over this size (0 = unlimited)")
}

// Validate errors out if the encoding is set to Delta.
func (Config) Validate() error {
	if DefaultEncoding == Delta {
		// Delta is deprecated.
		return errors.New("delta encoding is deprecated")
	}
	return nil
}

// String implements flag.Value.
func (e Encoding) String() string {
	if known, found := encodings[e]; found {
		return known.Name
	}
	return fmt.Sprintf("%d", e)
}

// Set implements flag.Value.
func (e *Encoding) Set(s string) error {
	// First see if the name was given
	for k, v := range encodings {
		if s == v.Name {
			*e = k
			return nil
		}
	}
	// Otherwise, accept a number
	i, err := strconv.Atoi(s)
	if err != nil {
		return err
	}

	_, ok := encodings[Encoding(i)]
	if !ok {
		return fmt.Errorf("invalid chunk encoding: %s", s)
	}

	*e = Encoding(i)
	return nil
}

var encodings = map[Encoding]encoding{}

type encoding struct{ Name string }

const (
	// Delta encoding is no longer supported and will be automatically changed to DoubleDelta.
	// It still exists here to not change the  flag values.
	Delta Encoding = iota
	// DoubleDelta encoding
	DoubleDelta
	// Varbit encoding
	Varbit
	// Bigchunk encoding
	Bigchunk
	// PrometheusXorChunk is a wrapper around Prometheus XOR-encoded chunk.
	PrometheusXorChunk
)
