// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	// ReadCompartmentIDPlaceholder is replaced with the read compartment ID when expanding templated
	// settings such as the Kafka topic name.
	ReadCompartmentIDPlaceholder = "<read-compartment-id>"

	// WriteCompartmentIDPlaceholder is replaced with the write compartment ID when expanding templated
	// settings such as the Kafka cluster address and SASL credentials.
	WriteCompartmentIDPlaceholder = "<write-compartment-id>"

	// MaxReadCompartments is the maximum number of read compartments, so a read compartment ID
	// always fits in a uint8.
	MaxReadCompartments = math.MaxUint8 + 1
)

var (
	ErrInvalidNumReadCompartments  = errors.New("compartments read.num-compartments must be greater than 0 when compartments are enabled")
	ErrTooManyReadCompartments     = fmt.Errorf("compartments read.num-compartments must not be greater than %d", MaxReadCompartments)
	ErrInvalidNumWriteCompartments = errors.New("compartments write.num-compartments must be greater than 0 when compartments are enabled")

	ErrCompartment0WithoutSuffixRequiresCompartments = errors.New("compartments compartment-0-without-suffix requires compartments to be enabled")
)

// Config holds the configuration for the compartments architecture.
type Config struct {
	Enabled                   bool        `yaml:"enabled"`
	Compartment0WithoutSuffix bool        `yaml:"compartment_0_without_suffix"`
	Read                      ReadConfig  `yaml:"read"`
	Write                     WriteConfig `yaml:"write"`
}

// ReadConfig holds the configuration of the read compartments.
type ReadConfig struct {
	NumCompartments int `yaml:"num_compartments"`
}

// WriteConfig holds the configuration of the write compartments.
type WriteConfig struct {
	NumCompartments int `yaml:"num_compartments"`
}

// RegisterFlags registers the compartments flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "compartments.enabled", false, "Whether the compartments architecture is enabled.")
	f.BoolVar(&cfg.Compartment0WithoutSuffix, "compartments.compartment-0-without-suffix", false, "Whether read compartment 0 and write compartment 0 use the names without the compartment suffix, which are the names used when compartments are disabled. Set the same value on every component.")
	cfg.Read.RegisterFlagsWithPrefix("compartments.read.", f)
	cfg.Write.RegisterFlagsWithPrefix("compartments.write.", f)
}

// RegisterFlagsWithPrefix registers the read compartments flags with the given prefix.
func (cfg *ReadConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of read compartments.")
}

// RegisterFlagsWithPrefix registers the write compartments flags with the given prefix.
func (cfg *WriteConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of write compartments. Each ingester consumes its partition from every write compartment's Kafka cluster.")
}

// Validate returns an error if the config is invalid.
func (cfg *Config) Validate() error {
	if !cfg.Enabled {
		if cfg.Compartment0WithoutSuffix {
			return ErrCompartment0WithoutSuffixRequiresCompartments
		}
		return nil
	}
	if cfg.Read.NumCompartments <= 0 {
		return ErrInvalidNumReadCompartments
	}
	if cfg.Read.NumCompartments > MaxReadCompartments {
		return ErrTooManyReadCompartments
	}
	if cfg.Write.NumCompartments <= 0 {
		return ErrInvalidNumWriteCompartments
	}
	return nil
}

// ReplaceReadCompartment returns s with the ReadCompartmentIDPlaceholder replaced by the read
// compartment ID.
func ReplaceReadCompartment(s string, readCompartmentID int) string {
	return strings.ReplaceAll(s, ReadCompartmentIDPlaceholder, strconv.Itoa(readCompartmentID))
}

// ReplaceWriteCompartment returns s with the WriteCompartmentIDPlaceholder replaced by the write
// compartment ID.
func ReplaceWriteCompartment(s string, writeCompartmentID int) string {
	return strings.ReplaceAll(s, WriteCompartmentIDPlaceholder, strconv.Itoa(writeCompartmentID))
}

// WithReadCompartmentSuffix returns name with the "-rc-<id>" suffix that scopes it to a single read
// compartment.
func WithReadCompartmentSuffix(name string, compartmentID int) string {
	return name + "-rc-" + strconv.Itoa(compartmentID)
}

// WithWriteCompartmentSuffix returns name with the "-wc-<id>" suffix that scopes it to a single write
// compartment.
func WithWriteCompartmentSuffix(name string, writeCompartmentID int) string {
	return name + "-wc-" + strconv.Itoa(writeCompartmentID)
}

// ReadCompartmentName returns name scoped to the given read compartment. With
// Compartment0WithoutSuffix, read compartment 0 uses name without a suffix.
func (cfg Config) ReadCompartmentName(name string, readCompartmentID int) string {
	if readCompartmentID == 0 && cfg.Compartment0WithoutSuffix {
		return name
	}
	return WithReadCompartmentSuffix(name, readCompartmentID)
}

// WriteCompartmentName returns name scoped to the given write compartment. With
// Compartment0WithoutSuffix, write compartment 0 uses name without a suffix.
func (cfg Config) WriteCompartmentName(name string, writeCompartmentID int) string {
	if writeCompartmentID == 0 && cfg.Compartment0WithoutSuffix {
		return name
	}
	return WithWriteCompartmentSuffix(name, writeCompartmentID)
}

// BlocksCacheBucketID returns the ID that scopes the blocks metadata cache keys of the given read
// compartment. The ID is empty when compartments are disabled, and for read compartment 0 with
// Compartment0WithoutSuffix.
func (cfg Config) BlocksCacheBucketID(readCompartmentID int) string {
	if !cfg.Enabled || (readCompartmentID == 0 && cfg.Compartment0WithoutSuffix) {
		return ""
	}
	return WithReadCompartmentSuffix("blocks", readCompartmentID)
}
