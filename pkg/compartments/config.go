// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"errors"
	"flag"
	"strconv"
	"strings"

	"github.com/grafana/dskit/tenant"
)

const (
	// ReadCompartmentIDPlaceholder is replaced with the read compartment ID when expanding templated
	// settings such as the Kafka topic name.
	ReadCompartmentIDPlaceholder = "<read-compartment-id>"

	// WriteCompartmentIDPlaceholder is replaced with the write compartment ID when expanding templated
	// settings such as the Kafka cluster address and SASL credentials.
	WriteCompartmentIDPlaceholder = "<write-compartment-id>"
)

var (
	ErrInvalidNumReadCompartments  = errors.New("compartments read.num-compartments must be greater than 0 when compartments are enabled")
	ErrInvalidNumWriteCompartments = errors.New("compartments write.num-compartments must be greater than 0 when compartments are enabled")
)

// Config holds the configuration for the compartments architecture.
type Config struct {
	Enabled bool        `yaml:"enabled"`
	Read    ReadConfig  `yaml:"read"`
	Write   WriteConfig `yaml:"write"`
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
		return nil
	}
	if cfg.Read.NumCompartments <= 0 {
		return ErrInvalidNumReadCompartments
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

// ReadCompartmentTenantIDInfix is inserted into tenant IDs as "/rc/<id>" before any metadata.
const ReadCompartmentTenantIDInfix = "/rc/"

// TenantIDWithReadCompartment returns tenantID with the "/rc/<compartmentID>" suffix. tenantID must
// not contain tenant metadata. Use tenant.Metadata.WithTenant to add the metadata after the suffix.
func TenantIDWithReadCompartment(tenantID string, compartmentID int) string {
	return tenantID + ReadCompartmentTenantIDInfix + strconv.Itoa(compartmentID)
}

// TrimReadCompartmentFromTenantID removes a well-formed "/rc/<digits>" that sits right before the
// tenant metadata or at the end of tenantIDWithMetadata. It returns the input unchanged when no
// such suffix is present.
func TrimReadCompartmentFromTenantID(tenantIDWithMetadata string) string {
	tenantID, metadata, ok := SplitReadCompartmentTenantID(tenantIDWithMetadata)
	if !ok {
		return tenantIDWithMetadata
	}
	return tenantID + metadata
}

// SplitReadCompartmentTenantID splits tenantIDWithMetadata into the tenant ID without a
// well-formed "/rc/<compartmentID>" suffix and the metadata (including the leading ':').
// ok is false when no well-formed suffix is present. The returned strings are slices of the
// input and do not allocate.
func SplitReadCompartmentTenantID(tenantIDWithMetadata string) (tenantID, metadata string, ok bool) {
	// Most tenant IDs have no read compartment suffix. IndexByte finds those IDs faster than LastIndex.
	if strings.IndexByte(tenantIDWithMetadata, '/') < 0 {
		return "", "", false
	}

	tenantWithCompartment, metadata := tenantIDWithMetadata, ""
	if idx := strings.IndexByte(tenantIDWithMetadata, ':'); idx != -1 {
		tenantWithCompartment, metadata = tenantIDWithMetadata[:idx], tenantIDWithMetadata[idx:]
	}

	rcIdx := strings.LastIndex(tenantWithCompartment, ReadCompartmentTenantIDInfix)
	if rcIdx < 0 {
		return "", "", false
	}

	if _, err := strconv.Atoi(tenantWithCompartment[rcIdx+len(ReadCompartmentTenantIDInfix):]); err != nil {
		return "", "", false
	}
	return tenantWithCompartment[:rcIdx], metadata, true
}

// ParseTenantIDWithMetadata parses tenantIDWithMetadata like tenant.ParseWithMetadata. It also accepts
// a well-formed "/rc/<compartmentID>" suffix before the metadata, and the returned tenantID keeps that
// suffix. It does not allocate when the input is valid.
func ParseTenantIDWithMetadata(tenantIDWithMetadata string) (tenantID string, metadata tenant.Metadata, err error) {
	baseTenantID, metadataSource, ok := SplitReadCompartmentTenantID(tenantIDWithMetadata)
	if !ok {
		return tenant.ParseWithMetadata(tenantIDWithMetadata)
	}
	if err := tenant.ValidTenantID(baseTenantID); err != nil {
		return "", tenant.Metadata{}, err
	}
	if metadata, err = tenant.ParseMetadata(metadataSource); err != nil {
		return "", tenant.Metadata{}, err
	}
	return tenantIDWithMetadata[:len(tenantIDWithMetadata)-len(metadataSource)], metadata, nil
}
