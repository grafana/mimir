// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/continuoustest"
)

//go:embed default_definition.yaml
var defaultDefinitionYAML []byte

type Protocol string

const (
	ProtocolV1   Protocol = "v1"
	ProtocolV2   Protocol = "v2"
	ProtocolOTLP Protocol = "otlp"
)

func (p Protocol) writeProtocol() continuoustest.WriteProtocol {
	switch p {
	case ProtocolV2:
		return "prometheus2"
	case ProtocolOTLP:
		return "otlp-http"
	default:
		return "prometheus"
	}
}

// Family is a family of series, with its own sample format and dimensions.
type Family string

const (
	FamilyFloat           Family = "float"
	FamilyNativeHistogram Family = "native_histogram"
)

// MetricType is the metric type of a float series.
type MetricType string

const (
	MetricTypeCounter  MetricType = "counter"
	MetricTypeGauge    MetricType = "gauge"
	MetricTypeUnknown  MetricType = "unknown"
	MetricTypeInfo     MetricType = "info"
	MetricTypeStateset MetricType = "stateset"
)

// Charset is the set of characters that the names of metrics and labels are made of.
type Charset string

const (
	// CharsetLegacy names only use the characters allowed by the legacy name validation scheme.
	CharsetLegacy Charset = "legacy"
	// CharsetUTF8 names also use other characters, such as dots and non-ASCII letters.
	CharsetUTF8 Charset = "utf8"
)

// Unit is the unit of the metadata of a series.
type Unit string

const (
	UnitNone    Unit = "none"
	UnitSeconds Unit = "seconds"
	UnitBytes   Unit = "bytes"
)

// Help is the kind of help text of the metadata of a series.
type Help string

const (
	// HelpNone is an empty help text.
	HelpNone Help = "none"
	// HelpASCII is a sentence of ASCII characters.
	HelpASCII Help = "ascii"
	// HelpUTF8 also has non-ASCII characters and special characters, such as quotes, a backslash and a
	// newline.
	HelpUTF8 Help = "utf8"
)

type SampleType string

const (
	SampleTypeInt   SampleType = "int"
	SampleTypeFloat SampleType = "float"
)

type Kind string

const (
	KindCounter Kind = "counter"
	KindGauge   Kind = "gauge"
)

// Layout is the shape of the buckets of a native histogram.
type Layout string

const (
	// LayoutContiguous has positive buckets in a single span.
	LayoutContiguous Layout = "contiguous"
	// LayoutGaps has positive buckets in several spans separated by empty buckets.
	LayoutGaps Layout = "gaps"
	// LayoutNegative has negative and positive buckets, in several spans, and a populated zero bucket.
	LayoutNegative Layout = "negative"
	// LayoutZeroOnly only has observations in the zero bucket.
	LayoutZeroOnly Layout = "zero_only"
)

// Definition describes the behavior space to explore.
type Definition struct {
	// Protocols and WriteSizes are what each write picks from: a protocol, and how many series to
	// write a sample of.
	Protocols  []Protocol `yaml:"protocols"`
	WriteSizes []int      `yaml:"write_sizes"`

	// Charsets and Metadata define dimensions of the series of every family. Floats and
	// NativeHistograms define the other dimensions of the series of each family. A family without a
	// definition isn't explored.
	Charsets         []Charset                   `yaml:"charsets"`
	Metadata         MetadataDefinition          `yaml:"metadata"`
	Floats           *FloatsDefinition           `yaml:"floats"`
	NativeHistograms *NativeHistogramsDefinition `yaml:"native_histograms"`
}

// families returns the dimensions of the series of each explored family, in a fixed order. With a
// given seed, the order determines which series each write picks.
func (d Definition) families() [][]dimension {
	var families [][]dimension
	if d.Floats != nil {
		families = append(families, d.familyDimensions(FamilyFloat, d.Floats.dimensions()))
	}
	if d.NativeHistograms != nil {
		families = append(families, d.familyDimensions(FamilyNativeHistogram, d.NativeHistograms.dimensions()))
	}
	return families
}

func (d Definition) familyDimensions(family Family, dims []dimension) []dimension {
	return append([]dimension{
		newDimension(labelFamily, []Family{family}),
		newDimension(labelCharset, d.Charsets),
		newDimension(labelUnit, d.Metadata.Units),
		newDimension(labelHelp, d.Metadata.Helps),
	}, dims...)
}

func newDimension[T ~string](name string, values []T) dimension {
	d := dimension{name: name}
	for _, v := range values {
		d.values = append(d.values, string(v))
	}
	return d
}

// exclusions returns the pairs of values that can't be written together, when writing to a tenant
// with the given name validation scheme. The protocol of a write counts as a dimension. A pair only
// excludes the series that have both dimensions.
func exclusions(scheme model.ValidationScheme) []exclusion {
	custom := strconv.Itoa(int(histogram.CustomBucketsSchema))
	excl := []exclusion{
		// OTLP has neither unknown, info nor stateset metrics, so they would be written as gauges.
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelType, value2: string(MetricTypeUnknown)},
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelType, value2: string(MetricTypeInfo)},
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelType, value2: string(MetricTypeStateset)},
		// OTLP exponential histograms only have integer counts.
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelSampleType, value2: string(SampleTypeFloat)},
		// OTLP has no gauge histograms, so a gauge would be written as a cumulative histogram.
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelKind, value2: string(KindGauge)},
		// Custom buckets are written to OTLP as explicit bucket histograms, which the OTLP writer
		// doesn't support.
		{dim1: labelProtocol, value1: string(ProtocolOTLP), dim2: labelSchema, value2: custom},
		// Custom buckets have neither negative buckets nor a zero bucket.
		{dim1: labelSchema, value1: custom, dim2: labelLayout, value2: string(LayoutNegative)},
		{dim1: labelSchema, value1: custom, dim2: labelLayout, value2: string(LayoutZeroOnly)},
	}
	if scheme != model.UTF8Validation {
		// Remote write requests with UTF-8 names are rejected, while the OTLP endpoint escapes them.
		excl = append(excl,
			exclusion{dim1: labelProtocol, value1: string(ProtocolV1), dim2: labelCharset, value2: string(CharsetUTF8)},
			exclusion{dim1: labelProtocol, value1: string(ProtocolV2), dim2: labelCharset, value2: string(CharsetUTF8)},
		)
	}
	return excl
}

// MetadataDefinition lists the values of each metadata dimension of the series of every family.
type MetadataDefinition struct {
	Units []Unit `yaml:"units"`
	Helps []Help `yaml:"helps"`
}

func (d MetadataDefinition) validate() error {
	if err := validateValues("units", d.Units, []Unit{UnitNone, UnitSeconds, UnitBytes}); err != nil {
		return err
	}
	return validateValues("helps", d.Helps, []Help{HelpNone, HelpASCII, HelpUTF8})
}

// FloatsDefinition lists the values of each dimension of float series to explore.
type FloatsDefinition struct {
	Types []MetricType `yaml:"types"`
}

func (d FloatsDefinition) validate() error {
	return validateValues("types", d.Types, []MetricType{MetricTypeCounter, MetricTypeGauge, MetricTypeUnknown, MetricTypeInfo, MetricTypeStateset})
}

// dimensions returns the float dimensions in a fixed order.
func (d FloatsDefinition) dimensions() []dimension {
	return []dimension{newDimension(labelType, d.Types)}
}

// NativeHistogramsDefinition lists the values of each native histogram dimension to explore.
type NativeHistogramsDefinition struct {
	SampleTypes []SampleType `yaml:"sample_types"`
	Kinds       []Kind       `yaml:"kinds"`
	Schemas     []int32      `yaml:"schemas"`
	Layouts     []Layout     `yaml:"layouts"`
}

func (d NativeHistogramsDefinition) validate() error {
	if err := validateValues("sample_types", d.SampleTypes, []SampleType{SampleTypeInt, SampleTypeFloat}); err != nil {
		return err
	}
	if err := validateValues("kinds", d.Kinds, []Kind{KindCounter, KindGauge}); err != nil {
		return err
	}
	if err := validateValues("schemas", d.Schemas, nil); err != nil {
		return err
	}
	for _, s := range d.Schemas {
		if !histogram.IsExponentialSchema(s) && !histogram.IsCustomBucketsSchema(s) {
			return fmt.Errorf("schemas: %d is neither an exponential schema, between %d and %d, nor the custom buckets schema %d", s, histogram.ExponentialSchemaMin, histogram.ExponentialSchemaMax, histogram.CustomBucketsSchema)
		}
	}
	return validateValues("layouts", d.Layouts, []Layout{LayoutContiguous, LayoutGaps, LayoutNegative, LayoutZeroOnly})
}

// validateValues checks that values is non-empty and free of duplicates. If allowed is non-nil,
// it also checks that every value is one of allowed.
func validateValues[T comparable](name string, values, allowed []T) error {
	if len(values) == 0 {
		return fmt.Errorf("%s: at least one value is required", name)
	}
	for i, v := range values {
		if allowed != nil && !slices.Contains(allowed, v) {
			return fmt.Errorf("%s: unknown value %v, supported values are %v", name, v, allowed)
		}
		if slices.Contains(values[:i], v) {
			return fmt.Errorf("%s: duplicate value %v", name, v)
		}
	}
	return nil
}

// dimensions returns the native histogram dimensions in a fixed order.
func (d NativeHistogramsDefinition) dimensions() []dimension {
	schemas := dimension{name: labelSchema}
	for _, v := range d.Schemas {
		schemas.values = append(schemas.values, strconv.Itoa(int(v)))
	}
	return []dimension{
		newDimension(labelSampleType, d.SampleTypes),
		newDimension(labelKind, d.Kinds),
		schemas,
		newDimension(labelLayout, d.Layouts),
	}
}

// ParseDefinition parses and validates a definition.
func ParseDefinition(data []byte) (Definition, error) {
	var def Definition
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&def); err != nil {
		return Definition{}, fmt.Errorf("parsing definition: %w", err)
	}
	if err := validateValues("protocols", def.Protocols, []Protocol{ProtocolV1, ProtocolV2, ProtocolOTLP}); err != nil {
		return Definition{}, err
	}
	if err := validateValues("write_sizes", def.WriteSizes, nil); err != nil {
		return Definition{}, err
	}
	for _, s := range def.WriteSizes {
		if s <= 0 {
			return Definition{}, fmt.Errorf("write_sizes: %d is not positive", s)
		}
	}
	if err := validateValues("charsets", def.Charsets, []Charset{CharsetLegacy, CharsetUTF8}); err != nil {
		return Definition{}, err
	}
	if err := def.Metadata.validate(); err != nil {
		return Definition{}, fmt.Errorf("metadata: %w", err)
	}
	if def.Floats == nil && def.NativeHistograms == nil {
		return Definition{}, errors.New("at least one of floats and native_histograms is required")
	}
	if def.Floats != nil {
		if err := def.Floats.validate(); err != nil {
			return Definition{}, fmt.Errorf("floats: %w", err)
		}
	}
	if def.NativeHistograms != nil {
		if err := def.NativeHistograms.validate(); err != nil {
			return Definition{}, fmt.Errorf("native_histograms: %w", err)
		}
	}
	return def, nil
}

// LoadDefinition loads the definition from path, or the built-in default definition if path is empty.
func LoadDefinition(path string) (Definition, error) {
	if path == "" {
		return ParseDefinition(defaultDefinitionYAML)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Definition{}, fmt.Errorf("reading definition file: %w", err)
	}
	return ParseDefinition(data)
}
