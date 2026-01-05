//
// Copyright (c) 2025 The go-yaml Project Contributors
// SPDX-License-Identifier: Apache-2.0
//

package yaml

import (
	"errors"

	"go.yaml.in/yaml/v4/internal/libyaml"
)

// LineBreak represents the line ending style for YAML output.
type LineBreak = libyaml.LineBreak

// Line break constants for different platforms.
const (
	LineBreakLN   = libyaml.LN_BREAK   // Unix-style \n (default)
	LineBreakCR   = libyaml.CR_BREAK   // Old Mac-style \r
	LineBreakCRLN = libyaml.CRLN_BREAK // Windows-style \r\n
)

// options holds configuration for both loading and dumping YAML.
type options struct {
	// Loader options
	singleDocument bool
	knownFields    bool
	uniqueKeys     bool

	// Dumper options
	indent                int
	compactSeqIndent      bool
	lineWidth             int
	unicode               bool
	canonical             bool
	lineBreak             LineBreak
	explicitStart         bool
	explicitEnd           bool
	flowSimpleCollections bool
}

// Option allows configuring YAML loading and dumping operations.
type Option func(*options) error

// WithIndent sets the number of spaces to use for indentation when
// dumping YAML content.
//
// Valid values are 2-9. Common choices: 2 (compact), 4 (readable).
func WithIndent(indent int) Option {
	return func(o *options) error {
		if indent < 2 || indent > 9 {
			return errors.New("yaml: indent must be between 2 and 9 spaces")
		}
		o.indent = indent
		return nil
	}
}

// WithCompactSeqIndent configures whether the sequence indicator '- ' is
// considered part of the indentation when dumping YAML content.
//
// If compact is true, '- ' is treated as part of the indentation.
// If compact is false, '- ' is not treated as part of the indentation.
// When called without arguments, defaults to true.
func WithCompactSeqIndent(compact ...bool) Option {
	if len(compact) > 1 {
		panic("yaml: WithCompactSeqIndent accepts at most one argument")
	}
	val := len(compact) == 0 || compact[0]
	return func(o *options) error {
		o.compactSeqIndent = val
		return nil
	}
}

// WithKnownFields enables or disables strict field checking during YAML loading.
//
// When enabled, loading will return an error if the YAML input contains fields
// that do not correspond to any fields in the target struct.
// When called without arguments, defaults to true.
func WithKnownFields(knownFields ...bool) Option {
	if len(knownFields) > 1 {
		panic("yaml: WithKnownFields accepts at most one argument")
	}
	val := len(knownFields) == 0 || knownFields[0]
	return func(o *options) error {
		o.knownFields = val
		return nil
	}
}

// WithSingleDocument configures the Loader to only process the first document
// in a YAML stream. After the first document is loaded, subsequent calls to
// Load will return io.EOF.
//
// When called without arguments, defaults to true.
//
// This is useful when you expect exactly one document and want behavior
// similar to [Unmarshal].
func WithSingleDocument(singleDocument ...bool) Option {
	if len(singleDocument) > 1 {
		panic("yaml: WithSingleDocument accepts at most one argument")
	}
	val := len(singleDocument) == 0 || singleDocument[0]
	return func(o *options) error {
		o.singleDocument = val
		return nil
	}
}

// WithLineWidth sets the preferred line width for YAML output.
//
// When encoding long strings, the encoder will attempt to wrap them at this
// width using literal block style (|). Set to -1 or 0 for unlimited width.
//
// The default is 80 characters.
func WithLineWidth(width int) Option {
	return func(o *options) error {
		if width < 0 {
			width = -1
		}
		o.lineWidth = width
		return nil
	}
}

// WithUnicode controls whether non-ASCII characters are allowed in YAML output.
//
// When true, non-ASCII characters appear as-is (e.g., "café").
// When false, non-ASCII characters are escaped (e.g., "caf\u00e9").
// When called without arguments, defaults to true.
//
// The default is true.
func WithUnicode(unicode ...bool) Option {
	if len(unicode) > 1 {
		panic("yaml: WithUnicode accepts at most one argument")
	}
	val := len(unicode) == 0 || unicode[0]
	return func(o *options) error {
		o.unicode = val
		return nil
	}
}

// WithUniqueKeys enables or disables duplicate key detection during YAML loading.
//
// When enabled, loading will return an error if the YAML input contains
// duplicate keys in any mapping. This is a security feature that prevents
// key override attacks.
// When called without arguments, defaults to true.
//
// The default is true.
func WithUniqueKeys(uniqueKeys ...bool) Option {
	if len(uniqueKeys) > 1 {
		panic("yaml: WithUniqueKeys accepts at most one argument")
	}
	val := len(uniqueKeys) == 0 || uniqueKeys[0]
	return func(o *options) error {
		o.uniqueKeys = val
		return nil
	}
}

// WithCanonical forces canonical YAML output format.
//
// When enabled, the encoder outputs strictly canonical YAML with explicit
// tags for all values. This produces verbose output primarily useful for
// debugging and YAML spec compliance testing.
// When called without arguments, defaults to true.
//
// The default is false.
func WithCanonical(canonical ...bool) Option {
	if len(canonical) > 1 {
		panic("yaml: WithCanonical accepts at most one argument")
	}
	val := len(canonical) == 0 || canonical[0]
	return func(o *options) error {
		o.canonical = val
		return nil
	}
}

// WithLineBreak sets the line ending style for YAML output.
//
// Available options:
//   - LineBreakLN: Unix-style \n (default)
//   - LineBreakCR: Old Mac-style \r
//   - LineBreakCRLN: Windows-style \r\n
//
// The default is LineBreakLN.
func WithLineBreak(lineBreak LineBreak) Option {
	return func(o *options) error {
		o.lineBreak = lineBreak
		return nil
	}
}

// WithExplicitStart controls whether document start markers (---) are always emitted.
//
// When true, every document begins with an explicit "---" marker.
// When false (default), the marker is omitted for the first document.
// When called without arguments, defaults to true.
func WithExplicitStart(explicit ...bool) Option {
	if len(explicit) > 1 {
		panic("yaml: WithExplicitStart accepts at most one argument")
	}
	val := len(explicit) == 0 || explicit[0]
	return func(o *options) error {
		o.explicitStart = val
		return nil
	}
}

// WithExplicitEnd controls whether document end markers (...) are always emitted.
//
// When true, every document ends with an explicit "..." marker.
// When false (default), the marker is omitted.
// When called without arguments, defaults to true.
func WithExplicitEnd(explicit ...bool) Option {
	if len(explicit) > 1 {
		panic("yaml: WithExplicitEnd accepts at most one argument")
	}
	val := len(explicit) == 0 || explicit[0]
	return func(o *options) error {
		o.explicitEnd = val
		return nil
	}
}

// WithFlowSimpleCollections controls whether simple collections use flow style.
//
// When true, sequences and mappings containing only scalar values (no nested
// collections) are rendered in flow style if they fit within the line width.
// Example: {name: test, count: 42} or [a, b, c]
// When called without arguments, defaults to true.
//
// When false (default), all collections use block style.
func WithFlowSimpleCollections(flow ...bool) Option {
	if len(flow) > 1 {
		panic("yaml: WithFlowSimpleCollections accepts at most one argument")
	}
	val := len(flow) == 0 || flow[0]
	return func(o *options) error {
		o.flowSimpleCollections = val
		return nil
	}
}

// Options combines multiple options into a single Option.
// This is useful for creating option presets or combining version defaults
// with custom options.
//
// Example:
//
//	opts := yaml.Options(yaml.V4, yaml.WithIndent(3))
//	yaml.Dump(&data, opts)
func Options(opts ...Option) Option {
	return func(o *options) error {
		for _, opt := range opts {
			if err := opt(o); err != nil {
				return err
			}
		}
		return nil
	}
}

// OptsYAML parses a YAML string containing option settings and returns
// an Option that can be combined with other options using Options().
//
// The YAML string can specify any of these fields:
//   - indent (int)
//   - compact-seq-indent (bool)
//   - line-width (int)
//   - unicode (bool)
//   - canonical (bool)
//   - line-break (string: ln, cr, crln)
//   - explicit-start (bool)
//   - explicit-end (bool)
//   - flow-simple-coll (bool)
//   - known-fields (bool)
//   - single-document (bool)
//   - unique-keys (bool)
//
// Only fields specified in the YAML will override other options when
// combined. Unspecified fields won't affect other options.
//
// Example:
//
//	opts, err := yaml.OptsYAML(`
//	  indent: 3
//	  known-fields: true
//	`)
//	yaml.Dump(&data, yaml.Options(V4, opts))
func OptsYAML(yamlStr string) (Option, error) {
	var cfg struct {
		Indent                *int    `yaml:"indent"`
		CompactSeqIndent      *bool   `yaml:"compact-seq-indent"`
		LineWidth             *int    `yaml:"line-width"`
		Unicode               *bool   `yaml:"unicode"`
		Canonical             *bool   `yaml:"canonical"`
		LineBreak             *string `yaml:"line-break"`
		ExplicitStart         *bool   `yaml:"explicit-start"`
		ExplicitEnd           *bool   `yaml:"explicit-end"`
		FlowSimpleCollections *bool   `yaml:"flow-simple-coll"`
		KnownFields           *bool   `yaml:"known-fields"`
		SingleDocument        *bool   `yaml:"single-document"`
		UniqueKeys            *bool   `yaml:"unique-keys"`
	}
	if err := Load([]byte(yamlStr), &cfg, WithKnownFields()); err != nil {
		return nil, err
	}

	// Build options only for fields that were set
	var optList []Option
	if cfg.Indent != nil {
		optList = append(optList, WithIndent(*cfg.Indent))
	}
	if cfg.CompactSeqIndent != nil {
		optList = append(optList, WithCompactSeqIndent(*cfg.CompactSeqIndent))
	}
	if cfg.LineWidth != nil {
		optList = append(optList, WithLineWidth(*cfg.LineWidth))
	}
	if cfg.Unicode != nil {
		optList = append(optList, WithUnicode(*cfg.Unicode))
	}
	if cfg.ExplicitStart != nil {
		optList = append(optList, WithExplicitStart(*cfg.ExplicitStart))
	}
	if cfg.ExplicitEnd != nil {
		optList = append(optList, WithExplicitEnd(*cfg.ExplicitEnd))
	}
	if cfg.FlowSimpleCollections != nil {
		optList = append(optList, WithFlowSimpleCollections(*cfg.FlowSimpleCollections))
	}
	if cfg.KnownFields != nil {
		optList = append(optList, WithKnownFields(*cfg.KnownFields))
	}
	if cfg.SingleDocument != nil && *cfg.SingleDocument {
		optList = append(optList, WithSingleDocument())
	}
	if cfg.UniqueKeys != nil {
		optList = append(optList, WithUniqueKeys(*cfg.UniqueKeys))
	}
	if cfg.Canonical != nil {
		optList = append(optList, WithCanonical(*cfg.Canonical))
	}
	if cfg.LineBreak != nil {
		switch *cfg.LineBreak {
		case "ln":
			optList = append(optList, WithLineBreak(LineBreakLN))
		case "cr":
			optList = append(optList, WithLineBreak(LineBreakCR))
		case "crln":
			optList = append(optList, WithLineBreak(LineBreakCRLN))
		default:
			return nil, errors.New("yaml: invalid line-break value (use ln, cr, or crln)")
		}
	}

	return Options(optList...), nil
}

// V2 provides go-yaml v2 formatting defaults:
//   - 2-space indentation
//   - Non-compact sequence indentation
//   - 80-character line width
//   - Unicode enabled
//   - Unique keys enforced
//
// Usage:
//
//	yaml.Dump(&data, yaml.V2)
//	yaml.Dump(&data, yaml.V2, yaml.WithIndent(4))
var V2 = Options(
	WithIndent(2),
	WithCompactSeqIndent(false),
	WithLineWidth(80),
	WithUnicode(true),
	WithUniqueKeys(true),
)

// V3 provides go-yaml v3 formatting defaults:
//   - 4-space indentation (classic go-yaml v3 style)
//   - Non-compact sequence indentation
//   - 80-character line width
//   - Unicode enabled
//   - Unique keys enforced
//
// Usage:
//
//	yaml.Dump(&data, yaml.V3)
//	yaml.Dump(&data, yaml.V3, yaml.WithIndent(2))
var V3 = Options(
	WithIndent(4),
	WithCompactSeqIndent(false),
	WithLineWidth(80),
	WithUnicode(true),
	WithUniqueKeys(true),
)

// V4 provides go-yaml v4 formatting defaults:
//   - 2-space indentation (more compact than v3)
//   - Compact sequence indentation
//   - 80-character line width
//   - Unicode enabled
//   - Unique keys enforced
//
// Usage:
//
//	yaml.Dump(&data, yaml.V4)
var V4 = Options(
	WithIndent(2),
	WithCompactSeqIndent(true),
	WithLineWidth(80),
	WithUnicode(true),
	WithUniqueKeys(true),
)

// applyOptions applies the given options to a new options struct.
// Starts with v4 defaults.
func applyOptions(opts ...Option) (*options, error) {
	o := &options{
		canonical: false,
		lineBreak: libyaml.LN_BREAK,

		// v4 defaults
		indent:           2,
		compactSeqIndent: true,
		lineWidth:        80,
		unicode:          true,
		uniqueKeys:       true,
	}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}
