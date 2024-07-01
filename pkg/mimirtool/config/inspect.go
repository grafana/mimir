// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"bytes"
	_ "embed" // need this for oldCortexConfig
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/tools/doc-generator/parse"
)

var (
	ErrParameterNotFound = errors.New("could not find parameter with this path")
)

type InspectedEntryFactory func() *InspectedEntry

// InspectedEntry is the structure that holds a configuration block or a single configuration parameters.
// Blocks contain other InspectedEntries.

type EntryKind string

const (
	KindBlock EntryKind = "block"
	KindField EntryKind = "field"
)

type InspectedEntry struct {
	Kind     EntryKind `json:"kind"`
	Name     string    `json:"name"`
	Required bool      `json:"required"`
	Desc     string    `json:"desc"`

	// In case the Kind is "block"
	BlockEntries       []*InspectedEntry `json:"blockEntries,omitempty"`
	BlockFlagsPrefix   string            `json:"blockFlagsPrefix,omitempty"`
	BlockFlagsPrefixes []string          `json:"blockFlagsPrefixes,omitempty"`

	// In case the Kind is "field"
	FieldValue        Value           `json:"fieldValue,omitempty"`
	FieldDefaultValue Value           `json:"fieldDefaultValue,omitempty"`
	FieldFlag         string          `json:"fieldFlag,omitempty"`
	FieldType         string          `json:"fieldType,omitempty"`
	FieldCategory     string          `json:"fieldCategory,omitempty"`
	FieldElement      *InspectedEntry `json:"fieldElement,omitempty"` // when FieldType is "slice" or "map"
}

func (i *InspectedEntry) GetValue(path string) (Value, error) {
	child, err := i.find(path)
	if err != nil {
		return Value{}, errors.Wrap(err, path)
	}
	return child.FieldValue, nil
}

// MustGetValue does the same as GetVValue, but panics if there's an error.
func (i *InspectedEntry) MustGetValue(path string) Value {
	v, err := i.GetValue(path)
	if err != nil {
		panic(err)
	}
	return v
}

// SetValue sets individual parameters. val can be any value. If an error is returned,
// its errors.Cause will be ErrParameterNotFound.
func (i *InspectedEntry) SetValue(path string, v Value) error {
	child, err := i.find(path)
	if err != nil {
		return err
	}

	child.FieldValue = v
	return nil
}

// String implements flag.Value
func (i *InspectedEntry) String() string {
	if val, ok := i.FieldValue.AsInterface().(flag.Value); ok {
		return val.String()
	}
	return fmt.Sprintf("%v", i.FieldValue.AsInterface())
}

// Set implements flag.Value
func (i *InspectedEntry) Set(s string) (err error) {
	if val, ok := i.FieldValue.AsInterface().(flag.Value); ok {
		// If the value already knows how to be set, then use that
		return val.Set(s)
	} else if i.FieldValue.IsUnset() {
		// Else, maybe the value wasn't initialized and is nil, so the type assertion failed.
		zero := i.zeroValuePtr()
		if zeroValue, ok := zero.AsInterface().(flag.Value); ok {
			i.FieldValue = zero
			return zeroValue.Set(s)
		}
	}

	// Otherwise, it should be a primitive go type (int, string, float64).
	// Decoding it as YAML should be sufficiently reliable.
	var v Value
	switch i.FieldType {
	case "string":
		v = StringValue(s)
	default:
		jsonDecoder := yaml.NewDecoder(bytes.NewBuffer([]byte(s)))
		v, err = i.decodeValue(jsonDecoder)
	}
	i.FieldValue = v
	return
}

// IsBoolFlag is used by flag package to support to setting bool flags without using value.
func (i *InspectedEntry) IsBoolFlag() bool {
	return i.FieldType == "boolean"
}

func (i *InspectedEntry) RegisterFlags(fs *flag.FlagSet, logger log.Logger) {
	if i.Kind == KindBlock {
		for _, e := range i.BlockEntries {
			e.RegisterFlags(fs, logger)
		}
		return
	}
	if i.FieldFlag == "" {
		return
	}
	fs.Var(i, i.FieldFlag, i.Desc)
}

func (i *InspectedEntry) UnmarshalJSON(b []byte) error {
	// use a type alias that doesn't have any methods, so we force json to unmarshal everything else as it normally would
	type plain InspectedEntry
	err := json.Unmarshal(b, (*plain)(i))
	if err != nil {
		return err
	}

	if i.Kind != KindField {
		return nil
	}

	return i.unmarshalJSONValue(b)
}

// unmarshalJSONValue extracts the "fieldValue" JSON field from b and unmarshals it into a typed
// value according to i.FieldType and parse.ReflectType.
//
// For example, it takes `{ "fieldValue": 123 }` and sets i.FieldValue to int(123). The default json.Unmarshal
// behaviour would be to unmarshal it into float64(123).
func (i *InspectedEntry) unmarshalJSONValue(b []byte) error {
	jsonValues := &struct {
		Raw        json.RawMessage `json:"fieldValue"`
		RawDefault json.RawMessage `json:"fieldDefaultValue"`
	}{}
	err := json.Unmarshal(b, jsonValues)
	if err != nil {
		return err
	}

	decodeIfPresent := func(b []byte) (Value, error) {
		if len(b) == 0 || bytes.Equal(b, []byte("null")) {
			return Value{}, nil
		}
		return i.decodeValue(json.NewDecoder(bytes.NewBuffer(b)))
	}

	var v Value
	v, err = decodeIfPresent(jsonValues.Raw)
	if err != nil {
		return err
	}
	i.FieldValue = v

	v, err = decodeIfPresent(jsonValues.RawDefault)
	if err != nil {
		return err
	}
	i.FieldDefaultValue = v
	return nil
}

func (i *InspectedEntry) MarshalYAML() (interface{}, error) {
	return i.asMap(), nil
}

func (i *InspectedEntry) asMap() map[string]interface{} {
	combined := make(map[string]interface{}, len(i.BlockEntries))
	for _, e := range i.BlockEntries {
		if e.Kind == KindField {
			if val := e.FieldValue; !val.IsUnset() {
				if val.IsSlice() {
					combined[e.Name] = val.AsSlice()
				} else {
					combined[e.Name] = val.AsInterface()
				}
			}
		} else if e.Name != notInYaml {
			if subMap := e.asMap(); len(subMap) > 0 {
				combined[e.Name] = subMap
			}
		}
	}
	return combined
}

func (i *InspectedEntry) UnmarshalYAML(value *yaml.Node) error {
	if i.FieldType == "slice" {
		decodedSlice, err := i.decodeSlice(value)
		if err != nil {
			return errors.Wrapf(err, "could not unmarshal %s", i.Name)
		}
		i.FieldValue = decodedSlice
		return nil
	}

	if i.Kind == KindField {
		decodedValue, err := i.decodeValue(value)
		if err != nil {
			return err
		}
		i.FieldValue = decodedValue
		return nil
	}

	for idx := 0; idx < len(value.Content); idx += 2 {
		yamlField := value.Content[idx].Value
		subNode := value.Content[idx+1]
		for _, entry := range i.BlockEntries {
			if yamlField == entry.Name {
				err := entry.UnmarshalYAML(subNode)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (i *InspectedEntry) zeroValuePtr() Value {
	var typ reflect.Type
	switch i.FieldType {
	case "duration":
		d := duration(0)
		typ = reflect.TypeOf(&d)
	case "list of strings":
		typ = reflect.TypeOf(stringSlice{})
	default:
		typ = parse.ReflectType(i.FieldType)
	}

	return InterfaceValue(reflect.New(typ).Interface()) // create a new typed pointer
}

type decoder interface {
	Decode(interface{}) error
}

func (i *InspectedEntry) decodeValue(decoder decoder) (Value, error) {
	decoded := i.zeroValuePtr()

	err := decoder.Decode(decoded.AsInterface())
	if err != nil {
		return Value{}, err
	}

	switch i.FieldType {
	case "duration":
		value := decoded.AsInterface().(**duration)
		return DurationValue(time.Duration(**value)), err
	case "list of strings":
		return InterfaceValue(*decoded.AsInterface().(*stringSlice)), nil
	default:
		// return a dereferenced typed value
		return InterfaceValue(reflect.ValueOf(decoded.AsInterface()).Elem().Interface()), nil
	}
}

func (i *InspectedEntry) decodeSlice(value *yaml.Node) (Value, error) {
	slice := make([]*InspectedEntry, len(value.Content))
	for idx := range slice {
		slice[idx] = i.FieldElement.Clone()
		err := value.Content[idx].Decode(slice[idx])
		if err != nil {
			return Value{}, err
		}
	}
	return SliceValue(slice), nil
}

func (i *InspectedEntry) find(path string) (*InspectedEntry, error) {
	if path == "" {
		return i, nil
	}

	nextSegment, restOfPath := cutFirstPathSegment(path)

	if i.Kind != KindBlock {
		// if path was non-empty, then there's more to recurse, but this isn't a block
		return nil, ErrParameterNotFound
	}

	for _, e := range i.BlockEntries {
		if e.Name == nextSegment {
			return e.find(restOfPath)
		}
	}
	return nil, ErrParameterNotFound
}

func cutFirstPathSegment(path string) (string, string) {
	segments := strings.SplitN(path, ".", 2)
	nextSegment := segments[0]
	var restOfPath string
	if len(segments) > 1 {
		restOfPath = segments[1]
	}
	return nextSegment, restOfPath
}

// Delete deletes a leaf parameter or entire subtree from the InspectedEntry.
// Delete also recursively deletes any parent blocks that, because of this delete, now contain no entries.
// If an error is returned, it's errors.Cause will be ErrParameterNotFound.
func (i *InspectedEntry) Delete(path string) error {
	return errors.Wrap(i.delete(path), path)
}

func (i *InspectedEntry) delete(path string) error {
	var (
		nextSegment, restOfPath = cutFirstPathSegment(path)
		next                    *InspectedEntry
		nextIndex               int
	)

	for idx, entry := range i.BlockEntries {
		if entry.Name == nextSegment {
			next = entry
			nextIndex = idx
			break
		}
	}
	if next == nil {
		return ErrParameterNotFound
	}

	if restOfPath == "" {
		i.BlockEntries = append(i.BlockEntries[:nextIndex], i.BlockEntries[nextIndex+1:]...)
	} else {
		err := next.delete(restOfPath)
		if err != nil {
			return err
		}

		if len(next.BlockEntries) == 0 {
			i.BlockEntries = append(i.BlockEntries[:nextIndex], i.BlockEntries[nextIndex+1:]...)
		}
	}

	return nil
}

// Walk visits all leaf parameters of the InspectedEntry in a depth-first manner and calls f. If f returns an error,
// the traversal is not stopped. The error Walk returns are the combined errors that all f invocations returned. If no
// f invocations returned an error, then Walk returns nil.
func (i InspectedEntry) Walk(f func(path string, value Value) error) error {
	errs := multierror.New()
	i.walk("", &errs, f)
	return errs.Err()
}

func (i InspectedEntry) walk(path string, errs *multierror.MultiError, f func(path string, value Value) error) {
	for _, e := range i.BlockEntries {
		fieldPath := e.Name
		if path != "" {
			fieldPath = path + "." + e.Name
		}

		if e.Kind == KindField {
			errs.Add(f(fieldPath, e.FieldValue))
		} else {
			e.walk(fieldPath, errs, f)
		}
	}
}

// GetFlag returns the CLI flag name of the parameter.
func (i InspectedEntry) GetFlag(path string) (string, error) {
	child, err := i.find(path)
	if err != nil {
		return "", errors.Wrap(err, path)
	}
	return child.FieldFlag, nil
}

func (i InspectedEntry) GetDefaultValue(path string) (Value, error) {
	child, err := i.find(path)
	if err != nil {
		return Value{}, err
	}
	return child.FieldDefaultValue, nil
}

func (i InspectedEntry) MustGetDefaultValue(path string) Value {
	val, err := i.GetDefaultValue(path)
	if err != nil {
		panic(err)
	}
	return val
}

func (i InspectedEntry) SetDefaultValue(path string, val Value) error {
	entry, err := i.find(path)
	if err != nil {
		return errors.Wrap(ErrParameterNotFound, path)
	}

	entry.FieldDefaultValue = val
	return nil
}

func (i *InspectedEntry) Clone() *InspectedEntry {
	if i == nil {
		return nil
	}
	blockEntries := make([]*InspectedEntry, len(i.BlockEntries))
	for idx := range blockEntries {
		blockEntries[idx] = i.BlockEntries[idx].Clone()
	}

	blockFlagsPrefixes := make([]string, len(i.BlockFlagsPrefixes))
	for idx := range blockFlagsPrefixes {
		blockFlagsPrefixes[idx] = i.BlockFlagsPrefixes[idx]
	}

	return &InspectedEntry{
		Kind:               i.Kind,
		Name:               i.Name,
		Required:           i.Required,
		Desc:               i.Desc,
		BlockEntries:       blockEntries,
		BlockFlagsPrefix:   i.BlockFlagsPrefix,
		BlockFlagsPrefixes: blockFlagsPrefixes,
		FieldValue:         i.FieldValue,
		FieldDefaultValue:  i.FieldDefaultValue,
		FieldFlag:          i.FieldFlag,
		FieldType:          i.FieldType,
		FieldCategory:      i.FieldCategory,
		FieldElement:       i.FieldElement.Clone(),
	}
}

// Describe returns a JSON-serialized result of InspectConfig
func Describe(val flagext.RegistererWithLogger) ([]byte, error) {
	parsedCfg, err := InspectConfig(val)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(parsedCfg, "", "  ")
}

// InspectConfig returns an InspectedEntry that represents the root block of the configuration.
func InspectConfig(cfg flagext.RegistererWithLogger) (*InspectedEntry, error) {
	return InspectConfigWithFlags(cfg, parse.Flags(cfg, log.NewNopLogger()))
}

// InspectConfigWithFlags does the same as InspectConfig while allowing to provide custom CLI flags. This is
// useful when the configuration struct does not implement flagext.RegistererWithLogger.
func InspectConfigWithFlags(cfg interface{}, flags map[uintptr]*flag.Flag) (*InspectedEntry, error) {
	blocks, err := parse.Config(cfg, flags, parse.RootBlocks)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't generate parsed config")
	}

	// we use blocks[0] because parse.Config returns the root block as the first entry and every direct child of
	// the root block as the rest of the entries in blocks
	return convertBlockToEntry(blocks[0]), nil
}

func convertEntriesToEntries(blocks []*parse.ConfigEntry) []*InspectedEntry {
	entries := make([]*InspectedEntry, len(blocks))
	for idx := range entries {
		entries[idx] = convertEntryToEntry(blocks[idx])
	}
	return entries
}

func convertEntryToEntry(entry *parse.ConfigEntry) *InspectedEntry {
	e := &InspectedEntry{
		Name:     entry.Name,
		Required: entry.Required,
		Desc:     entry.FieldDesc,
	}

	switch entry.Kind {
	case parse.KindSlice:
		e.Kind = KindField
		e.FieldType = "slice"
		element := convertBlockToEntry(entry.Element)
		e.FieldElement = element
	case parse.KindBlock:
		e.Kind = KindBlock
		e.BlockEntries = convertEntriesToEntries(entry.Block.Entries)
		e.BlockFlagsPrefix = entry.Block.FlagsPrefix
		e.BlockFlagsPrefixes = entry.Block.FlagsPrefixes
	case parse.KindField:
		e.Kind = KindField
		e.FieldFlag = entry.FieldFlag
		e.FieldType = entry.FieldType
		e.FieldCategory = entry.FieldCategory
		e.FieldDefaultValue = parseDefaultValue(e, entry.FieldDefault)
	default:
		panic(fmt.Sprintf("cannot handle parse kind %q, entry name: %s", entry.Kind, entry.Name))
	}
	return e
}

func parseDefaultValue(e *InspectedEntry, def string) Value {
	yamlNodeKind := yaml.ScalarNode
	if strings.HasPrefix(e.FieldType, "map") {
		yamlNodeKind = yaml.MappingNode
	} else if strings.HasPrefix(e.FieldType, "list") {
		yamlNodeKind = yaml.SequenceNode
	}
	value, _ := e.decodeValue(&yaml.Node{Kind: yamlNodeKind, Value: def})
	return value
}

func convertBlockToEntry(block *parse.ConfigBlock) *InspectedEntry {
	return &InspectedEntry{
		Kind:               KindBlock,
		Name:               block.Name,
		BlockEntries:       convertEntriesToEntries(block.Entries),
		BlockFlagsPrefix:   block.FlagsPrefix,
		BlockFlagsPrefixes: block.FlagsPrefixes,
	}
}

// duration type allows parsing of time.Duration from multiple formats.
type duration time.Duration

func (d *duration) UnmarshalYAML(value *yaml.Node) error {
	td := time.Duration(0)
	err := value.Decode(&td)
	if err == nil {
		*d = duration(td)
		return nil
	}

	md := model.Duration(0)
	err = value.Decode(&md)
	if err == nil {
		*d = duration(md)
		return nil
	}

	nanos := int64(0)
	err = value.Decode(&nanos)
	if err == nil {
		*d = duration(nanos)
		return nil
	}

	return fmt.Errorf("failed to decode duration: %q", value.Value)
}

func (d *duration) UnmarshalJSON(data []byte) error {
	if bytes.HasPrefix(data, []byte("\"")) {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}

		val1, err := time.ParseDuration(s)
		if err == nil {
			*d = duration(val1)
			return nil
		}

		val2, err := model.ParseDuration(s)
		if err == nil {
			*d = duration(val2)
			return nil
		}
		return err
	}

	// if it doesn't look like string, decode it as number.
	val := int64(0)
	err := json.Unmarshal(data, &val)
	if err == nil {
		*d = duration(val)
		return nil
	}

	return fmt.Errorf("failed to decode duration: %q", data)
}

// stringSlice combines the behaviour of flagext.StringSlice and flagext.StringSliceCSV.
// Its fmt.Stringer implementation returns a comma-joined string - this is handy for
// outputting the slice as the value of a flag during extractFlags.
// Its yaml.Unmarshaler implementation supports reading in both YAML sequences and comma-delimited strings.
// Its yaml.Marshaler implementation marshals the slice as a regular go slice.
type stringSlice []string

func (s stringSlice) String() string {
	return strings.Join(s, ",")
}

func (s stringSlice) MarshalYAML() (interface{}, error) {
	return []string(s), nil
}

func (s *stringSlice) UnmarshalYAML(value *yaml.Node) error {
	err := value.Decode((*[]string)(s))
	if err != nil {
		err = value.Decode((*flagext.StringSliceCSV)(s))
		if err != nil {
			return err
		}
	}
	return nil
}

const notInYaml = "not-in-yaml"

//go:embed descriptors/mimir-v2.6.0-flags-only.json
var mimirConfigFlagsOnly []byte

//go:embed descriptors/mimir-v2.6.0.json
var mimirConfig []byte

var mimirConfigDeserialized = loadDefaultMimirConfig()

func loadDefaultMimirConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(mimirConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(mimirConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}

func DefaultMimirConfig() *InspectedEntry {
	return mimirConfigDeserialized.Clone()
}

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

//go:embed descriptors/cortex-v1.11.0-flags-only.json
var oldCortexConfigFlagsOnly []byte

var oldCortexConfigDeserialized = loadDefaultCortexConfig()

func loadDefaultCortexConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}

func DefaultCortexConfig() *InspectedEntry {
	return oldCortexConfigDeserialized.Clone()
}

//go:embed descriptors/gem-v1.7.0.json
var gem170CortexConfig []byte

//go:embed descriptors/gem-v1.7.0-flags-only.json
var gem170CortexConfigFlagsOnly []byte

var gem170CortexConfigDeserialized = loadDefaultGEM170Config()

func loadDefaultGEM170Config() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(gem170CortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(gem170CortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}

func DefaultGEM170Config() *InspectedEntry {
	return gem170CortexConfigDeserialized.Clone()
}

//go:embed descriptors/gem-v2.6.0.json
var gemCortexConfig []byte

//go:embed descriptors/gem-v2.6.0-flags-only.json
var gemCortexConfigFlagsOnly []byte

var gemCortexConfigDeserialized = loadDefaultGEMConfig()

func loadDefaultGEMConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(gemCortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(gemCortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}

func DefaultGEMConfig() *InspectedEntry {
	return gemCortexConfigDeserialized.Clone()
}
