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

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/tools/doc-generator/parse"
)

var (
	ErrParameterNotFound = errors.New("could not find parameter with this path")
)

type InspectedEntryFactory func() *InspectedEntry

// InspectedEntry is the structure that holds a configuration block or a single configuration parameters.
// Blocks contain other other InspectedEntries.
type InspectedEntry struct {
	Kind     parse.EntryKind `json:"kind"`
	Name     string          `json:"name"`
	Required bool            `json:"required"`
	Desc     string          `json:"desc"`

	// In case the Kind is "block"
	BlockEntries       []*InspectedEntry `json:"blockEntries,omitempty"`
	BlockFlagsPrefix   string            `json:"blockFlagsPrefix,omitempty"`
	BlockFlagsPrefixes []string          `json:"blockFlagsPrefixes,omitempty"`

	// In case the Kind is "field"
	FieldValue        interface{} `json:"fieldValue,omitempty"`
	FieldDefaultValue interface{} `json:"fieldDefaultValue,omitempty"`
	FieldFlag         string      `json:"fieldFlag,omitempty"`
	FieldType         string      `json:"fieldType,omitempty"`
	FieldCategory     string      `json:"fieldCategory,omitempty"`
}

// String implements flag.Value
func (i *InspectedEntry) String() string {
	if val, ok := i.FieldValue.(flag.Value); ok {
		return val.String()
	}
	return fmt.Sprintf("%v", i.FieldValue)
}

// Set implements flag.Value
func (i *InspectedEntry) Set(s string) (err error) {
	if val, ok := i.FieldValue.(flag.Value); ok {
		// If the value already know how to be set, then use that
		return val.Set(s)
	}
	// Otherwise, it should be a primitive go type (int, string, float64).
	// Decoding it as YAML should be sufficiently reliable.
	switch i.FieldType {
	case "string":
		i.FieldValue = s
	default:
		jsonDecoder := yaml.NewDecoder(bytes.NewBuffer([]byte(s)))
		i.FieldValue, err = decodeValue(i.FieldType, jsonDecoder)
	}
	return
}

// IsBoolFlag is used by flag package to support to setting bool flags without using value.
func (i *InspectedEntry) IsBoolFlag() bool {
	return i.FieldType == "boolean"
}

func (i *InspectedEntry) RegisterFlags(fs *flag.FlagSet, logger log.Logger) {
	if i.Kind == parse.KindBlock {
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

	if i.Kind != parse.KindField {
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

	decodeIfPresent := func(b []byte) (interface{}, error) {
		if len(b) == 0 {
			return nil, nil
		}
		return decodeValue(i.FieldType, json.NewDecoder(bytes.NewBuffer(b)))
	}

	i.FieldValue, err = decodeIfPresent(jsonValues.Raw)
	if err != nil {
		return err
	}

	i.FieldDefaultValue, err = decodeIfPresent(jsonValues.RawDefault)
	if err != nil {
		return err
	}
	return nil
}

func (i *InspectedEntry) MarshalYAML() (interface{}, error) {
	return i.asMap(), nil
}

func (i *InspectedEntry) asMap() map[string]interface{} {
	combined := make(map[string]interface{}, len(i.BlockEntries))
	for _, e := range i.BlockEntries {
		if e.Kind == parse.KindField {
			if e.FieldValue != nil {
				combined[e.Name] = e.FieldValue
			}
		} else if e.Name != notInYaml {
			combined[e.Name] = e.asMap()
		}
	}
	return combined
}

func (i *InspectedEntry) UnmarshalYAML(value *yaml.Node) error {
	if i.Kind == parse.KindField {
		decodedValue, err := decodeValue(i.FieldType, value)
		if err != nil {
			return err
		}
		i.FieldValue = decodedValue
		return err
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

func decodeValue(fieldType string, decoder interface{ Decode(interface{}) error }) (interface{}, error) {
	typ := parse.ReflectType(fieldType)

	switch fieldType {
	case "duration":
		d := duration(0)
		typ = reflect.TypeOf(&d)
	case "list of string":
		typ = reflect.TypeOf(stringSlice{})
	}

	decoded := reflect.New(typ).Interface() // create a new typed pointer
	err := decoder.Decode(decoded)
	if err != nil {
		return nil, err
	}

	switch fieldType {
	case "duration":
		// convert it to time.Duration.
		value := decoded.(**duration)
		return time.Duration(**value), err
	case "list of string":
		return *decoded.(*stringSlice), nil
	default:
		// return a dereferenced typed value
		return reflect.ValueOf(decoded).Elem().Interface(), nil
	}
}

// GetValue returns the golang value of the parameter as an interface{}.
// The value will be returned so that type assertions on the value work.
// For example, for a duration parameter writing
// 	val, _ := inspectedEntry.GetValue("path"); duration := val.(time.Duration)
// will not panic
func (i InspectedEntry) GetValue(path string) (interface{}, error) {
	entry, err := i.find(path)
	if err != nil {
		return nil, errors.Wrap(err, path)
	}
	if entry.Kind != parse.KindField {
		return nil, errors.Wrap(ErrParameterNotFound, path)
	}
	return entry.FieldValue, nil
}

// MustGetValue does the same as GetValue, but panics if there's an error.
func (i InspectedEntry) MustGetValue(path string) interface{} {
	val, err := i.GetValue(path)
	if err != nil {
		panic(err)
	}
	return val
}

func (i *InspectedEntry) find(path string) (*InspectedEntry, error) {
	if path == "" {
		return i, nil
	}

	nextSegment, restOfPath := cutFirstPathSegment(path)

	if i.Kind != parse.KindBlock {
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

// SetValue sets individual parameters. val can be any value. If an error is returned,
// its errors.Cause will be ErrParameterNotFound.
func (i *InspectedEntry) SetValue(path string, val interface{}) error {
	entry, err := i.find(path)
	if err != nil {
		return errors.Wrap(ErrParameterNotFound, path)
	}

	entry.FieldValue = val
	return nil
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
func (i InspectedEntry) Walk(f func(path string, value interface{}) error) error {
	errs := multierror.New()
	i.walk("", &errs, f)
	return errs.Err()
}

func (i InspectedEntry) walk(path string, errs *multierror.MultiError, f func(path string, value interface{}) error) {
	for _, e := range i.BlockEntries {
		fieldPath := e.Name
		if path != "" {
			fieldPath = path + "." + e.Name
		}

		if e.Kind == parse.KindField {
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

func (i InspectedEntry) GetDefaultValue(path string) (interface{}, error) {
	child, err := i.find(path)
	if err != nil {
		return nil, err
	}
	return child.FieldDefaultValue, nil
}

func (i InspectedEntry) MustGetDefaultValue(path string) interface{} {
	val, err := i.GetDefaultValue(path)
	if err != nil {
		panic(err)
	}
	return val
}

func (i InspectedEntry) SetDefaultValue(path string, val interface{}) error {
	entry, err := i.find(path)
	if err != nil {
		return errors.Wrap(ErrParameterNotFound, path)
	}

	entry.FieldDefaultValue = val
	return nil
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
	blocks, err := parse.Config(nil, cfg, flags)
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
		Kind:     entry.Kind,
		Name:     entry.Name,
		Required: entry.Required,
		Desc:     entry.FieldDesc,
	}

	if e.Kind == parse.KindBlock {
		e.BlockEntries = convertEntriesToEntries(entry.Block.Entries)
		e.BlockFlagsPrefix = entry.Block.FlagsPrefix
		e.BlockFlagsPrefixes = entry.Block.FlagsPrefixes
	} else {
		e.FieldFlag = entry.FieldFlag
		e.FieldType = entry.FieldType
		e.FieldCategory = entry.FieldCategory
		e.FieldValue = nil
		e.FieldDefaultValue = getDefaultValue(entry)
	}
	return e
}

func getDefaultValue(entry *parse.ConfigEntry) interface{} {
	yamlNodeKind := yaml.ScalarNode
	if strings.HasPrefix(entry.FieldType, "map") {
		yamlNodeKind = yaml.MappingNode
	} else if strings.HasPrefix(entry.FieldType, "list") {
		yamlNodeKind = yaml.SequenceNode
	}
	value, _ := decodeValue(entry.FieldType, &yaml.Node{Kind: yamlNodeKind, Value: entry.FieldDefault})
	return value
}

func convertBlockToEntry(block *parse.ConfigBlock) *InspectedEntry {
	return &InspectedEntry{
		Kind:               parse.KindBlock,
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
// outputting the slice as the value of a flag during convertFlags.
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

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

//go:embed descriptors/cortex-v1.11.0-flags-only.json
var oldCortexConfigFlagsOnly []byte

//go:embed descriptors/mimir-v2.0.0-flags-only.json
var mimirConfigFlagsOnly []byte

const notInYaml = "not-in-yaml"

func DefaultMimirConfig() *InspectedEntry {
	cfg, err := InspectConfig(&mimir.Config{})
	if err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(mimirConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         parse.KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}

func DefaultCortexConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         parse.KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}
