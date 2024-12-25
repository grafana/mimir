package csproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	gogojson "github.com/gogo/protobuf/jsonpb"
	gogo "github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/jsonpb"        //nolint: staticcheck // using this deprecated package intentionally as this is a compatibility shim
	protov1 "github.com/golang/protobuf/proto" //nolint: staticcheck // using this deprecated package intentionally as this is a compatibility shim
	"google.golang.org/protobuf/encoding/protojson"
	protov2 "google.golang.org/protobuf/proto"
)

// JSONMarshaler returns an implementation of the json.Marshaler interface that formats msg to JSON
// using the specified options.
func JSONMarshaler(msg interface{}, opts ...JSONOption) json.Marshaler {
	m := jsonMarshaler{
		msg: msg,
	}
	for _, o := range opts {
		o(&m.opts)
	}
	return &m
}

// jsonMarshaler wraps a Protobuf message and satisfies the json.Marshaler interface
type jsonMarshaler struct {
	msg  interface{}
	opts jsonOptions
}

// compile-time interface check
var _ json.Marshaler = (*jsonMarshaler)(nil)

// MarshalJSON satisfies the json.Marshaler interface
//
// If the wrapped message is nil, or a non-nil interface value holding nil, this method returns nil.
// If the message satisfies the json.Marshaler interface we delegate to it directly.  Otherwise,
// this method calls the appropriate underlying runtime (Gogo vs Google V1 vs Google V2) based on
// the message's actual type.
func (m *jsonMarshaler) MarshalJSON() ([]byte, error) {
	value := reflect.ValueOf(m.msg)
	if m.msg == nil || value.Kind() == reflect.Ptr && value.IsNil() {
		return nil, nil
	}

	// call the message's implementation directly, if present
	if jm, ok := m.msg.(json.Marshaler); ok {
		return jm.MarshalJSON()
	}

	var buf bytes.Buffer

	// Google V2 message?
	if msg, isV2 := m.msg.(protov2.Message); isV2 {
		mo := protojson.MarshalOptions{
			Indent:          m.opts.indent,
			UseEnumNumbers:  m.opts.useEnumNumbers,
			EmitUnpopulated: m.opts.emitZeroValues,
		}
		b, err := mo.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("unable to marshal message to JSON: %w", err)
		}
		return b, nil
	}

	// Google V1 message?
	if msg, isV1 := m.msg.(protov1.Message); isV1 {
		jm := jsonpb.Marshaler{
			Indent:       m.opts.indent,
			EnumsAsInts:  m.opts.useEnumNumbers,
			EmitDefaults: m.opts.emitZeroValues,
		}
		if err := jm.Marshal(&buf, msg); err != nil {
			return nil, fmt.Errorf("unable to marshal message to JSON: %w", err)
		}
		return buf.Bytes(), nil
	}

	// Gogo message?
	if msg, isGogo := m.msg.(gogo.Message); isGogo {
		jm := gogojson.Marshaler{
			Indent:       m.opts.indent,
			EnumsAsInts:  m.opts.useEnumNumbers,
			EmitDefaults: m.opts.emitZeroValues,
		}
		if err := jm.Marshal(&buf, msg); err != nil {
			return nil, fmt.Errorf("unable to marshal message to JSON: %w", err)
		}
		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unsupported message type %T", m.msg)
}

// JSONUnmarshaler returns an implementation of the json.Unmarshaler interface that unmarshals a
// JSON data stream into msg using the specified options.
func JSONUnmarshaler(msg interface{}, opts ...JSONOption) json.Unmarshaler {
	m := jsonUnmarshaler{
		msg: msg,
	}
	for _, o := range opts {
		o(&m.opts)
	}
	return &m

}

// jsonUnmarshaler wraps a Protobuf message and satisfies the json.Unmarshaler interface
type jsonUnmarshaler struct {
	msg  interface{}
	opts jsonOptions
}

// UnmarshalJSON satisfies the json.Unmarshaler interface.
//
// If the wrapped message is nil, or a non-nil interface value holding nil, this method returns an error.
// If the message satisfies the json.Marshaler interface we delegate to it directly.  Otherwise,
// this method calls the appropriate underlying runtime (Gogo vs Google V1 vs Google V2) based on
// the message's actual type.
func (m *jsonUnmarshaler) UnmarshalJSON(data []byte) error {
	if m.msg == nil || reflect.ValueOf(m.msg).IsNil() {
		return fmt.Errorf("cannot unmarshal into nil")
	}

	// call the message's implementation directly, if present
	if jm, ok := m.msg.(json.Unmarshaler); ok {
		return jm.UnmarshalJSON(data)
	}

	// Google V2 message?
	if msg, isV2 := m.msg.(protov2.Message); isV2 {
		mo := protojson.UnmarshalOptions{
			AllowPartial:   m.opts.allowPartial,
			DiscardUnknown: m.opts.allowUnknownFields,
		}
		if err := mo.Unmarshal(data, msg); err != nil {
			return fmt.Errorf("unable to unmarshal JSON data: %w", err)
		}
		return nil
	}

	// Google V1 message?
	if msg, isV1 := m.msg.(protov1.Message); isV1 {
		jm := jsonpb.Unmarshaler{
			AllowUnknownFields: m.opts.allowUnknownFields,
		}
		if err := jm.Unmarshal(bytes.NewReader(data), msg); err != nil {
			return fmt.Errorf("unable to unmarshal JSON data: %w", err)
		}
		return nil
	}

	// Gogo message?
	if msg, isGogo := m.msg.(gogo.Message); isGogo {
		jm := gogojson.Unmarshaler{
			AllowUnknownFields: m.opts.allowUnknownFields,
		}
		if err := jm.Unmarshal(bytes.NewBuffer(data), msg); err != nil {
			return fmt.Errorf("unable to unmarshal JSON data: %w", err)
		}
		return nil
	}

	return fmt.Errorf("unsupported message type %T", m.msg)
}

// JSONOption defines a function that sets a specific JSON formatting option
type JSONOption func(*jsonOptions)

// JSONIndent returns a JSONOption that configures the JSON indentation.
//
// Passing an empty string disables indentation.  If not empty, indent must consist of only spaces or
// tab characters.
func JSONIndent(indent string) JSONOption {
	return func(opts *jsonOptions) {
		opts.indent = indent
	}
}

// JSONUseEnumNumbers returns a JSON option that enables or disables outputting integer values rather
// than the enum names for enum fields.
func JSONUseEnumNumbers(useNumbers bool) JSONOption {
	return func(opts *jsonOptions) {
		opts.useEnumNumbers = useNumbers
	}
}

// JSONIncludeZeroValues returns a JSON option that enables or disables including zero-valued fields
// in the JSON output.
func JSONIncludeZeroValues(emitZeroValues bool) JSONOption {
	return func(opts *jsonOptions) {
		opts.emitZeroValues = emitZeroValues
	}
}

// JSONAllowUnknownFields returns a JSON option that configures JSON unmarshaling to skip unknown
// fields rather than return an error
func JSONAllowUnknownFields(allow bool) JSONOption {
	return func(opts *jsonOptions) {
		opts.allowUnknownFields = allow
	}
}

// JSONAllowPartialMessages returns a JSON option that configured JSON unmarshaling to not return an
// error if unmarshaling data results in required fields not being set on the message.
//
// Note: only applies to Google V2 (google.golang.org/protobuf) messages that are using proto2 syntax.
func JSONAllowPartialMessages(allow bool) JSONOption {
	return func(opts *jsonOptions) {
		opts.allowPartial = allow
	}
}

// jsonOptions defines the JSON formatting options
//
// These options are a subset of those available by each of the three supported runtimes.  The supported
// options consist of the things that are provided by all 3 runtimes in the same manner.  If you need
// the full spectrum of the formatting options you will need to use the appropriate runtime.
//
// The zero value results in no indentation, enum values using the enum names, and not including
// zero-valued fields in the output.
type jsonOptions struct {
	// If set, generate multi-line output such that each field is prefixed by indent and terminated
	// by a newline
	indent string
	// If true, enum fields will be output as integers rather than the enum value names
	useEnumNumbers bool
	// If true, include zero-valued fields in the JSON output
	emitZeroValues bool

	// If true, unknown fields will be discarded when unmarshaling rather than unmarshaling returning
	// an error
	allowUnknownFields bool
	// If true, unmarshaled messages with missing required fields will not return an error
	//
	// Note: only applies to Google V2 (google.golang.org/protobuf) messages that are using proto2 syntax.
	allowPartial bool
}
