//go:build enable_antithesis_sdk

package internal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync/atomic"

	detailsjson "github.com/antithesishq/antithesis-sdk-go/internal/json"
)

var (
	warnedNonFinite        atomic.Bool
	warnedUnsafeInteger    atomic.Bool
	warnedNonFiniteOperand atomic.Bool
)

// Details serializes optional user details, preserving objects and wrapping other
// JSON values. A failed serialization reports an SDK error without losing the event;
// coerced non-finite floats and integers beyond ±2^53 report SDK warnings.
func Details(value any, context string) json.RawMessage {
	return DetailsWithFields(value, nil, context)
}

// DetailsWithFields merges generated guidance fields into the details in the
// same traversal, without visiting the entries of ordinary maps and structs
// that they shadow. Custom marshaler output remains opaque. Non-nil fields
// always yield an object, even for nil details.
func DetailsWithFields(value any, fields map[string]any, context string) json.RawMessage {
	if value != nil {
		kind := reflect.ValueOf(value)
		switch kind.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
			if kind.IsNil() {
				value = nil
			}
		}
	}
	if value == nil {
		if fields == nil {
			return nil
		}
		// With nothing to merge into, the fields are the object.
		value, fields = fields, nil
	}
	if len(fields) == 0 {
		fields = nil
	}
	data, warnings, err := detailsjson.MarshalDetailsWithFields(value, fields)
	if err != nil {
		serializationFailed(context, err)
		return nil
	}
	if fields == nil {
		// The encoder only normalizes the root while merging fields.
		data = detailsObject(data)
	}
	warnDetails(warnings, context)
	return data
}

// EmitWithDetails writes a packet that embeds caller details as ordinary
// values, so the packet and its details serialize in one pass under the details
// rules: numbers are normalized with the same once-per-process warnings, and a
// failure drops the whole packet with an SDK error. Embedded details are not
// wrapped, so callers must embed objects (or nil).
func EmitWithDetails(packet any, context string) {
	data, warnings, err := detailsjson.MarshalDetails(packet)
	if err != nil {
		serializationFailed(context, err)
		return
	}
	warnDetails(warnings, context)
	handler.output(data)
}

func serializationFailed(context string, err error) {
	diagnose("antithesis_error", 6001, fmt.Sprintf("Go SDK failed to serialize details for \"%s\": %v", context, err), context)
}

// Each warning is claimed once per process, and only for output that was
// written. The loads keep steady-state emissions off the failing swap.
func warnDetails(warnings detailsjson.Warnings, context string) {
	if warnings.NonFinite && !warnedNonFinite.Load() && warnedNonFinite.CompareAndSwap(false, true) {
		diagnose("antithesis_warning", 6002, fmt.Sprintf("Go SDK replaced non-finite numbers in details for \"%s\" with the strings \"NaN\", \"Infinity\" or \"-Infinity\"; later occurrences in this process are not reported", context), context)
	}
	if warnings.UnsafeInteger && !warnedUnsafeInteger.Load() && warnedUnsafeInteger.CompareAndSwap(false, true) {
		diagnose("antithesis_warning", 6003, fmt.Sprintf("Go SDK emitted integers beyond ±2^53 in details for \"%s\"; consumers that parse JSON numbers as doubles will round them; later occurrences in this process are not reported", context), context)
	}
}

// WarnNonFiniteOperand stands in for the numeric guidance packet that a
// non-finite operand suppresses, once per process like the details warnings.
func WarnNonFiniteOperand(id string) {
	if !warnedNonFiniteOperand.Load() && warnedNonFiniteOperand.CompareAndSwap(false, true) {
		diagnose("antithesis_warning", 6004, fmt.Sprintf("Go SDK skipped numeric guidance for \"%s\" because an operand was not finite; later occurrences in this process are not reported", id), id)
	}
}

// The encoder's output is compact, so the first byte decides the shape.
func detailsObject(data []byte) json.RawMessage {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	if data[0] == '{' {
		return data
	}
	wrapped := make([]byte, 0, len(data)+10)
	wrapped = append(wrapped, `{"value":`...)
	wrapped = append(wrapped, data...)
	return append(wrapped, '}')
}

type diagnostic struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Context string `json:"context"`
}

// Details runs while the caller is still building its packet, so a diagnostic
// always precedes the assertion or event it describes.
func diagnose(key string, code int, message, context string) {
	Json_data(map[string]diagnostic{key: {code, message, context}})
}
