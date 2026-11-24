//go:build enable_antithesis_sdk

package json

//go:generate sh generate.sh

import (
	stdjson "encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type Number = stdjson.Number

var numberType = reflect.TypeFor[Number]()

type Warnings struct{ NonFinite, UnsafeInteger bool }

func MarshalDetails(v any) ([]byte, Warnings, error) {
	return MarshalDetailsWithFields(v, nil)
}

// MarshalDetailsWithFields serializes v with the generated fields merged into
// the root object in one traversal: root entries sharing a field's key are
// skipped before their values are visited, and the fields follow the remaining
// entries. A root that is not an object is normalized so the result is one:
// null gives way to the fields alone, an object produced by a marshaler is
// merged, and any other value moves under "value" unless a field of that name
// shadows it. Nil fields serialize v as it is.
func MarshalDetailsWithFields(v any, fields map[string]any) ([]byte, Warnings, error) {
	e := newEncodeState()
	defer func() { e.rootFields = nil; encodeStatePool.Put(e) }()
	data, err := e.marshalDetails(v, fields, encOpts{escapeHTML: true})
	if err != nil {
		return nil, Warnings{}, err
	}
	return data, e.warnings, nil
}

func (e *encodeState) marshalDetails(v any, fields map[string]any, opts encOpts) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			if je, ok := r.(jsonError); ok {
				err = je.error
			} else {
				panic(r)
			}
		}
	}()
	e.rootFields = fields
	e.reflectValue(reflect.ValueOf(v), opts)
	if fields != nil {
		return e.completeRootFields(fields, opts), nil
	}
	return append([]byte(nil), e.Bytes()...), nil
}

// A root map or struct writes the generated fields itself; every other root
// shape is normalized here, after the caller's value has been visited once.
func (e *encodeState) completeRootFields(fields map[string]any, opts encOpts) []byte {
	out := e.Bytes()
	switch {
	case e.rootFields == nil && out[0] == '{':
		return append([]byte(nil), out...)
	case string(out) == "null":
		e.Reset()
	case out[0] == '{':
		// A marshaler's object is opaque to the exclusion hook, so its keys
		// are merged from the validated output rather than from caller data.
		var object map[string]stdjson.RawMessage
		if err := stdjson.Unmarshal(out, &object); err != nil {
			e.error(err)
		}
		e.Reset()
		e.rootFields = fields
		e.reflectValue(reflect.ValueOf(object), opts)
		return append([]byte(nil), e.Bytes()...)
	default:
		if _, shadowed := fields["value"]; shadowed {
			// The shadowed value never reaches the wire, nor do its warnings.
			e.Reset()
			e.warnings = Warnings{}
			break
		}
		e.rootFields = nil
		e.writeRootFields(fields, ',', opts)
		wrapped := make([]byte, 0, len(e.Bytes())+10)
		wrapped = append(wrapped, `{"value":`...)
		wrapped = append(wrapped, e.Bytes()...)
		return append(wrapped, '}')
	}
	e.rootFields = nil
	if e.writeRootFields(fields, '{', opts) == '{' {
		e.WriteString("{}")
	} else {
		e.WriteByte('}')
	}
	return append([]byte(nil), e.Bytes()...)
}

// writeRootFields writes the generated fields in key order, each preceded by
// next and then by ',', and returns the separator the next entry would need.
func (e *encodeState) writeRootFields(fields map[string]any, next byte, opts encOpts) byte {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	opts.quoted = false
	for _, key := range keys {
		e.WriteByte(next)
		next = ','
		e.Write(appendString(e.AvailableBuffer(), key, opts.escapeHTML))
		e.WriteByte(':')
		e.reflectValue(reflect.ValueOf(fields[key]), opts)
	}
	return next
}

func errorEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	e.Write(appendString(e.AvailableBuffer(), fmt.Sprintf("%+v", v.Interface().(error)), opts.escapeHTML))
}
func addrErrorEncoder(e *encodeState, v reflect.Value, opts encOpts) {
	errorEncoder(e, v.Addr(), opts)
}

func unsafeNumberInteger(s string) bool {
	if strings.ContainsAny(s, ".eE") {
		return false
	}
	s = strings.TrimPrefix(s, "-")
	const bound = "9007199254740992"
	return len(s) > len(bound) || len(s) == len(bound) && s >= bound
}
