// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"reflect"
	"time"

	"github.com/grafana/dskit/flagext"
)

func StringValue(s string) Value {
	return Value{val: s}
}

func DurationValue(d time.Duration) Value {
	return Value{val: d}
}

func SliceValue(slice []*InspectedEntry) Value {
	return Value{slice: slice}
}

func InterfaceValue(v interface{}) Value {
	return Value{val: v}
}

func IntValue(i int) Value {
	return Value{val: i}
}

var Nil = Value{}

type Value struct {
	val interface{}

	slice []*InspectedEntry
}

// UnmarshalJSON is empty because unmarshalling without type information is not possible. InspectedEntry.UnmarshalJSON
// does the unmarshalling _with_ type information.
func (v *Value) UnmarshalJSON([]byte) error {
	return nil
}

func (v Value) MarshalJSON() ([]byte, error) {
	if v.slice != nil {
		b, err := json.Marshal(v.slice)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	b, err := json.Marshal(v.val)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (v Value) Equals(other Value) bool {
	return reflect.DeepEqual(v.AsInterface(), other.AsInterface())
}

func (v Value) String() string {
	if val, ok := v.val.(flag.Value); ok {
		return val.String()
	}
	return fmt.Sprintf("%v", v.val)
}

func (v Value) IsUnset() bool {
	return v.val == nil && v.slice == nil
}

func (v Value) IsSlice() bool {
	return v.slice != nil
}

func (v Value) AsString() string {
	if v.val == nil {
		return ""
	}
	return v.val.(string)
}

func (v Value) AsInt() int {
	if v.val == nil {
		return 0
	}
	return v.val.(int)
}

func (v Value) AsURL() flagext.URLValue {
	if v.val == nil {
		return flagext.URLValue{}
	}
	return v.val.(flagext.URLValue)
}

func (v Value) AsFloat() float64 {
	if v.val == nil {
		return 0.0
	}
	return v.val.(float64)
}

func (v Value) AsBool() bool {
	if v.val == nil {
		return false
	}
	return v.val.(bool)
}

func (v Value) AsDuration() time.Duration {
	if v.val == nil {
		return time.Duration(0)
	}
	return v.val.(time.Duration)
}

func (v Value) AsSlice() []*InspectedEntry {
	return v.slice
}

func (v Value) AsInterface() interface{} {
	return v.val
}
