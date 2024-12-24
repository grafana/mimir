// SPDX-License-Identifier: AGPL-3.0-only

package lazyquery

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
)

func TestCopyParamsDeepCopy(t *testing.T) {
	original := &storage.SelectHints{
		Start:    1000,
		End:      2000,
		Step:     10,
		Range:    3600,
		Func:     "rate",
		Grouping: []string{"label1", "label2"},
	}

	copied := copyParams(original)

	// First verify the structs themselves are different
	assert.NotSame(t, original, copied)

	// Then check each field is a different pointer
	originalVal := reflect.ValueOf(original).Elem()
	copiedVal := reflect.ValueOf(copied).Elem()
	typ := originalVal.Type()
	for i := 0; i < typ.NumField(); i++ {
		originalField := originalVal.Field(i)
		copiedField := copiedVal.Field(i)

		// Check if values are equal
		assert.Equal(t, originalField.Interface(), copiedField.Interface(), "Field %s has different values", typ.Field(i).Name)

		switch originalField.Kind() {
		// For reference types, ensure they point to different memory
		case reflect.Slice, reflect.Map, reflect.Ptr:
			if !originalField.IsNil() {
				assert.NotEqual(t, originalField.UnsafePointer(), copiedField.UnsafePointer(), "Field %s shares memory between original and copy", typ.Field(i).Name)
			}
		default:
			// Any other types are copied by value, so the assert.Equal above is enough.
		}
	}
}
