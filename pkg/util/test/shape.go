// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xlab/treeprint"
)

const ignoredFieldName = "<name ignored>"

// We use unsafe casting to convert between some Mimir and Prometheus types
// For this to be safe, the two types need to have the same shape (same fields
// in the same order). This function requires that this property is maintained.
// The fields do not need to have the same names to make the conversion safe,
// but we also check the names are the same here to ensure there's no confusion
// (eg. two bool fields swapped) when ignoreName is false. However, when you
// know the names are different, you can set ignoreName to true.
// The ignoreXXXPrefix flag is used to ignore fields with the XXX_ prefix.
func RequireSameShape(t *testing.T, expectedType, actualType any, ignoreName bool, ignoreXXXPrefix bool) {
	expectedFormatted := prettyPrintType(reflect.TypeOf(expectedType), ignoreName, ignoreXXXPrefix)
	actualFormatted := prettyPrintType(reflect.TypeOf(actualType), ignoreName, ignoreXXXPrefix)

	require.Equal(t, expectedFormatted, actualFormatted)
}

func prettyPrintType(t reflect.Type, ignoreName bool, ignoreXXXPrefix bool) string {
	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("expected %s to be a struct but is %s", t.Name(), t.Kind()))
	}

	tree := treeprint.NewWithRoot("<root>")
	addTypeToTree(t, tree, ignoreName, ignoreXXXPrefix)

	return tree.String()
}

func addTypeToTree(t reflect.Type, tree treeprint.Tree, ignoreName bool, ignoreXXXPrefix bool) {
	if t.Kind() == reflect.Pointer {
		fieldName := t.Name()
		if ignoreXXXPrefix && strings.HasPrefix(fieldName, "XXX_") {
			return
		}
		if ignoreName {
			fieldName = ignoredFieldName
		}
		name := fmt.Sprintf("%s: *%s", fieldName, t.Elem().Kind())
		addTypeToTree(t.Elem(), tree.AddBranch(name), ignoreName, ignoreXXXPrefix)
		return
	}

	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("unexpected kind %s", t.Kind()))
	}

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		fieldName := f.Name
		if ignoreXXXPrefix && strings.HasPrefix(fieldName, "XXX_") {
			return
		}
		if ignoreName {
			fieldName = ignoredFieldName
		}

		switch f.Type.Kind() {
		case reflect.Pointer:
			name := fmt.Sprintf("+%v %s: *%s", f.Offset, fieldName, f.Type.Elem().Kind())
			addTypeToTree(f.Type.Elem(), tree.AddBranch(name), ignoreName, ignoreXXXPrefix)
		case reflect.Slice:
			name := fmt.Sprintf("+%v %s: []%s", f.Offset, fieldName, f.Type.Elem().Kind())

			if isPrimitive(f.Type.Elem().Kind()) {
				tree.AddNode(name)
			} else {
				addTypeToTree(f.Type.Elem(), tree.AddBranch(name), ignoreName, ignoreXXXPrefix)
			}
		default:
			name := fmt.Sprintf("+%v %s: %s", f.Offset, fieldName, f.Type.Kind())
			tree.AddNode(name)
		}
	}
}

func isPrimitive(k reflect.Kind) bool {
	switch k {
	case reflect.Bool,
		reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64,
		reflect.Uintptr,
		reflect.Float32,
		reflect.Float64,
		reflect.Complex64,
		reflect.Complex128,
		reflect.String,
		reflect.UnsafePointer:
		return true
	default:
		return false
	}
}
