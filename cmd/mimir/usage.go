// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/grafana/mimir/pkg/mimir"
)

// category is an enumeration of flag categories.
type category int

const (
	// categoryBasic is the basic flag category.
	categoryBasic category = iota
	// categoryAdvanced is the advanced flag category.
	categoryAdvanced
	// categoryExperimental is the experimental flag category.
	categoryExperimental
)

// usage prints command-line usage given a certain category of flags.
func usage(cfg *mimir.Config, cat category) error {
	fields := map[uintptr]reflect.StructField{}
	if err := parseConfig(cfg, fields); err != nil {
		return err
	}

	fs := flag.CommandLine
	fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
	fs.VisitAll(func(fl *flag.Flag) {
		v := reflect.ValueOf(fl.Value)
		if v.Kind() != reflect.Ptr {
			// Ignore flags with non-pointer values
			return
		}

		ptr := v.Pointer()
		field, ok := fields[ptr]
		if !ok {
			// This flag doesn't correspond to any configuration field
			return
		}

		var fieldCat category
		catStr := field.Tag.Get("category")
		switch catStr {
		case "advanced":
			fieldCat = categoryAdvanced
		case "experimental":
			fieldCat = categoryExperimental
		default:
			fieldCat = categoryBasic
		}
		if fieldCat != cat {
			// Don't print help for this flag since it's the wrong category
			return
		}

		var b strings.Builder
		fmt.Fprintf(&b, "  -%s", fl.Name) // Two spaces before -; see next two comments.
		name, usage := flag.UnquoteUsage(fl)
		if len(name) > 0 {
			b.WriteString(" ")
			b.WriteString(name)
		}
		// Boolean flags of one ASCII letter are so common we
		// treat them specially, putting their usage on the same line.
		if b.Len() <= 4 { // space, space, '-', 'x'.
			b.WriteString("\t")
		} else {
			// Four spaces before the tab triggers good alignment
			// for both 4- and 8-space tab stops.
			b.WriteString("\n    \t")
		}
		b.WriteString(strings.ReplaceAll(usage, "\n", "\n    \t"))

		if !isZeroValue(fl, fl.DefValue) {
			if _, ok := fl.Value.(*stringValue); ok {
				// put quotes on the value
				fmt.Fprintf(&b, " (default %q)", fl.DefValue)
			} else {
				fmt.Fprintf(&b, " (default %v)", fl.DefValue)
			}
		}
		fmt.Fprint(fs.Output(), b.String(), "\n")
	})

	return nil
}

// isZeroValue determines whether the string represents the zero
// value for a flag.
func isZeroValue(fl *flag.Flag, value string) bool {
	// Build a zero value of the flag's Value type, and see if the
	// result of calling its String method equals the value passed in.
	// This works unless the Value type is itself an interface type.
	typ := reflect.TypeOf(fl.Value)
	var z reflect.Value
	if typ.Kind() == reflect.Ptr {
		z = reflect.New(typ.Elem())
	} else {
		z = reflect.Zero(typ)
	}
	return value == z.Interface().(flag.Value).String()
}

// -- string Value
type stringValue string

func (s *stringValue) Set(val string) error {
	*s = stringValue(val)
	return nil
}

func (s *stringValue) String() string { return string(*s) }

// parseConfig parses a mimir.Config and populates fields.
func parseConfig(cfg interface{}, fields map[uintptr]reflect.StructField) error {
	// The input config is expected to be a pointer to struct.
	if reflect.TypeOf(cfg).Kind() != reflect.Ptr {
		t := reflect.TypeOf(cfg)
		return fmt.Errorf("%s is a %s while a %s is expected", t, t.Kind(), reflect.Ptr)
	}
	v := reflect.ValueOf(cfg).Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("%s is a %s while a %s is expected", v, v.Kind(), reflect.Struct)
	}

	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type.Kind() == reflect.Func {
			continue
		}

		fieldValue := v.FieldByIndex(field.Index)

		// Take address of field value and map it to field
		fields[fieldValue.Addr().Pointer()] = field

		// Recurse if a struct
		if field.Type.Kind() != reflect.Struct {
			continue
		}

		if err := parseConfig(fieldValue.Addr().Interface(), fields); err != nil {
			return err
		}
	}

	return nil
}
