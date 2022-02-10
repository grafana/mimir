// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/util/fieldcategory"
)

// usage prints command-line usage, the printAll argument controls whether also non-basic flags will be included.
func usage(cfg *mimir.Config, printAll bool) error {
	fields := map[uintptr]reflect.StructField{}
	if err := parseConfig(cfg, fields); err != nil {
		return err
	}

	fs := flag.CommandLine
	fmt.Fprintf(fs.Output(), "Usage of %s:\n", os.Args[0])
	fs.VisitAll(func(fl *flag.Flag) {
		v := reflect.ValueOf(fl.Value)
		fieldCat := fieldcategory.Basic

		if override, ok := fieldcategory.GetOverride(fl.Name); ok {
			fieldCat = override
		} else if v.Kind() == reflect.Ptr {
			ptr := v.Pointer()
			field, ok := fields[ptr]
			if ok {
				catStr := field.Tag.Get("category")
				switch catStr {
				case "advanced":
					fieldCat = fieldcategory.Advanced
				case "experimental":
					fieldCat = fieldcategory.Experimental
				}
			}
		}

		if fieldCat != fieldcategory.Basic && !printAll {
			// Don't print help for this flag since we're supposed to print only basic flags
			return
		}

		var b strings.Builder
		// Two spaces before -; see next two comments.
		fmt.Fprintf(&b, "  -%s", fl.Name)
		name := getFlagName(fl)
		if len(name) > 0 {
			b.WriteString(" ")
			b.WriteString(name)
		}
		// Four spaces before the tab triggers good alignment
		// for both 4- and 8-space tab stops.
		b.WriteString("\n    \t")
		if fieldCat == fieldcategory.Experimental {
			b.WriteString("[experimental] ")
		}
		b.WriteString(strings.ReplaceAll(fl.Usage, "\n", "\n    \t"))

		if !isZeroValue(fl, fl.DefValue) {
			v := reflect.ValueOf(fl.Value)
			if v.Kind() == reflect.Ptr {
				v = v.Elem()
			}
			if v.Kind() == reflect.String {
				// put quotes on the value
				fmt.Fprintf(&b, " (default %q)", fl.DefValue)
			} else {
				fmt.Fprintf(&b, " (default %v)", fl.DefValue)
			}
		}
		fmt.Fprint(fs.Output(), b.String(), "\n")
	})

	if !printAll {
		fmt.Fprintf(fs.Output(), "\nTo see all flags, use -help-all\n")
	}

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

func getFlagName(fl *flag.Flag) string {
	if getter, ok := fl.Value.(flag.Getter); ok {
		v := reflect.ValueOf(getter.Get())
		t := v.Type()
		switch t.Name() {
		case "bool":
			return ""
		case "Duration":
			return "duration"
		case "float64":
			return "float"
		case "int", "int64":
			return "int"
		case "string":
			return "string"
		case "uint", "uint64":
			return "uint"
		case "Secret":
			return "string"
		default:
			return "value"
		}
	}

	// Check custom types.
	switch reflect.ValueOf(fl.Value).Type().String() {
	case "*flagext.Secret":
		return "string"
	}

	return "value"
}
