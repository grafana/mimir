// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"github.com/grafana/dskit/multierror"
)

type Parameters interface {
	Delete(path string) error
	GetFlag(path string) (string, error)

	GetValue(path string) (Value, error)
	MustGetValue(path string) Value
	SetValue(path string, v Value) error

	GetDefaultValue(path string) (Value, error)
	MustGetDefaultValue(path string) Value
	SetDefaultValue(path string, v Value) error

	Walk(f func(path string, value Value) error) error
}

type defaultValueInspectedEntry struct {
	*InspectedEntry
}

func (i defaultValueInspectedEntry) Walk(f func(path string, value Value) error) error {
	errs := multierror.New()
	i.walk("", &errs, f)
	return errs.Err()
}

func (i defaultValueInspectedEntry) GetValue(path string) (Value, error) {
	return i.InspectedEntry.GetDefaultValue(path)
}

func (i defaultValueInspectedEntry) MustGetValue(path string) Value {
	return i.InspectedEntry.MustGetDefaultValue(path)
}

func (i defaultValueInspectedEntry) SetValue(path string, v Value) error {
	return i.InspectedEntry.SetDefaultValue(path, v)
}

func (i defaultValueInspectedEntry) walk(path string, errs *multierror.MultiError, f func(path string, value Value) error) {
	for _, e := range i.BlockEntries {
		fieldPath := e.Name
		if path != "" {
			fieldPath = path + "." + e.Name
		}

		if e.Kind == KindField {
			errs.Add(f(fieldPath, e.FieldDefaultValue))
		} else {
			defaultValueInspectedEntry{e}.walk(fieldPath, errs, f)
		}
	}
}
