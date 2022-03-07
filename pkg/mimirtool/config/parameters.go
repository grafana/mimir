// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"github.com/grafana/dskit/multierror"

	"github.com/grafana/mimir/tools/doc-generator/parse"
)

type Parameters interface {
	Delete(path string) error
	GetDefaultValue(path string) (interface{}, error)
	GetFlag(path string) (string, error)
	GetValue(path string) (interface{}, error)
	MustGetDefaultValue(path string) interface{}
	MustGetValue(path string) interface{}
	SetDefaultValue(path string, val interface{}) error
	SetValue(path string, val interface{}) error
	Walk(f func(path string, value interface{}) error) error
}

type defaultValueInspectedEntry struct {
	*InspectedEntry
}

func (i defaultValueInspectedEntry) GetValue(path string) (interface{}, error) {
	return i.InspectedEntry.GetDefaultValue(path)
}

func (i defaultValueInspectedEntry) MustGetValue(path string) interface{} {
	return i.InspectedEntry.MustGetDefaultValue(path)
}

func (i defaultValueInspectedEntry) SetValue(path string, val interface{}) error {
	return i.InspectedEntry.SetDefaultValue(path, val)
}

func (i defaultValueInspectedEntry) Walk(f func(path string, value interface{}) error) error {
	errs := multierror.New()
	i.walk("", &errs, f)
	return errs.Err()
}

func (i defaultValueInspectedEntry) walk(path string, errs *multierror.MultiError, f func(path string, value interface{}) error) {
	for _, e := range i.BlockEntries {
		fieldPath := e.Name
		if path != "" {
			fieldPath = path + "." + e.Name
		}

		if e.Kind == parse.KindField {
			errs.Add(f(fieldPath, e.FieldDefaultValue))
		} else {
			defaultValueInspectedEntry{e}.walk(fieldPath, errs, f)
		}
	}
}
