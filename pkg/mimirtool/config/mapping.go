// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"github.com/grafana/dskit/multierror"
)

type Mapper interface {
	DoMap(source, target Parameters) error
}

type Mapping func(oldPath string, oldVal Value) (newPath string, newVal Value)

// BestEffortDirectMapper implement Mapper and naively maps the values of all parameters form the source to the
// same name parameter in the target. It ignores all errors while setting the values.
type BestEffortDirectMapper struct{}

func (BestEffortDirectMapper) DoMap(source, target Parameters) error {
	err := source.Walk(func(path string, value Value) error {
		_ = target.SetValue(path, value)
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}
	return nil
}

// PathMapper applies the mappings to specific parameters of the source.
type PathMapper struct {
	PathMappings map[string]Mapping
}

// DoMap applies the Mappings from source to target.
// The error DoMap returns are the combined errors that all Mappings returned. If no
// Mappings returned an error, then DoMap returns nil.
func (m PathMapper) DoMap(source, target Parameters) error {
	errs := multierror.New()
	for path, mapping := range m.PathMappings {
		oldVal, err := source.GetValue(path)
		if err != nil {
			errs.Add(err)
			continue
		}
		newPath, newVal := mapping(path, oldVal)
		err = target.SetValue(newPath, newVal)
		if err != nil {
			errs.Add(err)
		}
	}
	return errs.Err()
}

type MultiMapper []Mapper

func (m MultiMapper) DoMap(source, target Parameters) error {
	errs := multierror.New()
	for _, mapper := range m {
		errs.Add(mapper.DoMap(source, target))
	}
	return errs.Err()
}

type MapperFunc func(source, target Parameters) error

func (m MapperFunc) DoMap(source, target Parameters) error {
	return m(source, target)
}

func RenameMapping(to string) Mapping {
	return func(_ string, oldVal Value) (newPath string, newVal Value) {
		newPath = to
		newVal = oldVal
		return
	}
}
