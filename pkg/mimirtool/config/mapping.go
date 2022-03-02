// SPDX-License-Identifier: AGPL-3.0-only

package config

import "github.com/grafana/dskit/multierror"

type Mapper interface {
	DoMap(source, target *InspectedEntry) error
}

type Mapping func(oldPath string, oldVal interface{}) (newPath string, newVal interface{})

// BestEffortDirectMapper implement Mapper and naively maps the values of all parameters form the source to the
// same name parameter in the target. It ignores all errors while setting the values.
type BestEffortDirectMapper struct{}

func (BestEffortDirectMapper) DoMap(source, target *InspectedEntry) error {
	err := source.Walk(func(path string, value interface{}) error {
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
func (m PathMapper) DoMap(source, target *InspectedEntry) error {
	errs := multierror.New()
	for path, mapping := range m.PathMappings {
		oldVal, err := source.GetValue(path)
		if err != nil {
			errs.Add(err)
			continue
		}
		errs.Add(target.SetValue(mapping(path, oldVal)))
	}
	return errs.Err()
}

type NoopMapper struct{}

func (NoopMapper) DoMap(_, _ *InspectedEntry) error {
	return nil
}

type MultiMapper []Mapper

func (m MultiMapper) DoMap(source, target *InspectedEntry) error {
	errs := multierror.New()
	for _, mapper := range m {
		errs.Add(mapper.DoMap(source, target))
	}
	return errs.Err()
}

type MapperFunc func(source, target *InspectedEntry) error

func (m MapperFunc) DoMap(source, target *InspectedEntry) error {
	return m(source, target)
}

func RenameMapping(to string) Mapping {
	return func(oldPath string, oldVal interface{}) (newPath string, newVal interface{}) {
		newPath = to
		newVal = oldVal
		return
	}
}
