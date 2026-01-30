// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"flag"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"go.yaml.in/yaml/v3"
)

type ConversionNotices struct {
	RemovedCLIFlags        []string
	RemovedParameters      []string
	ChangedDefaults        []ChangedDefault
	SkippedChangedDefaults []ChangedDefault
	PrunedDefaults         []PrunedDefault
}

type ChangedDefault struct {
	Path                   string
	OldDefault, NewDefault string
}

type changedDefault struct {
	path                   string
	oldDefault, newDefault Value
}

func (d changedDefault) asExported() ChangedDefault {
	return ChangedDefault{
		Path:       d.path,
		OldDefault: d.oldDefault.String(),
		NewDefault: d.newDefault.String(),
	}
}

type PrunedDefault struct {
	Path  string
	Value string
}

// Convert converts the passed YAML contents and CLI flags in the source schema to a YAML config and CLI flags
// in the target schema. sourceFactory and targetFactory are assumed to return
// InspectedEntries where the FieldValue is the default value of the configuration parameter.
// Convert uses sourceFactory and targetFactory to also prune the default values from the resulting config.
// Convert returns the marshalled YAML config in the target schema.
func Convert(
	contents []byte,
	flags []string,
	m Mapper,
	sourceFactory, targetFactory InspectedEntryFactory,
	useNewDefaults, showDefaults bool,
) (convertedContents []byte, convertedFlags []string, _ ConversionNotices, conversionErr error) {

	var (
		notices = &ConversionNotices{}
		err     error
	)

	notices.RemovedParameters, notices.RemovedCLIFlags, err = reportDeletedFlags(contents, flags, sourceFactory)
	if err != nil {
		return nil, nil, ConversionNotices{}, err
	}

	source, target := sourceFactory(), targetFactory()

	err = yaml.Unmarshal(contents, &source)
	if err != nil {
		return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not unmarshal old Cortex configuration file")
	}

	err = addFlags(source, flags)
	if err != nil {
		return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not parse provided flags")
	}

	err = m.DoMap(source, target)
	if err != nil {
		return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not map old config to new config")
	}

	sourceDefaults, err := prepareSourceDefaults(m, sourceFactory, targetFactory)
	if err != nil {
		return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not prune defaults in new config")
	}

	changeableDefaults, err := reportChangedDefaults(target, sourceDefaults)
	if err != nil {
		return nil, nil, ConversionNotices{}, err
	}
	notices.ChangedDefaults, notices.SkippedChangedDefaults, err = changeDefaults(changeableDefaults, target, useNewDefaults)
	if err != nil {
		return nil, nil, ConversionNotices{}, err
	}

	if showDefaults {
		err = changeNilsToDefaults(target)
		if err != nil {
			return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not set unset parameters to default values")
		}
	}

	var newFlags []string
	if len(contents) == 0 {
		newFlags, err = extractAllAsFlags(target)
	} else if len(flags) > 0 {
		newFlags, err = extractInputFlags(target, flags, m, sourceFactory, targetFactory)
	}
	if err != nil {
		return nil, nil, ConversionNotices{}, err
	}

	yamlBytes, err := yaml.Marshal(target)
	if err != nil {
		return nil, nil, ConversionNotices{}, errors.Wrap(err, "could not marshal converted config to YAML")
	}

	return yamlBytes, newFlags, *notices, nil
}

func changeDefaults(defaults []changedDefault, target Parameters, useNewDefaults bool) ([]ChangedDefault, []ChangedDefault, error) {
	var changedDefaults, skippedChangedDefault []ChangedDefault
	for _, def := range defaults {
		currentValue := target.MustGetValue(def.path)
		if currentValue.IsUnset() {
			// The value will be implicitly changed to the new default value.
			changedDefaults = append(changedDefaults, def.asExported())
		} else if useNewDefaults && currentValue.Equals(def.oldDefault) {
			err := target.SetValue(def.path, def.newDefault)
			if err != nil {
				return nil, nil, err
			}
			changedDefaults = append(changedDefaults, def.asExported())
		} else {
			skippedChangedDefault = append(skippedChangedDefault, def.asExported())
		}
	}
	return changedDefaults, skippedChangedDefault, nil
}

func reportChangedDefaults(target, sourceDefaults Parameters) ([]changedDefault, error) {
	var defs []changedDefault

	err := target.Walk(func(path string, _ Value) error {
		oldDefault, err := sourceDefaults.GetDefaultValue(path)
		if err != nil {
			if errors.Is(err, ErrParameterNotFound) {
				// This looks like a new parameter because it doesn't exist in the old schema; no default to change from
				return nil
			}
			return err
		}
		newDefault := target.MustGetDefaultValue(path)

		if !oldDefault.Equals(newDefault) {
			defs = append(defs, changedDefault{
				path:       path,
				oldDefault: oldDefault,
				newDefault: newDefault,
			})
		}
		return nil
	})
	return defs, err
}

func changeNilsToDefaults(target *InspectedEntry) error {
	return target.Walk(func(path string, value Value) error {
		if !value.IsUnset() {
			return nil // If the value is already set, don't change it.
		}
		return target.SetValue(path, target.MustGetDefaultValue(path))
	})
}

func extractAllAsFlags(target *InspectedEntry) ([]string, error) {
	return extractFlags(target, func(_ string, v Value) bool { return !v.IsUnset() })
}

func extractInputFlags(target *InspectedEntry, inputFlags []string, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) ([]string, error) {
	flagsNewPaths, err := mapOldFlagsToNewPaths(inputFlags, m, sourceFactory, targetFactory)
	if err != nil {
		return nil, err
	}

	return extractFlags(target, func(path string, _ Value) bool {
		_, ok := flagsNewPaths[path]
		return ok
	})
}

func extractFlags(target *InspectedEntry, shouldExtract func(path string, v Value) bool) ([]string, error) {
	var flagsWithValues, toDelete []string

	err := target.Walk(func(path string, value Value) error {
		flagName, err := target.GetFlag(path)
		if err != nil {
			return err
		}
		if flagName == "" {
			return nil
		}

		if !shouldExtract(path, value) {
			return nil
		}

		flagsWithValues = append(flagsWithValues, fmt.Sprintf("-%s=%s", flagName, value))
		toDelete = append(toDelete, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, path := range toDelete {
		err = target.Delete(path)
		if err != nil {
			return nil, err
		}
	}
	return flagsWithValues, nil
}

// addFlags parses the flags and add their values to the config
func addFlags(entry *InspectedEntry, flags []string) error {
	fs := flag.NewFlagSet(flag.CommandLine.Name(), flag.ContinueOnError)
	fs.Usage = func() {} // Disable dumping old cortex help text on error
	entry.RegisterFlags(fs, nil)
	return fs.Parse(flags)
}

// mapOldFlagsToNewPaths works by creating a new source and target parameters. It uses the values from the passed flags
// to populate the source config. Then it deletes all other parameters from the source but the ones set by the provided flags.
// It sets all values in the target config to nil. It uses the mapper to convert the source to the target.
// It assumes that any parameter that has a non-nil value in the target config
// must have been set by the flags. It returns the paths in the target of the remaining parameters.
//
// mapOldFlagsToNewPaths does not return values along with the flag names because it does not have the rest of the
// config values from the YAML file, so it's likely that the Mapper didn't have enough information to do a correct mapping.
// Renames and other trivial mappings should not be affected by this.
func mapOldFlagsToNewPaths(flags []string, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) (map[string]struct{}, error) {
	source, target := sourceFactory(), targetFactory()

	err := addFlags(source, flags)
	if err != nil {
		return nil, err
	}
	flagIsSet := parseFlagNames(flags)

	var parametersWithoutProvidedFlags []string
	err = source.Walk(func(path string, _ Value) error {
		flagName, err := source.GetFlag(path)
		if err != nil {
			if !errors.Is(err, ErrParameterNotFound) {
				return err
			}
			return nil
		}

		if !flagIsSet[flagName] {
			parametersWithoutProvidedFlags = append(parametersWithoutProvidedFlags, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// delete any parameters from the source that are not set by the flags
	for _, path := range parametersWithoutProvidedFlags {
		err = source.Delete(path)
		if err != nil {
			return nil, err
		}
	}

	var allTargetParams []string
	err = target.Walk(func(path string, _ Value) error {
		allTargetParams = append(allTargetParams, path)
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}

	for _, path := range allTargetParams {
		err = target.SetValue(path, Nil)
		if err != nil {
			return nil, err
		}
	}

	// swallow the error, because we deleted some parameters from the source, not all mappings will work
	_ = m.DoMap(source, target)

	remainingFlags := map[string]struct{}{}
	err = target.Walk(func(path string, value Value) error {
		if !value.IsUnset() {
			remainingFlags[path] = struct{}{}
		}
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}
	return remainingFlags, nil
}

// returns map where keys are flag names found in flags, and value is "true".
func parseFlagNames(flags []string) map[string]bool {
	names := map[string]bool{}
	for _, f := range flags {
		name := f
		name = strings.TrimPrefix(name, "-")
		name = strings.TrimPrefix(name, "-") // trim the prefix twice in case the flag was passed as --flag instead of -flag
		name = strings.SplitN(name, "=", 2)[0]
		name = strings.SplitN(name, " ", 2)[0]
		names[name] = true
	}

	return names
}

// prepareSourceDefaults maps source defaults to target defaults the same way regular source config is mapped to target config.
// This enables lookups of source default values using their target paths.
func prepareSourceDefaults(m Mapper, sourceFactory, targetFactory InspectedEntryFactory) (Parameters, error) {
	sourceDefaults, mappedSourceDefaults := sourceFactory(), targetFactory()
	err := m.DoMap(defaultValueInspectedEntry{sourceDefaults}, defaultValueInspectedEntry{mappedSourceDefaults})
	return mappedSourceDefaults, err
}

func reportDeletedFlags(contents []byte, flags []string, sourceFactory InspectedEntryFactory) (removedFieldPaths, removedFlags []string, _ error) {
	// Find YAML options that user is using, but are no longer supported.
	{
		s := sourceFactory()

		if err := yaml.Unmarshal(contents, &s); err != nil {
			return nil, nil, errors.Wrap(err, "could not unmarshal Cortex configuration file")
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if !val.IsUnset() {
				removedFieldPaths = append(removedFieldPaths, path)
			}
		}
	}

	// Find CLI flags that user is using, but are no longer supported.
	{
		s := sourceFactory()

		if err := addFlags(s, flags); err != nil {
			return nil, nil, err
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if !val.IsUnset() {
				fl, _ := s.GetFlag(path)
				if fl != "" {
					removedFlags = append(removedFlags, fl)
				}
			}
		}
	}

	// Report any provided CLI flags that cannot be found in YAML, and that are not supported anymore.
	providedFlags := parseFlagNames(flags)
	for _, f := range removedCLIOptions {
		if providedFlags[f] {
			removedFlags = append(removedFlags, f)
		}
	}

	return
}
