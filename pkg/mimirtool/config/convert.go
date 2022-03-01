// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	_ "embed" // need this for oldCortexConfig
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/tools/doc-generator/parse"
)

// CortexToMimirMapper maps from cortex-1.11.0 to mimir-2.0.0 configurations
var CortexToMimirMapper = MultiMapper{
	// first try to naively map keys from old config to same keys from new config
	BestEffortDirectMapper{},
	// next map
	MapperFunc(func(source, target *InspectedEntry) error {
		amDiscovery, err := source.GetValue("ruler.enable_alertmanager_discovery")
		if err != nil {
			return errors.Wrap(err, "could not convert ruler.enable_alertmanager_discovery")
		}
		if amDiscovery == nil || !amDiscovery.(bool) {
			return nil
		}

		amURL, err := source.GetValue("ruler.alertmanager_url")
		if err != nil {
			return errors.Wrap(err, "could not get ruler.alertmanager_url")
		}

		amURLs := strings.Split(amURL.(string), ",")
		for i := range amURLs {
			amURLs[i] = "dnssrvnoa+" + amURLs[i]
		}
		return target.SetValue("ruler.alertmanager_url", strings.Join(amURLs, ","))
	}),
	// last apply any special treatment to other parameters
	PathMapper{
		PathMappings: map[string]Mapping{
			"blocks_storage.tsdb.max_exemplars":                               RenameMapping("limits.max_global_exemplars_per_user"),
			"query_range.results_cache.cache.background.writeback_goroutines": RenameMapping("frontend.results_cache.memcached.max_async_concurrency"),
			"query_range.results_cache.cache.background.writeback_buffer":     RenameMapping("frontend.results_cache.memcached.max_async_buffer_size"),
			"query_range.results_cache.cache.memcached.batch_size":            RenameMapping("frontend.results_cache.memcached.max_get_multi_batch_size"),
			"query_range.results_cache.cache.memcached.parallelism":           RenameMapping("frontend.results_cache.memcached.max_get_multi_concurrency"),
			"query_range.results_cache.cache.memcached_client.max_idle_conns": RenameMapping("frontend.results_cache.memcached.max_idle_connections"),
		},
	},
}

type InspectedEntryFactory func() *InspectedEntry

// Convert converts the passed YAML contents and CLI flags in the source schema to a YAML config and CLI flags
// in the target schema. sourceFactory and targetFactory are assumed to return
// InspectedEntries where the FieldValue is the default value of the configuration parameter.
// Convert uses sourceFactory and targetFactory to also prune the default values from the resulting config.
// Convert returns the marshalled YAML config in the target schema.
func Convert(contents []byte, flags []string, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) ([]byte, []string, error) {
	removedFieldPaths, removedFlags, err := reportDeletedFlags(contents, flags, sourceFactory)
	if err != nil {
		return nil, nil, err
	}

	source, target := sourceFactory(), targetFactory()

	err = yaml.Unmarshal(contents, &source)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not unmarshal old Cortex configuration file")
	}

	err = addFlags(source, flags)
	if err != nil {
		return nil, nil, err
	}

	err = m.DoMap(source, target)
	if err != nil {
		return nil, nil, err
	}

	sourceDefaults, targetDefaults, err := prepareDefaults(m, sourceFactory, targetFactory)
	if err != nil {
		return nil, nil, err
	}

	pruneDefaults(target, sourceDefaults, targetDefaults)

	var newFlags []string
	if len(flags) > 0 {
		newFlags, err = convertFlags(flags, m, target, sourceFactory, targetFactory)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "could not convert passed CLI args: "+err.Error())
		}
	}

	yamlBytes, err := yaml.Marshal(target)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not marshal converted config to YAML")
	}

	for _, f := range removedFieldPaths {
		_, _ = fmt.Fprintln(os.Stderr, "field", f, "is no longer supported")
	}
	for _, f := range removedFlags {
		_, _ = fmt.Fprintf(os.Stderr, "flag -%s is no longer supported", f)
	}

	return yamlBytes, newFlags, err
}

func convertFlags(flags []string, m Mapper, target *InspectedEntry, sourceFactory, targetFactory InspectedEntryFactory) ([]string, error) {
	flagsNewPaths, err := mapOldFlagsToNewPaths(flags, m, sourceFactory, targetFactory)
	if err != nil {
		return nil, err
	}
	var newFlagsWithValues []string
	err = target.Walk(func(path string, value interface{}) error {
		if _, ok := flagsNewPaths[path]; !ok {
			return nil
		}
		flagName, err := target.GetFlag(path)
		if err != nil {
			return err
		}

		newFlagsWithValues = append(newFlagsWithValues, fmt.Sprintf("-%s=%v", flagName, value))
		return nil
	})
	if err != nil {
		return nil, err
	}

	// remove the parameters from the YAML, only keep the flags
	for f := range flagsNewPaths {
		err = target.Delete(f)
		if err != nil {
			return nil, err
		}
	}

	return newFlagsWithValues, nil
}

// addFlags parses the flags and add their values to the config
func addFlags(entry *InspectedEntry, flags []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
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
	err = source.Walk(func(path string, value interface{}) error {
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
	err = target.Walk(func(path string, value interface{}) error {
		allTargetParams = append(allTargetParams, path)
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}

	for _, path := range allTargetParams {
		err = target.SetValue(path, nil)
		if err != nil {
			return nil, err
		}
	}

	// swallow the error, because we deleted some parameters from the source, not all mappings will work
	_ = m.DoMap(source, target)

	remainingFlags := map[string]struct{}{}
	err = target.Walk(func(path string, value interface{}) error {
		if value != nil {
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

// prepareDefaults maps source defaults to target defaults the same way regular source config is mapped to target config.
// This enables lookups of cortex default values using their mimir paths.
func prepareDefaults(m Mapper, sourceFactory, targetFactory InspectedEntryFactory) (cortexDefaults, mimirDefaults *InspectedEntry, err error) {
	oldCortexDefaults, mappedCortexDefaults := sourceFactory(), targetFactory()

	err = m.DoMap(oldCortexDefaults, mappedCortexDefaults)
	return mappedCortexDefaults, DefaultMimirConfig(), err
}

// TODO dimitarvdimitrov convert this to a Mapper, ideally splitting default pruning from "default changed" warnings
// pruneDefaults removes the defaults from fullParams and prints to os.Stderr any changed defaults
// which haven't been overwritten. pruneDefaults swallows prints any errors during pruning to os.Stderr
func pruneDefaults(fullParams, oldDefaults, newDefaults *InspectedEntry) {
	var pathsToDelete []string

	err := fullParams.Walk(func(path string, value interface{}) error {
		newDefault, err := newDefaults.GetValue(path)
		if err != nil {
			return errors.Wrap(err, "expecting value "+path+" to have a default")
		}

		oldDefault, err := oldDefaults.GetValue(path)
		if err != nil {
			// We don't expect new fields exist in the old struct.
			if errors.Is(err, ErrParameterNotFound) {
				return err
			}
			oldDefault = nil
		}

		// Use reflect.DeepEqual to easily compare different type aliases that resolve to the same underlying type,
		// fields with interface types, and maps and slices.
		if value == nil || reflect.DeepEqual(value, oldDefault) || reflect.DeepEqual(value, newDefault) {
			if !reflect.DeepEqual(oldDefault, newDefault) {
				_, _ = fmt.Fprintf(os.Stderr, "using a new default for %s: %v (used to be %v)\n", path, newDefault, oldDefault)
			}

			pathsToDelete = append(pathsToDelete, path)
		}
		return nil
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	for _, p := range pathsToDelete {
		err = fullParams.Delete(p)
		if err != nil {
			err = errors.Wrap(err, "cloud not delete parameter with default value from config")
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}
}

func reportDeletedFlags(contents []byte, flags []string, sourceFactory InspectedEntryFactory) (removedFieldPaths, removedFlags []string, _ error) {
	cortexConfigWithNoValues := func() (*InspectedEntry, error) {
		s := sourceFactory()

		return s, s.Walk(func(path string, value interface{}) error {
			return s.SetValue(path, nil)
		})
	}

	// Find YAML options that user is using, but are no longer supported.
	{
		s, err := cortexConfigWithNoValues()
		if err != nil {
			return nil, nil, err
		}

		if err := yaml.Unmarshal(contents, &s); err != nil {
			return nil, nil, errors.Wrap(err, "could not unmarshal Cortex configuration file")
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if val != nil {
				removedFieldPaths = append(removedFieldPaths, path)
			}
		}
	}

	// Find CLI flags that user is using, but are no longer supported.
	{
		s, err := cortexConfigWithNoValues()
		if err != nil {
			return nil, nil, err
		}

		if err := addFlags(s, flags); err != nil {
			return nil, nil, err
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if val != nil {
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

func DefaultMimirConfig() *InspectedEntry {
	cfg, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	if err != nil {
		panic(err)
	}
	return cfg
}

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

//go:embed descriptors/cortex-v1.11.0-flags-only.json
var oldCortexConfigFlagsOnly []byte

const notInYaml = "not-in-yaml"

func DefaultCortexConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         parse.KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}
