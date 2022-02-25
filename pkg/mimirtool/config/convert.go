// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	_ "embed" // need this for oldCortexConfig
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
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

// Convert converts the passed YAML contents in the source schema to a YAML config in the target schema.
// It prunes the default values in the resulting config. sourceFactory and targetFactory are assumed to return
// InspectedEntries where the FieldValue is the default value of the configuration parameter.
// Convert uses sourceFactory and targetFactory to also prune the default values from the resulting config.
// Convert returns the marshalled YAML config in the target schema.
func Convert(contents []byte, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) ([]byte, error) {
	source, target := sourceFactory(), targetFactory()

	err := yaml.Unmarshal(contents, &source)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal old Cortex configuration file")
	}

	err = m.DoMap(source, target)
	if err != nil {
		return nil, err
	}

	sourceDefaults, targetDefaults, err := prepareDefaults(m, sourceFactory, targetFactory)
	if err != nil {
		return nil, err
	}

	pruneDefaults(target, sourceDefaults, targetDefaults)

	return yaml.Marshal(target)
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

func DefaultMimirConfig() *InspectedEntry {
	cfg, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	if err != nil {
		panic(err)
	}
	return cfg
}

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

func DefaultCortexConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	err := json.Unmarshal(oldCortexConfig, cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}
