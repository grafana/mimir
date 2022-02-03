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

// DefaultConverter converts from cortex-1.11.0 to mimir-2.0.0
var DefaultConverter = Converter{
	CortexToCortex: MultiMapper{
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
	},
	BaseCortexToMimir: BestEffortDirectMapper{},
	CortexToMimir: PathMapper{
		PathMappings: map[string]Mapping{
			"blocks_storage.tsdb.max_exemplars":                               RenameMapping("limits.max_global_exemplars_per_user"),
			"query_range.results_cache.cache.background.writeback_goroutines": RenameMapping("frontend.results_cache.memcached.max_async_concurrency"),
			"query_range.results_cache.cache.background.writeback_buffer":     RenameMapping("frontend.results_cache.memcached.max_async_buffer_size"),
			"query_range.results_cache.cache.memcached.batch_size":            RenameMapping("frontend.results_cache.memcached.max_get_multi_batch_size"),
			"query_range.results_cache.cache.memcached.parallelism":           RenameMapping("frontend.results_cache.memcached.max_get_multi_concurrency"),
			"query_range.results_cache.cache.memcached_client.max_idle_conns": RenameMapping("frontend.results_cache.memcached.max_idle_connections"),
		},
	},
	MimirToMimir: NoopMapper{},
}

// TODO refactor this to have only a slice of converters not separate fields
// Converter converts YAML config from Cortex v1.11.0 to Mimir v2.0.0
type Converter struct {
	CortexToCortex    Mapper
	BaseCortexToMimir Mapper
	CortexToMimir     Mapper
	MimirToMimir      Mapper
}

func (c Converter) Convert(contents []byte) ([]byte, error) {
	cortexCfg, mimirParams := defaultCortexParams(), defaultMimirParams()

	err := yaml.Unmarshal(contents, &cortexCfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not unmarshal old Cortex configuration file")
	}

	err = c.CortexToCortex.DoMap(cortexCfg, cortexCfg)
	if err != nil {
		return nil, err
	}

	err = c.BaseCortexToMimir.DoMap(cortexCfg, mimirParams)
	if err != nil {
		return nil, err
	}

	err = c.CortexToMimir.DoMap(cortexCfg, mimirParams)
	if err != nil {
		return nil, err
	}

	err = c.MimirToMimir.DoMap(mimirParams, mimirParams)
	if err != nil {
		return nil, err
	}

	cortexDefaults, mimirDefaults, err := c.prepareDefaults()
	if err != nil {
		return nil, err
	}

	pruneDefaults(mimirParams, cortexDefaults, mimirDefaults)

	return yaml.Marshal(mimirParams)
}

// prepareDefaults maps cortex defaults to mimir defaults the same way regular cortex config is mapped to mimir config.
// This enables lookups of cortex default values using their mimir equivalents.
func (c Converter) prepareDefaults() (cortexDefaults, mimirDefaults *InspectedEntry, err error) {
	oldCortexDefaults := defaultCortexParams()
	mimirDefaults, mappedCortexDefaults := defaultMimirParams(), defaultMimirParams()

	err = c.BaseCortexToMimir.DoMap(oldCortexDefaults, mappedCortexDefaults)
	if err != nil {
		return
	}
	err = c.CortexToMimir.DoMap(oldCortexDefaults, mappedCortexDefaults)
	return mappedCortexDefaults, mimirDefaults, err
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
			if err != ErrParameterNotFound {
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

func defaultMimirParams() *InspectedEntry {
	cfg, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	if err != nil {
		panic(err)
	}
	return cfg
}

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

func defaultCortexParams() *InspectedEntry {
	cfg := &InspectedEntry{}
	err := json.Unmarshal(oldCortexConfig, cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}
