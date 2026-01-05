// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/yaml.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import "go.yaml.in/yaml/v4"

// YAMLMarshalUnmarshal utility function that converts a YAML interface in a map
// doing marshal and unmarshal of the parameter
func YAMLMarshalUnmarshal(in interface{}) (map[string]interface{}, error) {
	yamlBytes, err := yaml.Dump(in)
	if err != nil {
		return nil, err
	}

	object := make(map[string]interface{})
	if err := yaml.Load(yamlBytes, object); err != nil {
		return nil, err
	}

	return object, nil
}
