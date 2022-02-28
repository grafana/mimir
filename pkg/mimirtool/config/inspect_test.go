// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
)

func TestInspectedEntry_SetThenGet(t *testing.T) {
	testCases := []struct {
		name       string
		testStruct interface{}
		path       string

		expectedErr   error
		expectedValue interface{}
	}{
		{
			name: "a simple field",
			testStruct: &struct {
				Field string `yaml:"field"`
			}{},
			path:          "field",
			expectedValue: "string",
		},
		{
			name: "a field within a struct",
			testStruct: &struct {
				Str struct {
					Field string `yaml:"field"`
				} `yaml:"struct"`
			}{},
			path:          "struct.field",
			expectedValue: "string",
		},
		{
			name: "fails with a non-existent field",
			testStruct: &struct {
				Field string `yaml:"field"`
			}{},
			path:        "field_2",
			expectedErr: ErrParameterNotFound,
		},
		{
			name: "a field within an inlined struct",
			testStruct: &struct {
				Str struct {
					Field string `yaml:"field"`
				} `yaml:",inline"`
			}{},
			path:          "field",
			expectedValue: "string",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inspectedConfig, err := DefaultValueInspector.InspectConfigWithFlags(tc.testStruct, nil)
			require.NoError(t, err)

			err = inspectedConfig.SetValue(tc.path, tc.expectedValue)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			actualValue, err := inspectedConfig.GetValue(tc.path)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
			} else {
				assert.Equal(t, tc.expectedValue, actualValue)
				assert.NoError(t, err)
			}
		})
	}
}

func TestInspectedEntry_Walk(t *testing.T) {
	testCases := []struct {
		name           string
		testStruct     interface{}
		expectedFields []string
	}{
		{
			name: "no recursion",
			testStruct: &struct {
				FieldA string `yaml:"field_a"`
			}{},
			expectedFields: []string{"field_a"},
		},
		{
			name: "with an inlined field",
			testStruct: &struct {
				FieldA struct {
					FieldB string `yaml:"field_b"`
				} `yaml:",inline"`
			}{},
			expectedFields: []string{"field_b"},
		},
		{
			name: "with a skipped struct",
			testStruct: &struct {
				FieldA struct {
					FieldB string `yaml:"field_b"`
				} `yaml:"-"`
			}{},
			expectedFields: []string{},
		},
		{
			name: "with a skipped field",
			testStruct: &struct {
				FieldA struct {
					FieldB string `yaml:"-"`
				} `yaml:"struct_a"`
			}{},
			expectedFields: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			inspectedConfig, err := DefaultValueInspector.InspectConfigWithFlags(tc.testStruct, nil)
			require.NoError(t, err)

			actualFields := listAllFields(inspectedConfig)

			if len(tc.expectedFields) == 0 {
				assert.Empty(t, actualFields)
			} else {
				assert.Equal(t, tc.expectedFields, actualFields)
			}
		})
	}
}

func TestInspectedEntry_Delete(t *testing.T) {
	testCases := []struct {
		name           string
		params         interface{}
		pathToDelete   string
		expectedErr    error
		expectedParams []string
	}{
		{
			name: "deletes a path",
			params: &struct {
				API struct {
					Prefix  string `yaml:"prefix"`
					Version string `yaml:"version"`
				} `yaml:"api"`
			}{},
			pathToDelete: "api.version",

			expectedParams: []string{"api.prefix"},
		},
		{
			name: "fails to delete a non-existent path",
			params: &struct {
				API struct {
					Version string `yaml:"version"`
				} `yaml:"api"`
			}{},
			pathToDelete: "api.something_different",
			expectedErr:  ErrParameterNotFound,
		},
		{
			name: "cleans up empty fields",
			params: &struct {
				API struct {
					Version string `yaml:"version"`
				} `yaml:"api"`
			}{},
			pathToDelete:   "api.version",
			expectedParams: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inspected, err := ZeroValueInspector.InspectConfigWithFlags(tc.params, nil)
			require.NoError(t, err)

			actualErr := inspected.Delete(tc.pathToDelete)

			if tc.expectedErr != nil {
				assert.ErrorIs(t, actualErr, tc.expectedErr)
			} else {
				assert.ElementsMatch(t, tc.expectedParams, listAllFields(inspected))
				assert.NoError(t, actualErr)
			}
		})
	}
}

func TestInspectedConfig_MarshalThenUnmarshalRetainsTypeInformation(t *testing.T) {
	inspectedConfig, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	require.NoError(t, err)
	require.NoError(t, inspectedConfig.SetValue("distributor.remote_timeout", time.Minute))
	bytes, err := yaml.Marshal(inspectedConfig)
	require.NoError(t, err)

	inspectedConfig, err = DefaultValueInspector.InspectConfig(&mimir.Config{})
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal(bytes, &inspectedConfig))

	val, err := inspectedConfig.GetValue("distributor.remote_timeout")
	require.NoError(t, err)
	assert.Equal(t, time.Minute, val) // if type info was lost this would be "1m" instead of time.Minute
}

func TestInspectedEntry_MarshalYAML(t *testing.T) {
	d, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	require.NoError(t, err)
	require.NoError(t, yaml.Unmarshal([]byte(`
distributor:
  remote_timeout: 10s
`), &d))

	val, err := d.GetValue("distributor.remote_timeout")
	assert.NoError(t, err)
	assert.Equal(t, time.Second*10, val)
}

func TestInspectConfig_HasDefaultValues(t *testing.T) {
	d, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	require.NoError(t, err)
	val, err := d.GetValue("distributor.remote_timeout")
	assert.NoError(t, err)
	assert.Equal(t, time.Second*20, val)
}

func TestInspectConfig_LoadingAConfigHasCorrectTypes(t *testing.T) {
	testCases := []struct {
		name         string
		path         string
		expectedType interface{}
	}{
		{
			name:         "int",
			path:         "distributor.max_recv_msg_size",
			expectedType: int(0),
		},
		{
			name:         "[]string",
			path:         "distributor.ha_tracker.kvstore.etcd.endpoints",
			expectedType: []string{},
		},
		{
			name:         "duration",
			path:         "server.graceful_shutdown_timeout",
			expectedType: time.Duration(0),
		},
		{
			name:         "bool",
			path:         "distributor.ha_tracker.kvstore.etcd.tls_enabled",
			expectedType: false,
		},
		{
			name:         "float",
			path:         "distributor.ha_tracker.kvstore.consul.watch_rate_limit",
			expectedType: float64(0),
		},
		{
			name:         "string",
			path:         "distributor.ha_tracker.kvstore.etcd.tls_cert_path",
			expectedType: "",
		},
		{
			name:         "time",
			path:         "querier.use_second_store_before_time",
			expectedType: &flagext.Time{},
		},
		{
			name:         "url",
			path:         "ruler.external_url",
			expectedType: flagext.URLValue{},
		},
	}

	params := DefaultCortexConfig()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := params.GetValue(tc.path)
			assert.NoError(t, err)
			assert.IsType(t, tc.expectedType, val)
		})
	}
}

func listAllFields(inspectedConfig *InspectedEntry) []string {
	var actualFields []string
	err := inspectedConfig.Walk(func(path string, value interface{}) error {
		actualFields = append(actualFields, path)
		return nil
	})
	if err != nil {
		panic("walkFn didn't return an error, but Walk itself did; don't know what to do: " + err.Error())
	}
	return actualFields
}
