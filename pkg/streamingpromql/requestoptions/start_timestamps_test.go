// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"testing"

	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

func TestParseUseStartTimestamps(t *testing.T) {
	tests := []struct {
		name            string
		header          string
		expectedOption  StartTimestampOption
		expectedPointer *bool
		expectedError   string
	}{
		{name: "absent", expectedOption: StartTimestampsUnset},
		{name: "disabled", header: "false", expectedOption: StartTimestampsDisabled, expectedPointer: new(false)},
		{name: "enabled", header: "true", expectedOption: StartTimestampsEnabled, expectedPointer: new(true)},
		{name: "uppercase enabled", header: "TRUE", expectedOption: StartTimestampsEnabled, expectedPointer: new(true)},
		{name: "numeric enabled", header: "1", expectedOption: StartTimestampsEnabled, expectedPointer: new(true)},
		{name: "numeric disabled", header: "0", expectedOption: StartTimestampsDisabled, expectedPointer: new(false)},
		{name: "invalid", header: "invalid", expectedError: `invalid value "invalid" for X-Prometheus-Use-Start-Timestamps header: must be a boolean`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseUseStartTimestamps(tc.header)
			if tc.expectedError != "" {
				require.EqualError(t, err, tc.expectedError)
				require.Equal(t, apierror.TypeBadData, apierror.TypeForError(err, apierror.TypeNone))
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedPointer, parsed)
			require.Equal(t, tc.expectedOption, StartTimestampOptionFromBool(parsed))
		})
	}
}

func TestStartTimestampOption_Resolve(t *testing.T) {
	tests := []struct {
		name          string
		engineDefault bool
		option        StartTimestampOption
		expected      bool
	}{
		{name: "false default absent", option: StartTimestampsUnset},
		{name: "false default disabled", option: StartTimestampsDisabled},
		{name: "false default enabled", option: StartTimestampsEnabled, expected: true},
		{name: "true default absent", engineDefault: true, option: StartTimestampsUnset, expected: true},
		{name: "true default disabled", engineDefault: true, option: StartTimestampsDisabled},
		{name: "true default enabled", engineDefault: true, option: StartTimestampsEnabled, expected: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.option.Resolve(tc.engineDefault))
		})
	}
}
