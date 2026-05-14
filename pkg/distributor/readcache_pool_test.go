// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseReadcacheAddresses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		input   string
		want    map[string]string
		wantErr bool
	}{
		{
			name:  "empty",
			input: "",
			want:  map[string]string{},
		},
		{
			name:  "single",
			input: "rc-1=host1:9095",
			want:  map[string]string{"rc-1": "host1:9095"},
		},
		{
			name:  "multi with whitespace",
			input: " rc-1=host1:9095 , rc-2=host2:9095 ",
			want:  map[string]string{"rc-1": "host1:9095", "rc-2": "host2:9095"},
		},
		{
			name:    "missing equal",
			input:   "rc-1-host1:9095",
			wantErr: true,
		},
		{
			name:    "empty value",
			input:   "rc-1=",
			wantErr: true,
		},
		{
			name:    "empty key",
			input:   "=host1:9095",
			wantErr: true,
		},
		{
			name:    "duplicate instance id",
			input:   "rc-1=h1,rc-1=h2",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseReadcacheAddresses(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
