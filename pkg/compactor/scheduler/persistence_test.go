// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestJobPersistenceManagerFactory(t *testing.T) {
	tests := map[string]struct {
		cfg         Config
		expectError bool
		errorMsg    string
	}{
		"bbolt persistence": {
			cfg: Config{
				PersistenceType: "bbolt",
				Bbolt: BboltConfig{
					Dir:        t.TempDir(),
					ShardCount: 4,
				},
			},
			expectError: false,
		},
		"nop persistence": {
			cfg: Config{
				PersistenceType: "none",
			},
			expectError: false,
		},
		"unrecognized persistence type": {
			cfg: Config{
				PersistenceType: "invalid",
			},
			expectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			mgr, err := jobPersistenceManagerFactory(tc.cfg, log.NewNopLogger())

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, mgr)
			err = mgr.Close()
			require.NoError(t, err)
		})
	}
}
