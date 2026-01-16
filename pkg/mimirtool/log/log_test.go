// SPDX-License-Identifier: AGPL-3.0-only

package log

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name        string
		level       string
		expectError bool
	}{
		{name: "debug level", level: "debug", expectError: false},
		{name: "info level", level: "info", expectError: false},
		{name: "warn level", level: "warn", expectError: false},
		{name: "error level", level: "error", expectError: false},
		{name: "fatal level", level: "fatal", expectError: false},
		{name: "invalid level", level: "invalid", expectError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, err := NewLogger(tc.level)
			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, logger)
			} else {
				require.NoError(t, err)
				require.NotNil(t, logger)
			}
		})
	}
}
