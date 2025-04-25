// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRecordVersionHeader(t *testing.T) {
	t.Run("no version header is assumed to be v0", func(t *testing.T) {
		rec := &kgo.Record{
			Headers: []kgo.RecordHeader{},
		}
		parsedVersion := ParseRecordVersion(rec)
		require.Equal(t, 0, parsedVersion)
	})

	tests := []struct {
		version int
	}{
		{
			version: 0,
		},
		{
			version: 1,
		},
		{
			version: 255,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version %d", tt.version), func(t *testing.T) {
			rec := &kgo.Record{
				Headers: []kgo.RecordHeader{RecordVersionHeader(tt.version)},
			}

			parsed := ParseRecordVersion(rec)
			require.Equal(t, tt.version, parsed)
		})
	}
}
