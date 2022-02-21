// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/shipper"
)

func TestUserTSDBUpdatedCachedShippedBlocks(t *testing.T) {
	t.Run("Missing metadata file", func(t *testing.T) {
		// Create TSDB directory without metadata file
		dpath := t.TempDir()
		db, err := tsdb.Open(dpath, nil, nil, nil, nil)
		require.NoError(t, err)
		u := userTSDB{
			db: db,
		}

		err = u.updateCachedShippedBlocks()
		fpath := filepath.Join(dpath, shipper.MetaFilename)
		require.EqualError(t, err, fmt.Sprintf(`failed to read "%s": open %s: no such file or directory`, fpath, fpath))
	})

	t.Run("Non-JSON metadata file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, shipper.MetaFilename)
		// Make an invalid JSON file
		require.NoError(t, os.WriteFile(fpath, []byte("{"), 0600))
		db, err := tsdb.Open(dpath, nil, nil, nil, nil)
		require.NoError(t, err)
		u := userTSDB{
			db: db,
		}

		err = u.updateCachedShippedBlocks()
		require.EqualError(t, err, fmt.Sprintf(`failed to parse "%s" as JSON: "{": unexpected end of JSON input`, fpath))
	})

	t.Run("Wrongly versioned metadata file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, shipper.MetaFilename)
		require.NoError(t, os.WriteFile(fpath, []byte(`{"version": 2}`), 0600))
		db, err := tsdb.Open(dpath, nil, nil, nil, nil)
		require.NoError(t, err)
		u := userTSDB{
			db: db,
		}

		err = u.updateCachedShippedBlocks()
		require.EqualError(t, err, "unexpected metadata file version 2")
	})
}
