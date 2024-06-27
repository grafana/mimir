package block

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetacache(t *testing.T) {
	dir := "/Users/peter/Grafana/investigation/meta-syncer"

	ent, err := os.ReadDir(dir)
	require.NoError(t, err)

	ts := int64(0)
	for _, ent := range ent {
		m, err := ReadMetaFromDir(filepath.Join(dir, ent.Name()))
		require.NoError(t, err)

		s := MetaBytesSize(m)
		fmt.Println(m.ULID.String(), m.Compaction.Level, len(m.Compaction.Sources), s)
		ts += s
	}
	fmt.Println(ts)
}
