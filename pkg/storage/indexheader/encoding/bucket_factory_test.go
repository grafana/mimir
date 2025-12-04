package encoding_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

func TestBucketDecbufFactory_NewDecbufAtUnchecked(t *testing.T) {
	ctx := t.Context()

	testDataDir := filepath.Join("../../../../tsdb-sync", "load-generator-1")
	bkt, err := filesystem.NewBucket(testDataDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	blockID := "01K4YXAQN41Y4W8KS1BBR27N4Y"
	indexPath := filepath.Join(blockID, "index")

	factory := streamencoding.NewBucketDecbufFactory(ctx, bkt, indexPath)
	defer factory.Stop()

	const symbolsOffset = 5
	dec := factory.NewDecbufAtChecked(symbolsOffset, nil)
	t.Cleanup(func() {
		require.NoError(t, dec.Close())
	})

	num := dec.Be32int()
	require.NotZero(t, num)

	for n := 0; dec.Err() == nil && n < num; n++ {
		println(fmt.Sprintf("%d: %d -> %s", n, dec.Position(), dec.UnsafeUvarintBytes()))
	}
	require.NoError(t, dec.Err())

	//syms, err := streamindex.NewSymbols(factory, index.FormatV2, 5, false)
	//require.NoError(t, err)
	//
	//offset, err := syms.ReverseLookup("__name__")
	//require.NoError(t, err)
	//require.NotZero(t, offset)

	//err = syms.ForEachSymbol([]string{"__name__"}, func(sym string, offset uint32) error {
	//	require.NotEmpty(t, sym)
	//	return nil
	//})
	//require.NoError(t, err)
}

func TestBucketBinaryReader(t *testing.T) {
	ctx := t.Context()

	//testDataDir := filepath.Join("../../../../tsdb-sync", "load-generator-1")
	testDataDir := filepath.Join("/Users/v/tmp/mimir-tsdb-data/mimir-prod-39-blocks", "2288501")
	bkt, err := filesystem.NewBucket(testDataDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	//blockID := ulid.MustParse("01K4YXAQN41Y4W8KS1BBR27N4Y")
	blockID := ulid.MustParse("01KBETSVZSAY3EWDJZPE2PZJ4G")

	const postingOffsetsInMemSampling = 32
	cfg := indexheader.Config{}
	r, err := indexheader.NewBucketBinaryReader(
		ctx,
		log.NewNopLogger(),
		objstore.WithNoopInstr(bkt),
		testDataDir,
		blockID,
		postingOffsetsInMemSampling,
		cfg,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, r.Close())
	})

	labels, err := r.LabelNames(ctx)
	require.NoError(t, err)
	for _, lbl := range labels {
		t.Logf("label: %s", lbl)
	}
	_ = r
}
