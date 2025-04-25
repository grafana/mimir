package parquetconverter

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/storage/parquet/tsdbcodec"
)

type fsUploader struct {
	dir string
}

func (u *fsUploader) Upload(ctx context.Context, p string, r io.Reader) error {
	err := os.MkdirAll(path.Join(u.dir, path.Dir(p)), os.ModePerm)
	if err != nil {
		return err
	}
	f, err := os.Create(path.Join(u.dir, p))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, r)
	return err
}

func TestCreate(t *testing.T) {
	lbls := tsdbcodec.GenerateTestLabelSets([]int{1, 100}, 100)
	series := tsdbcodec.GenerateTestStorageSeriesFromLabelSets(lbls, []int{1, 100}, 0, 100)
	blockDir := t.TempDir()
	id, err := tsdbcodec.CreateTSDBBlock(context.Background(), series, blockDir)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}
	blockPath := blockDir + "/" + id.String()
	u := &fsUploader{dir: t.TempDir()}
	err = TSDBBlockToParquet(context.Background(), id, u, blockPath, log.NewNopLogger())
	if err != nil {
		t.Fatalf("failed to convert block to parquet: %v", err)
	}
}
