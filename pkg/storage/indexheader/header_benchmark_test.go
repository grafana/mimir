// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func BenchmarkIndexHeaderSize(b *testing.B) {
	ctx := context.Background()

	// Use realistic timestamps to avoid TSDB validation errors
	now := time.Now()
	mint := timestamp.FromTime(now)
	maxt := timestamp.FromTime(now.Add(2 * time.Hour))

	for _, nameCount := range []int{20, 50, 100, 200} {
		for _, valueCount := range []int{1, 10, 100, 500, 1000, 5000} {
			b.Run(fmt.Sprintf("Names=%d/Values=%d", nameCount, valueCount), func(b *testing.B) {
				// Create unique directories for each sub-test to avoid concurrent access issues
				bucketDir := b.TempDir()
				bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
				require.NoError(b, err)
				defer func() {
					require.NoError(b, bkt.Close())
				}()
				instrBkt := objstore.WithNoopInstr(bkt)

				nameSymbols := generateSymbols("name", nameCount)
				valueSymbols := generateSymbols("value", valueCount)
				idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, mint, maxt, labels.FromStrings("ext1", "1"))
				require.NoError(b, err)
				_, err = block.Upload(ctx, log.NewNopLogger(), instrBkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
				require.NoError(b, err)

				indexHeaderV2FullPath := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
				require.NoError(b, WriteBinary(ctx, instrBkt, idIndexV2, indexHeaderV2FullPath))

				v2Stat, err := os.Stat(indexHeaderV2FullPath)
				require.NoError(b, err)
				b.ReportMetric(float64(v2Stat.Size()), "index-header-v2-bytes")

				v2Reader, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), instrBkt, bucketDir, idIndexV2, 32, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(b, err)

				labelNames, err := v2Reader.LabelNames(context.Background())
				require.NoError(b, err)
				//require.Equal(b, nameCount, len(labelNames))

				pqBuilder := NewParquetBuilder()
				for _, labelName := range labelNames {
					values, err := v2Reader.LabelValuesOffsets(
						context.Background(),
						labelName, "",
						func(string) bool { return true },
					)
					require.NoError(b, err)
					pqBuilder.Add(labelName, values)
				}

				indexHeaderVPqFullPath := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename+"-pq")
				err = pqBuilder.WriteToParquet(indexHeaderVPqFullPath)
				require.NoError(b, err)
				pqStat, err := os.Stat(indexHeaderVPqFullPath)
				require.NoError(b, err)
				b.ReportMetric(float64(pqStat.Size()), "index-header-pq-bytes")

				ratio := float64(pqStat.Size()) / float64(v2Stat.Size())
				b.ReportMetric(ratio, "pq-to-v2-bytes-ratio")
			})
		}
	}
}
