// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBlockBuilder_LocalFile(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	tenants := []string{"1", "2"}
	samplesPerTenant := 10

	cfg, overrides := blockBuilderConfig(t, "localhost:0", nil)
	cfg.GenerateSparseIndexHeaders = true
	cfg.LocalFile = filepath.Join(t.TempDir(), "dump.jsonl.gz")

	f, err := os.Create(cfg.LocalFile)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	enc := json.NewEncoder(gz)

	producedSamples := make(map[string][]mimirpb.Sample)
	recTime := time.Now().Add(-time.Hour)
	var offset int64
	for range samplesPerTenant {
		for _, tenant := range tenants {
			samples := floatSample(recTime.UnixMilli(), 1)
			req := createWriteRequest(tenant, samples, nil)
			val, err := req.Marshal()
			require.NoError(t, err)

			require.NoError(t, enc.Encode(&kgo.Record{
				Key:       []byte(tenant),
				Value:     val,
				Partition: 1,
				Offset:    offset,
				Timestamp: recTime,
			}))
			offset++
			producedSamples[tenant] = append(producedSamples[tenant], samples...)
		}
		recTime = recTime.Add(10 * time.Minute)
	}
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())

	bb, err := New(cfg, test.NewTestingLogger(t), prometheus.NewPedanticRegistry(), overrides)
	require.NoError(t, err)

	// In local-file mode running() consumes the file and returns, terminating the service.
	require.NoError(t, bb.StartAsync(ctx))
	require.NoError(t, bb.AwaitTerminated(ctx))

	for _, tenant := range tenants {
		tenantBucketDir := path.Join(cfg.BlocksStorage.Bucket.Filesystem.Directory, tenant)
		validateSparseIndexHeadersInDir(t, ctx, tenantBucketDir, cfg)
		compareQueryWithDir(t,
			tenantBucketDir,
			producedSamples[tenant], nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)
	}
}
