// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/testutil/e2eutil/prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.
package testhelper

import (
	"context"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/util/test"
)

// CreateBlock writes a block with the given series and numSamples samples each.
// Timeseries i%3==0 will contain floats, i%3==1 will contain histograms and i%3==2 will contain float histograms
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
) (id ulid.ULID, err error) {
	if len(series) < 3 {
		return id, errors.Errorf("not enough series to test different value types: floats, histograms, float histograms")
	}

	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = math.MaxInt64
	headOpts.EnableNativeHistograms.Store(true)
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		runutil.CloseWithErrCapture(&err, h, "TSDB Head")
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	var g errgroup.Group
	var timeStepSize = (maxt - mint) / int64(numSamples+1)
	var batchSize = len(series) / runtime.GOMAXPROCS(0)

	// For calculating the next series value type (float, histogram, etc)
	batchOffset := 0
	// prep some histogram test data
	testHistograms := test.GenerateTestHistograms(numSamples)
	testFloatHistograms := test.GenerateTestFloatHistograms(numSamples)

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		batchValueTypeOffset := batchOffset
		batchOffset += batchSize
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender(ctx)

				for j, lset := range batch {
					var err error
					switch (batchValueTypeOffset + j) % 3 {
					case 0:
						_, err = app.Append(0, lset, t, rand.Float64())
					case 1:
						_, err = app.AppendHistogram(0, lset, t, testHistograms[i], nil)
					case 2:
						_, err = app.AppendHistogram(0, lset, t, nil, testFloatHistograms[i])
					}
					if err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}

						return errors.Wrap(err, "add sample")
					}
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				t += timeStepSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, log.NewNopLogger(), []int64{maxt - mint}, nil, nil, true)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	id, err = c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if id.Compare(ulid.ULID{}) == 0 {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}

	blockDir := filepath.Join(dir, id.String())

	if _, err = metadata.InjectThanos(log.NewNopLogger(), blockDir, metadata.Thanos{
		Labels: extLset.Map(),
		Source: metadata.TestSource,
		Files:  []metadata.File{},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
		return id, errors.Wrap(err, "remove tombstones")
	}

	return id, nil
}
