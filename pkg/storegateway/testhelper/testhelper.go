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
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

// CreateBlock writes a block with the given series and numSamples samples each.
// Samples will be in the time range [mint, maxt).
func CreateBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	return createBlock(ctx, dir, series, numSamples, mint, maxt, extLset, resolution, false, hashFunc)
}

func createBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
	resolution int64,
	tombstones bool,
	hashFunc metadata.HashFunc,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = math.MaxInt64
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

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender(ctx)

				for _, lset := range batch {
					_, err := app.Append(0, lset, t, rand.Float64())
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

	files := []metadata.File{}
	if hashFunc != metadata.NoneFunc {
		paths := []string{}
		if err := filepath.Walk(blockDir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			paths = append(paths, path)
			return nil
		}); err != nil {
			return id, errors.Wrapf(err, "walking %s", dir)
		}

		for _, p := range paths {
			pHash, err := metadata.CalculateHash(p, metadata.SHA256Func, log.NewNopLogger())
			if err != nil {
				return id, errors.Wrapf(err, "calculating hash of %s", blockDir+p)
			}
			files = append(files, metadata.File{
				RelPath: strings.TrimPrefix(p, blockDir+"/"),
				Hash:    &pHash,
			})
		}
	}

	if _, err = metadata.InjectThanos(log.NewNopLogger(), blockDir, metadata.Thanos{
		Labels:     extLset.Map(),
		Downsample: metadata.ThanosDownsample{Resolution: resolution},
		Source:     metadata.TestSource,
		Files:      files,
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if !tombstones {
		if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
			return id, errors.Wrap(err, "remove tombstones")
		}
	}

	return id, nil
}
