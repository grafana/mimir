// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type BlockSubsetCommand struct {
	subsetBlock   string
	supersetBlock string
}

func (c *BlockSubsetCommand) Register(app *kingpin.Application) {
	cmd := app.Command("block-subset", "TODO")
	cmd.Action(c.checkSubset)

	cmd.Flag("subset-block-path", "TODO").
		Default("TODO").
		StringVar(&c.subsetBlock)

	cmd.Flag("superset-block-path", "TODO").
		Default("TODO").
		StringVar(&c.supersetBlock)
}

func (c *BlockSubsetCommand) checkSubset(_ *kingpin.ParseContext) error {
	sub, err := tsdb.OpenBlock(promslog.NewNopLogger(), c.subsetBlock, nil, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = sub.Close()
	}()

	sup, err := tsdb.OpenBlock(promslog.NewNopLogger(), c.supersetBlock, nil, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = sup.Close()
	}()

	subMeta, supMeta := sub.Meta(), sup.Meta()
	if subMeta.MinTime < supMeta.MinTime || subMeta.MaxTime > supMeta.MaxTime {
		return errors.New("subset block time range is not within the superset block time range")
	}

	// Check number of series
	if subMeta.Stats.NumSeries > supMeta.Stats.NumSeries {
		return errors.New("subset block has more series than the superset block")
	}

	// Check number of samples
	if subMeta.Stats.NumSamples > supMeta.Stats.NumSamples {
		return errors.New("subset block has more samples than the superset block")
	}

	subQ, err := tsdb.NewBlockQuerier(sub, subMeta.MinTime, subMeta.MaxTime)
	if err != nil {
		return err
	}
	defer func() {
		_ = subQ.Close()
	}()
	supQ, err := tsdb.NewBlockQuerier(sup, supMeta.MinTime, supMeta.MaxTime)
	if err != nil {
		return err
	}
	defer func() {
		_ = supQ.Close()
	}()

	// Check if all series in the subset block are in the superset block
	subSeries := subQ.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*"))
	supSeries := supQ.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".*"))

	s1Exists := subSeries.Next()
	s2Exists := supSeries.Next()
	svMismatch := 0
	seriesMis := 0
	var s1It, s2It chunkenc.Iterator
	for {

		if !s1Exists {
			// We are done.
			break
		}

		if !s2Exists {
			// TODO: mention which series
			return errors.New("subset block has series that are not in the superset block")
		}

		s1 := subSeries.At()
		s2 := supSeries.At()

		cmp := labels.Compare(s1.Labels(), s2.Labels())

		if cmp == 0 {
			// Check for samples to be subset
			mismatch := false

			s1It = s1.Iterator(s1It)
			s2It = s2.Iterator(s2It)
			s1Type := s1It.Next()
			s2Type := s2It.Next()
			for {
				if s1Type == chunkenc.ValNone {
					break
				}

				if s2Type == chunkenc.ValNone {
					// TODO: mention which sample for what series
					return errors.New("subset block has a sample that is not in the superset block")
				}

				s1T, s1V := s1It.At()
				s2T, s2V := s2It.At()

				if s1T == s2T {
					if s1V != s2V && (!math.IsNaN(s1V) || !math.IsNaN(s2V)) {
						svMismatch++
						mismatch = true
					}

					s1Type = s1It.Next()
					s2Type = s2It.Next()
				} else if s1T < s2T {
					// subSeries has a sample that is not in supSeries.
					// TODO: mention which sample for what series
					return errors.New("subset block has a sample that is not in the superset block")
				} else {
					// supSeries has a sample that is not in subSeries. Advance supSeries.
					s2Type = s2It.Next()
				}
			}

			if mismatch {
				seriesMis++
			}

			s1Exists = subSeries.Next()
			s2Exists = supSeries.Next()
		} else if cmp < 0 {
			// TODO: mention which series
			return errors.New("subset block has series that are not in the superset block")
		} else {
			// supSeries has a series that is not in subSeries. Advance supSeries.
			s2Exists = supSeries.Next()
		}
	}

	if svMismatch > 0 {
		fmt.Printf("%d samples mismatch in value for the same timestamp in %d series\n", svMismatch, seriesMis)
	}

	return nil
}
