package commands

import (
	"context"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/cortex-tools/pkg/bench"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"
)

// BlockGenCommand is the kingpin command to generate blocks of mock data.
type BlockGenCommand struct {
	Replicas   int                `yaml:"replicas"`
	Series     []bench.SeriesDesc `yaml:"series"`
	Cfg        BlockGenConfig     `yaml:"block_gen"`
	configFile string
}

type BlockGenConfig struct {
	Interval  time.Duration `yaml:"interval"`
	BlockSize time.Duration `yaml:"block_size"`
	BlockDir  string        `yaml:"block_dir"`
	MinT      int64         `yaml:"min_t"`
	MaxT      int64         `yaml:"max_t"`
}

// Register is used to register the command to a parent command.
func (f *BlockGenCommand) Register(app *kingpin.Application) {
	app.Flag("config.file", "configuration file for this tool").Required().StringVar(&f.configFile)
	app.Action(f.run)
}

func (f *BlockGenCommand) run(k *kingpin.ParseContext) error {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	content, err := os.ReadFile(f.configFile)
	if err != nil {
		return errors.Wrap(err, "unable to read workload YAML file from the disk")
	}

	err = yaml.Unmarshal(content, &f)
	if err != nil {
		return errors.Wrap(err, "unable to unmarshal workload YAML file")
	}

	if f.Cfg.BlockDir == "" {
		var err error
		f.Cfg.BlockDir, err = ioutil.TempDir("", "mockdata")
		if err != nil {
			return errors.Wrap(err, "failed to create tmp dir")
		}
	}

	seriesSet, totalSeriesTypeMap := bench.SeriesDescToSeries(f.Series)
	totalSeries := 0
	for _, typeTotal := range totalSeriesTypeMap {
		totalSeries += typeTotal
	}

	writeWorkLoad := bench.WriteWorkload{
		TotalSeries:        totalSeries,
		TotalSeriesTypeMap: totalSeriesTypeMap,
		Replicas:           f.Replicas,
		Series:             seriesSet,
	}

	interval := f.Cfg.Interval.Milliseconds()
	blockSize := f.Cfg.BlockSize.Milliseconds()

	level.Info(logger).Log("msg", "Generating data", "minT", f.Cfg.MinT, "maxT", f.Cfg.MaxT, "interval", interval)
	currentTs := (int64(f.Cfg.MinT) + interval - 1) / interval * interval

	ctx := context.Background()
	currentBlockID := int64(-1)
	lastBlockID := blockID(f.Cfg.MaxT, blockSize)
	var w *tsdb.BlockWriter
	for ; currentTs <= f.Cfg.MaxT; currentTs += interval {
		if currentBlockID != blockID(currentTs, blockSize) {
			if w != nil {
				_, err = w.Flush(ctx)
				if err != nil {
					return err
				}
			}

			currentBlockID = blockID(currentTs, blockSize)
			level.Info(logger).Log("msg", "starting new block", "block_id", currentBlockID, "blocks_left", lastBlockID-currentBlockID+1)
			w, err = tsdb.NewBlockWriter(log.NewNopLogger(), f.Cfg.BlockDir, blockSize)
			if err != nil {
				return err
			}
		}

		timeSeries := writeWorkLoad.GenerateTimeSeries("block_gen", time.Unix(currentTs/1000, 0))

		app := w.Appender(ctx)
		for _, s := range timeSeries {
			var ref uint64

			labels := prompbLabelsToLabelsLabels(s.Labels)
			sort.Slice(labels, func(i, j int) bool {
				return labels[i].Name < labels[j].Name
			})

			for _, sample := range s.Samples {
				ref, err = app.Append(ref, labels, sample.Timestamp, sample.Value)
				if err != nil {
					return err
				}
			}
		}

		err = app.Commit()
		if err != nil {
			return err
		}
	}

	_, err = w.Flush(ctx)

	level.Info(logger).Log("msg", "finished", "block_dir", f.Cfg.BlockDir)

	return err
}

func blockID(ts, blockSize int64) int64 {
	return ts / blockSize
}

func prompbLabelsToLabelsLabels(in []prompb.Label) labels.Labels {
	out := make(labels.Labels, len(in))
	for idx := range in {
		out[idx].Name = in[idx].Name
		out[idx].Value = in[idx].Value
	}
	return out
}
