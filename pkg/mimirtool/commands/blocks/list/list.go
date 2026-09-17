// SPDX-License-Identifier: AGPL-3.0-only

package list

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/listblocks"
)

type Command struct {
	bucket              bucket.Config
	userID              string
	format              string
	showDeleted         bool
	showLabels          bool
	showUlidTime        bool
	showSources         bool
	showParents         bool
	showHints           bool
	showCompactionLevel bool
	showBlockSize       bool
	showStats           bool
	splitCount          int
	minTime             flagext.Time
	maxTime             flagext.Time

	useUlidTimeForMinTimeCheck bool
}

func (c *Command) RegisterFlags(f *flag.FlagSet) {
	c.bucket.RegisterFlags(f)
	f.StringVar(&c.userID, "user", "", "The user (tenant) that owns the blocks to be listed")
	f.StringVar(&c.format, "format", "tabbed", "The format of the output. Must be one of \"tabbed\", \"json\", or \"yaml\"")
	f.BoolVar(&c.showDeleted, "show-deleted", false, "Show blocks marked for deletion")
	f.BoolVar(&c.showLabels, "show-labels", false, "Show block labels")
	f.BoolVar(&c.showUlidTime, "show-ulid-time", false, "Show time from ULID")
	f.BoolVar(&c.showSources, "show-sources", false, "Show compaction sources")
	f.BoolVar(&c.showParents, "show-parents", false, "Show parent blocks")
	f.BoolVar(&c.showHints, "show-hints", false, "Show compaction hints")
	f.BoolVar(&c.showCompactionLevel, "show-compaction-level", false, "Show compaction level")
	f.BoolVar(&c.showBlockSize, "show-block-size", false, "Show size of block based on details in meta.json, if available")
	f.BoolVar(&c.showStats, "show-stats", false, "Show block stats (number of series, chunks, samples)")
	f.IntVar(&c.splitCount, "split-count", 0, "It not 0, shows split number that would be used for grouping blocks during split compaction")
	f.Var(&c.minTime, "min-time", "If set, only blocks with MinTime >= this value are printed")
	f.Var(&c.maxTime, "max-time", "If set, only blocks with MaxTime <= this value are printed")
	f.BoolVar(&c.useUlidTimeForMinTimeCheck, "use-ulid-time-for-min-time-check", false, "If true, meta.json files for blocks with ULID time before min-time are not loaded. This may incorrectly skip blocks that have data from the future (minT/maxT higher than ULID).")
}

func (c *Command) Register(parent *kingpin.CmdClause, getLogger func() log.Logger) {
	cmd := parent.Command("list", "List blocks and show block details of a tenant.").
		Action(func(_ *kingpin.ParseContext) error {
			return c.Run(getLogger())
		})

	fs := flag.NewFlagSet("list", flag.PanicOnError)
	c.RegisterFlags(fs)
	fs.VisitAll(func(f *flag.Flag) {
		cmd.Flag(f.Name, f.Usage).SetValue(f.Value)
	})
}

func (c *Command) Validate() error {
	if c.userID == "" {
		return errors.New("no user specified")
	}
	if !slices.Contains([]string{"tabbed", "json", "yaml"}, c.format) {
		return fmt.Errorf("an invalid format was specified: %s", c.format)
	}
	return nil
}

func (c *Command) Run(logger log.Logger) error {
	if err := c.Validate(); err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	bkt, err := bucket.NewClient(ctx, c.bucket, "bucket", logger, nil)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	loadMetasMinTime := time.Time{}
	if c.useUlidTimeForMinTimeCheck {
		loadMetasMinTime = time.Time(c.minTime)
	}

	metas, deleteMarkerDetails, noCompactMarkerDetails, err := listblocks.LoadMetaFilesAndMarkers(ctx, bkt, c.userID, c.showDeleted, loadMetasMinTime)
	if err != nil {
		return fmt.Errorf("failed to read block metadata: %w", err)
	}

	blocks := buildBlocksList(metas, deleteMarkerDetails, noCompactMarkerDetails, c)
	if c.format == "tabbed" {
		printTabbedOutput(blocks, c)
		return nil
	}

	var b []byte
	if c.format == "yaml" {
		b, err = yaml.Marshal(blocks)
	} else {
		b, err = json.Marshal(blocks)
	}

	if err != nil {
		return fmt.Errorf("failed to marshal blocks list: %w", err)
	}

	fmt.Println(string(b))
	return nil
}

type noCompactSummary struct {
	Time   string `json:"time" yaml:"time"`
	Reason string `json:"reason" yaml:"reason"`
}

// buildBlocksList constructs a map of fields for each block that will be later marshalled by printTabbedOutput or into JSON/YAML
// printTabbedOutput uses key names created in this function directly so synchronize changes there (some keys aren't used by it)
func buildBlocksList(metas map[ulid.ULID]*block.Meta, deleteMarkerDetails map[ulid.ULID]block.DeletionMark, noCompactMarkerDetails map[ulid.ULID]block.NoCompactMark, cfg *Command) []map[string]any {
	blocks := make([]map[string]any, 0, len(metas))
	for _, b := range listblocks.SortBlocks(metas) {
		if !cfg.showDeleted && deleteMarkerDetails[b.ULID].DeletionTime != 0 {
			continue
		}
		if !time.Time(cfg.minTime).IsZero() && util.TimeFromMillis(b.MinTime).Before(time.Time(cfg.minTime)) {
			continue
		}
		if !time.Time(cfg.maxTime).IsZero() && util.TimeFromMillis(b.MaxTime).After(time.Time(cfg.maxTime)) {
			continue
		}

		m := make(map[string]any)

		m["blockID"] = b.ULID
		if cfg.splitCount > 0 {
			m["splitID"] = tsdb.HashBlockID(b.ULID) % uint32(cfg.splitCount)
		}
		if cfg.showUlidTime {
			m["ulidTime"] = util.TimeFromMillis(int64(b.ULID.Time())).UTC().Format(time.RFC3339)
		}
		m["minTime"] = util.TimeFromMillis(b.MinTime).UTC().Format(time.RFC3339)
		m["maxTime"] = util.TimeFromMillis(b.MaxTime).UTC().Format(time.RFC3339)

		duration := util.TimeFromMillis(b.MaxTime).Sub(util.TimeFromMillis(b.MinTime))
		m["duration"] = duration.String()
		m["durationSeconds"] = math.Floor(duration.Seconds())

		if val, ok := noCompactMarkerDetails[b.ULID]; ok {
			m["noCompact"] = noCompactSummary{
				Time:   time.Unix(val.NoCompactTime, 0).UTC().Format(time.RFC3339),
				Reason: string(val.Reason),
			}
		}

		if cfg.showDeleted {
			m["deletionTime"] = time.Unix(deleteMarkerDetails[b.ULID].DeletionTime, 0).UTC().Format(time.RFC3339)
		}

		if cfg.showCompactionLevel {
			m["level"] = b.Compaction.Level
		}

		if cfg.showBlockSize {
			m["size"] = listblocks.GetFormattedBlockSize(b)
			m["sizeBytes"] = listblocks.GetBlockSizeBytes(b)
		}

		if cfg.showStats {
			m["numSeries"] = b.Stats.NumSeries
			m["numSamples"] = b.Stats.NumSamples
			m["numChunks"] = b.Stats.NumChunks
		}

		if cfg.showLabels {
			m["labels"] = labels.FromMap(b.Thanos.Labels)
		}

		if cfg.showSources {
			m["sources"] = b.Compaction.Sources
		}

		if cfg.showParents {
			var p []ulid.ULID
			for _, pb := range b.Compaction.Parents {
				p = append(p, pb.ULID)
			}
			m["parents"] = p
		}

		if cfg.showHints {
			m["hints"] = b.Compaction.Hints
		}

		blocks = append(blocks, m)
	}

	return blocks
}

// nolint:errcheck
//
//goland:noinspection GoUnhandledErrorResult
func printTabbedOutput(blocks []map[string]any, cfg *Command) {

	tabber := tabwriter.NewWriter(os.Stdout, 1, 4, 3, ' ', 0)
	defer tabber.Flush()

	// Header
	fmt.Fprintf(tabber, "Block ID")
	if cfg.splitCount > 0 {
		fmt.Fprintf(tabber, "\tSplit ID")
	}
	if cfg.showUlidTime {
		fmt.Fprintf(tabber, "\tULID Time")
	}
	fmt.Fprintf(tabber, "\tMin Time")
	fmt.Fprintf(tabber, "\tMax Time")
	fmt.Fprintf(tabber, "\tDuration")
	fmt.Fprintf(tabber, "\tNo Compact")
	if cfg.showDeleted {
		fmt.Fprintf(tabber, "\tDeletion Time")
	}
	if cfg.showCompactionLevel {
		fmt.Fprintf(tabber, "\tLvl")
	}
	if cfg.showBlockSize {
		fmt.Fprintf(tabber, "\tSize")
	}
	if cfg.showStats {
		fmt.Fprintf(tabber, "\tSeries")
		fmt.Fprintf(tabber, "\tSamples")
		fmt.Fprintf(tabber, "\tChunks")
	}
	if cfg.showLabels {
		fmt.Fprintf(tabber, "\tLabels")
	}
	if cfg.showSources {
		fmt.Fprintf(tabber, "\tSources")
	}
	if cfg.showParents {
		fmt.Fprintf(tabber, "\tParents")
	}
	if cfg.showHints {
		fmt.Fprintf(tabber, "\tHints")
	}
	fmt.Fprintln(tabber)

	for _, b := range blocks {
		fmt.Fprintf(tabber, "%v", b["blockID"])
		if cfg.splitCount > 0 {
			fmt.Fprintf(tabber, "\t%d", b["splitID"])
		}
		if cfg.showUlidTime {
			fmt.Fprintf(tabber, "\t%v", b["ulidTime"])
		}
		fmt.Fprintf(tabber, "\t%v", b["minTime"])
		fmt.Fprintf(tabber, "\t%v", b["maxTime"])
		fmt.Fprintf(tabber, "\t%v", b["duration"])

		if val, ok := b["noCompact"]; ok {
			val := val.(noCompactSummary)
			fmt.Fprintf(tabber, "\t%v", []string{
				fmt.Sprintf("Time: %s", val.Time),
				fmt.Sprintf("Reason: %s", val.Reason),
			})
		} else {
			fmt.Fprintf(tabber, "\t")
		}

		if cfg.showDeleted {
			if val, ok := b["deletionTime"]; ok {
				fmt.Fprintf(tabber, "\t%v", val)
			} else {
				fmt.Fprintf(tabber, "\t") // no deletion time.
			}
		}

		if cfg.showCompactionLevel {
			fmt.Fprintf(tabber, "\t%d", b["level"])
		}

		if cfg.showBlockSize {
			fmt.Fprintf(tabber, "\t%s", b["size"])
		}

		if cfg.showStats {
			fmt.Fprintf(tabber, "\t%d", b["numSeries"])
			fmt.Fprintf(tabber, "\t%d", b["numSamples"])
			fmt.Fprintf(tabber, "\t%d", b["numChunks"])
		}

		if cfg.showLabels {
			fmt.Fprintf(tabber, "\t%s", b["labels"])
		}

		if cfg.showSources {
			fmt.Fprintf(tabber, "\t%v", b["sources"])
		}

		if cfg.showParents {
			fmt.Fprintf(tabber, "\t%v", b["parents"])
		}

		if cfg.showHints {
			fmt.Fprintf(tabber, "\t%v", b["hints"])
		}

		fmt.Fprintln(tabber)
	}
}
