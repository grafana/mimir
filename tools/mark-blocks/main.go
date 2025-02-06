package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type config struct {
	bucket             bucket.Config
	tenantID           string
	markType           string
	details            string
	blocks             flagext.StringSliceCSV
	blocksFile         string
	resumeIndex        int
	remove             bool
	metaPresencePolicy string
	dryRun             bool
	concurrency        int
}

func (cfg *config) registerFlags(f *flag.FlagSet) {
	cfg.bucket.RegisterFlags(f)
	f.StringVar(&cfg.tenantID, "tenant", "", "Tenant ID of the owner of the block. Required.")
	f.StringVar(&cfg.markType, "mark-type", "", "Mark type to create, valid options: deletion, no-compact. Required.")
	f.StringVar(&cfg.details, "details", "", "Details to include in a mark.")
	f.Var(&cfg.blocks, "blocks", "Comma separated list of blocks. If non-empty, blocks-file is ignored.")
	f.StringVar(&cfg.blocksFile, "blocks-file", "-", "File containing block IDs to mark. Defaults to standard input. Ignored if blocks is non-empty")
	f.IntVar(&cfg.resumeIndex, "resume-index", 0, "The index of the block in blocks-file to resume from")
	f.BoolVar(&cfg.remove, "remove", false, "If marks should be removed rather than uploaded.")
	f.StringVar(&cfg.metaPresencePolicy, "meta-presence-policy", "require", "How to validate block meta.json files: \"none\", \"skip-block\", or \"require\".")
	f.BoolVar(&cfg.dryRun, "dry-run", false, "Don't upload the markers generated, just print the intentions.")
	f.IntVar(&cfg.concurrency, "concurrency", 16, "How many markers to upload or remove concurrently.")
}

func (cfg *config) validate() error {
	if cfg.tenantID == "" {
		return errors.New("-tenant is required")
	}
	if err := tenant.ValidTenantID(cfg.tenantID); err != nil {
		return fmt.Errorf("-tenant is invalid: %w", err)
	}
	if cfg.markType == "" {
		return errors.New("-mark is required")
	}
	if cfg.blocksFile == "" && len(cfg.blocks) == 0 {
		return errors.New("one of -blocks or -blocks-file must be specified")
	}
	if cfg.concurrency < 1 {
		return errors.New("-concurrency must be positive")
	}
	if cfg.resumeIndex < 0 {
		return errors.New("-resume-index must be non-negative")
	}
	return nil
}

func main() {
	var cfg config
	cfg.registerFlags(flag.CommandLine)

	logger := log.NewLogfmtLogger(os.Stdout)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		level.Error(logger).Log("msg", "failed to parse flags", "err", err)
		os.Exit(1)
	}

	if err := cfg.validate(); err != nil {
		level.Error(logger).Log("msg", "flags did not pass validation", "err", err)
		os.Exit(1)
	}

	blocks, err := getBlocks(cfg.blocks, cfg.blocksFile)
	if err != nil {
		level.Error(logger).Log("msg", "failed to read blocks to mark", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		level.Error(logger).Log("msg", "failed to create bucket", "err", err)
		os.Exit(1)
	}

	if cfg.resumeIndex >= len(blocks) {
		level.Error(logger).Log("msg", "invalid resume index", "resumeIndex", cfg.resumeIndex, "numBlocks", len(blocks))
		os.Exit(1)
	} else if cfg.resumeIndex > 0 {
		level.Info(logger).Log("msg", "skipping blocks due to resume", "resumeIndex", cfg.resumeIndex, "numBlocks", len(blocks))
	}

	blocks = blocks[cfg.resumeIndex:]
	if len(blocks) == 0 {
		level.Warn(logger).Log("msg", "no blocks, nothing marked")
		os.Exit(0)
	}

	mpf, err := metaPresenceFunc(cfg.tenantID, bkt, cfg.metaPresencePolicy)
	if err != nil {
		level.Error(logger).Log("msg", "failed to handle meta validation policy", "err", err)
		os.Exit(1)
	}

	mbf, suffix, err := markerBytesFunc(cfg.markType, cfg.details)
	if err != nil {
		level.Error(logger).Log("msg", "failed to handle marker type", "err", err)
		os.Exit(1)
	}

	var f func(context.Context, int) error
	if cfg.remove {
		f = removeMarksFunc(cfg, bkt, blocks, mpf, suffix, logger)
	} else {
		f = addMarksFunc(cfg, bkt, blocks, mpf, mbf, suffix, logger)
	}

	if successUntil, err := forEachJobSuccessUntil(ctx, len(blocks), cfg.concurrency, f); err != nil {
		// since indices were possibly based on a subslice, account for that as a starting point
		// successUntil is known to be the first index that did not succeed
		resumeFrom := cfg.resumeIndex + successUntil

		level.Error(logger).Log("msg", "encountered a failure", "err", err, "resumeFrom", resumeFrom)
		os.Exit(1)
	}
}

// forEachJobSuccessUntil executes a function concurrently until an error is encountered or all jobs have completed
// It also returns the maximum index i where all indexes [0, i) succeeded and 0 <= i <= numJobs
func forEachJobSuccessUntil(ctx context.Context, numJobs int, jobConcurrency int, f func(ctx context.Context, idx int) error) (int, error) {
	succeeded := make([]bool, numJobs)
	err := concurrency.ForEachJob(ctx, numJobs, jobConcurrency, func(ctx context.Context, idx int) error {
		if err := f(ctx, idx); err != nil {
			return err
		}
		succeeded[idx] = true
		return nil
	})
	if err == nil {
		return numJobs, err
	}
	return slices.Index(succeeded, false), err
}

// removeMarksFunc returns a function that removes block markers for a given block index
func removeMarksFunc(cfg config, bkt objstore.Bucket, blocks []ulid.ULID, mpf metaPresence, suffix string, logger log.Logger) func(context.Context, int) error {
	return func(ctx context.Context, idx int) error {
		block := blocks[idx].String()

		skip, err := mpf(ctx, block)
		if err != nil {
			return err
		}
		if skip {
			level.Info(logger).Log("msg", fmt.Sprintf("skipping block because its meta.json was not found: %s", block))
			return nil
		}

		blockMarkPath := localMarkPath(cfg.tenantID, block, suffix)
		globalMarkPath := globalMarkPath(cfg.tenantID, block, suffix)

		if cfg.dryRun {
			level.Info(logger).Log("msg", fmt.Sprintf("would delete global mark at %s", globalMarkPath))
			level.Info(logger).Log("msg", fmt.Sprintf("would delete mark at %s", blockMarkPath))
			return nil
		}

		// Global mark deleted first, local mark deleted second to follow write ordering
		for _, markPath := range []string{globalMarkPath, blockMarkPath} {
			if err := bkt.Delete(ctx, markPath); err != nil {
				if bkt.IsObjNotFoundErr(err) {
					level.Info(logger).Log("msg", fmt.Sprintf("deleted mark, but it was not present at %s", markPath))
					continue
				}
				return err
			}
			level.Info(logger).Log("msg", fmt.Sprintf("deleted mark at %s", markPath))
		}
		return nil
	}
}

// addMarksFunc returns a function that uploads block markers for a given block index
func addMarksFunc(
	cfg config,
	bkt objstore.Bucket,
	blocks []ulid.ULID,
	mpf metaPresence,
	mbf markerBytes,
	suffix string,
	logger log.Logger) func(context.Context, int) error {

	return func(ctx context.Context, idx int) error {
		block := blocks[idx]
		blockStr := block.String()

		skip, err := mpf(ctx, blockStr)
		if err != nil {
			return err
		}
		if skip {
			level.Info(logger).Log("msg", fmt.Sprintf("skipping block because its meta.json was not found: %s", block))
			return nil
		}

		b, err := mbf(block)
		if err != nil {
			return err
		}

		blockMarkPath := localMarkPath(cfg.tenantID, blockStr, suffix)
		globalMarkPath := globalMarkPath(cfg.tenantID, blockStr, suffix)

		if cfg.dryRun {
			level.Info(logger).Log("msg", fmt.Sprintf("would upload mark to %s", blockMarkPath))
			level.Info(logger).Log("msg", fmt.Sprintf("would upload global mark to %s", globalMarkPath))
			return nil
		}

		// Local mark first, global mark second to follow write ordering
		for _, markPath := range []string{globalMarkPath, blockMarkPath} {
			if err := bkt.Upload(ctx, markPath, bytes.NewReader(b)); err != nil {
				return err
			}
			level.Info(logger).Log("msg", fmt.Sprintf("uploaded mark to %s", markPath))
		}
		return nil
	}
}

func localMarkPath(tenantID, blk, markSuffix string) string {
	return path.Join(tenantID, blk, markSuffix)
}

func globalMarkPath(tenantID, blk, markSuffix string) string {
	return path.Join(tenantID, block.MarkersPathname, blk+"-"+markSuffix)
}

func metaPath(tenantID, blk string) string {
	return path.Join(tenantID, blk, block.MetaFilename)
}

type markerBytes func(b ulid.ULID) ([]byte, error)

// markerBytesFunc accepts a mark type and details and returns a function to create a marker (given a blockID) and the name suffix of said marker
// If the mark type is unrecognized a non-nil error is returned
func markerBytesFunc(markType, details string) (markerBytes, string, error) {
	switch markType {
	case "no-compact":
		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(block.NoCompactMark{
				ID:            b,
				Version:       block.NoCompactMarkVersion1,
				NoCompactTime: time.Now().Unix(),
				Reason:        block.ManualNoCompactReason,
				Details:       details,
			})
		}, block.NoCompactMarkFilename, nil
	case "deletion":
		return func(b ulid.ULID) ([]byte, error) {
			return json.Marshal(block.DeletionMark{
				ID:           b,
				Version:      block.DeletionMarkVersion1,
				Details:      details,
				DeletionTime: time.Now().Unix(),
			})
		}, block.DeletionMarkFilename, nil
	default:
		return nil, "", fmt.Errorf("invalid mark type (must be no-compact or deletion): %q", markType)
	}
}

type metaPresence func(ctx context.Context, blk string) (bool, error)

func metaPresenceFunc(tenantID string, bkt objstore.Bucket, policy string) (metaPresence, error) {
	switch policy {
	case "none":
		// The meta is not checked at all
		return func(ctx context.Context, blk string) (bool, error) {
			return false, nil
		}, nil
	case "skip-block":
		// If the meta is not present, skip this block
		return func(ctx context.Context, blk string) (bool, error) {
			exists, err := bkt.Exists(ctx, metaPath(tenantID, blk))
			return !exists, err
		}, nil
	case "require":
		// If the meta is not present an error is returned
		return func(ctx context.Context, blk string) (bool, error) {
			metaName := metaPath(tenantID, blk)
			exists, err := bkt.Exists(ctx, metaName)
			if err != nil {
				return false, err
			}
			if !exists {
				return false, fmt.Errorf("block's metadata did not exist: %q", metaName)
			}
			return false, nil
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized meta-validation-policy: %q", policy)
	}

}

func getBlocks(blocks flagext.StringSliceCSV, filePath string) ([]ulid.ULID, error) {
	if len(blocks) > 0 {
		r := strings.NewReader(strings.Join(blocks, "\n"))
		return readBlocks(r)
	}

	input, err := getInputFile(filePath)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	return readBlocks(input)

}

func getInputFile(filePath string) (*os.File, error) {
	if filePath == "-" {
		return os.Stdin, nil
	}
	return os.Open(filePath)
}

// readBlocks reads lines of blockIDs
func readBlocks(r io.Reader) ([]ulid.ULID, error) {
	var blocks []ulid.ULID
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		u, err := ulid.Parse(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse a string=%s as a ULID", line)
		}
		blocks = append(blocks, u)
	}
	return blocks, nil
}
