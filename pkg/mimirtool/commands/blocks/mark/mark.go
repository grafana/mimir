// SPDX-License-Identifier: AGPL-3.0-only

package mark

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

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type Command struct {
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

func (c *Command) RegisterFlags(f *flag.FlagSet) {
	c.bucket.RegisterFlags(f)
	f.StringVar(&c.markType, "mark-type", "", "Mark type to create or remove, valid options: deletion, no-compact. Required.")
	f.StringVar(&c.tenantID, "tenant", "", "Tenant ID of the owner of the block(s). If empty then each block is assumed to be of the form tenantID/blockID, blockID otherwise.")
	f.StringVar(&c.details, "details", "", "Details to include in an added mark.")
	f.Var(&c.blocks, "blocks", "Comma separated list of blocks. If non-empty, blocks-file is ignored.")
	f.StringVar(&c.blocksFile, "blocks-file", "-", "File containing a block per-line. Defaults to standard input. Ignored if blocks is non-empty")
	f.IntVar(&c.resumeIndex, "resume-index", 0, "The index of the block to resume from")
	f.BoolVar(&c.remove, "remove", false, "If marks should be removed rather than uploaded.")
	f.StringVar(&c.metaPresencePolicy, "meta-presence-policy", "skip-block", "Policy on presence of block meta.json files: \"none\", \"skip-block\", or \"require\".")
	f.BoolVar(&c.dryRun, "dry-run", false, "Log changes that would be made instead of actually making them.")
	f.IntVar(&c.concurrency, "concurrency", 16, "How many markers to upload or remove concurrently.")
}

func (c *Command) Register(parent *kingpin.CmdClause, getLogger func() log.Logger) {
	cmd := parent.Command("mark", "Create or remove block markers (deletion, no-compact) in object storage.").
		Action(func(_ *kingpin.ParseContext) error {
			return c.Run(getLogger())
		})

	fs := flag.NewFlagSet("mark", flag.PanicOnError)
	c.RegisterFlags(fs)
	fs.VisitAll(func(f *flag.Flag) {
		cmd.Flag(f.Name, f.Usage).SetValue(f.Value)
	})
}

func (c *Command) Validate() error {
	if c.markType == "" {
		return errors.New("-mark-type is required")
	}
	if c.tenantID != "" {
		if err := tenant.ValidTenantID(c.tenantID); err != nil {
			return fmt.Errorf("-tenant is invalid: %w", err)
		}
	}
	if c.blocksFile == "" && len(c.blocks) == 0 {
		return errors.New("one of -blocks or -blocks-file must be specified")
	}
	if c.concurrency < 1 {
		return errors.New("-concurrency must be positive")
	}
	if c.resumeIndex < 0 {
		return errors.New("-resume-index must be non-negative")
	}
	return nil
}

type inputBlock struct {
	tenantID string
	blockID  ulid.ULID
}

func (c *Command) Run(logger log.Logger) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("flags did not pass validation: %w", err)
	}

	blocks, err := getBlocks(c.blocks, c.blocksFile, c.tenantID)
	if err != nil {
		return fmt.Errorf("failed to read blocks to mark: %w", err)
	}

	ctx := context.Background()
	bkt, err := bucket.NewClient(ctx, c.bucket, "bucket", logger, nil)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	if c.resumeIndex >= len(blocks) {
		return fmt.Errorf("invalid resume index %d for %d blocks", c.resumeIndex, len(blocks))
	} else if c.resumeIndex > 0 {
		level.Info(logger).Log("msg", "skipping blocks due to resume", "resumeIndex", c.resumeIndex, "numBlocks", len(blocks))
	}

	blocks = blocks[c.resumeIndex:]
	if len(blocks) == 0 {
		level.Warn(logger).Log("msg", "no blocks, nothing marked")
		return nil
	}

	mpf, err := metaPresenceFunc(bkt, c.metaPresencePolicy)
	if err != nil {
		return fmt.Errorf("failed to handle meta validation policy: %w", err)
	}

	mbf, suffix, err := markerBytesFunc(c.markType, c.details)
	if err != nil {
		return fmt.Errorf("failed to handle marker type: %w", err)
	}

	var f func(context.Context, int) error
	if c.remove {
		f = removeMarksFunc(bkt, blocks, mpf, suffix, logger, c.dryRun)
	} else {
		f = addMarksFunc(bkt, blocks, mpf, mbf, suffix, logger, c.dryRun)
	}

	if successUntil, err := forEachJobSuccessUntil(ctx, len(blocks), c.concurrency, f); err != nil {
		// since indices were possibly based on a subslice, account for that as a starting point
		// successUntil is known to be the first index that did not succeed
		resumeIndex := c.resumeIndex + successUntil
		return fmt.Errorf("encountered a failure (resume with -resume-index=%d): %w", resumeIndex, err)
	}

	return nil
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
	if err != nil {
		return slices.Index(succeeded, false), err
	}
	return numJobs, nil
}

// removeMarksFunc returns a function that removes block markers for a given block index
func removeMarksFunc(bkt objstore.Bucket, blocks []inputBlock, mpf metaPresence, suffix string, logger log.Logger, dryRun bool) func(context.Context, int) error {
	return func(ctx context.Context, idx int) error {
		tenantID := blocks[idx].tenantID
		blockID := blocks[idx].blockID.String()

		skip, err := mpf(ctx, tenantID, blockID)
		if err != nil {
			return err
		}
		if skip {
			level.Info(logger).Log("msg", fmt.Sprintf("skipping block because its meta.json was not found: %s/%s", tenantID, blockID))
			return nil
		}

		localMarkPath := localMarkPath(tenantID, blockID, suffix)
		globalMarkPath := globalMarkPath(tenantID, blockID, suffix)

		if dryRun {
			level.Info(logger).Log("msg", fmt.Sprintf("would delete global mark at %s", globalMarkPath))
			level.Info(logger).Log("msg", fmt.Sprintf("would delete mark at %s", localMarkPath))
			return nil
		}

		// Global mark deleted first, local mark deleted second to follow write ordering
		for _, markPath := range []string{globalMarkPath, localMarkPath} {
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
	bkt objstore.Bucket,
	blocks []inputBlock,
	mpf metaPresence,
	mbf markerBytes,
	suffix string,
	logger log.Logger,
	dryRun bool) func(context.Context, int) error {

	return func(ctx context.Context, idx int) error {
		tenantID := blocks[idx].tenantID
		block := blocks[idx].blockID
		blockStr := block.String()

		skip, err := mpf(ctx, tenantID, blockStr)
		if err != nil {
			return err
		}
		if skip {
			level.Info(logger).Log("msg", fmt.Sprintf("skipping block because its meta.json was not found: %s/%s", tenantID, blockStr))
			return nil
		}

		b, err := mbf(block)
		if err != nil {
			return err
		}

		localMarkPath := localMarkPath(tenantID, blockStr, suffix)
		globalMarkPath := globalMarkPath(tenantID, blockStr, suffix)

		if dryRun {
			level.Info(logger).Log("msg", fmt.Sprintf("would upload mark to %s", localMarkPath))
			level.Info(logger).Log("msg", fmt.Sprintf("would upload global mark to %s", globalMarkPath))
			return nil
		}

		// Local mark first, global mark second to follow write ordering
		for _, markPath := range []string{localMarkPath, globalMarkPath} {
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

type metaPresence func(ctx context.Context, tenantID string, blk string) (bool, error)

func metaPresenceFunc(bkt objstore.Bucket, policy string) (metaPresence, error) {
	switch policy {
	case "none":
		// The meta is not checked at all
		return func(_ context.Context, _, _ string) (bool, error) {
			return false, nil
		}, nil
	case "skip-block":
		// If the meta is not present, skip this block
		return func(ctx context.Context, tenantID, blockID string) (bool, error) {
			exists, err := bkt.Exists(ctx, metaPath(tenantID, blockID))
			return !exists, err
		}, nil
	case "require":
		// If the meta is not present an error is returned
		return func(ctx context.Context, tenantID, blockID string) (bool, error) {
			metaName := metaPath(tenantID, blockID)
			exists, err := bkt.Exists(ctx, metaName)
			if err != nil {
				return false, err
			}
			if !exists {
				return false, fmt.Errorf("block's metadata did not exist: %s", metaName)
			}
			return false, nil
		}, nil
	default:
		return nil, fmt.Errorf("unrecognized meta-presence-policy: %q", policy)
	}

}

func getBlocks(blocks flagext.StringSliceCSV, filePath string, tenantID string) ([]inputBlock, error) {
	if len(blocks) > 0 {
		r := strings.NewReader(strings.Join(blocks, "\n"))
		return readBlocks(r, tenantID)
	}

	input, err := getInputFile(filePath)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	return readBlocks(input, tenantID)

}

func getInputFile(filePath string) (*os.File, error) {
	if filePath == "-" {
		return os.Stdin, nil
	}
	return os.Open(filePath)
}

// readBlocks reads lines of blockIDs if tenant is non-empty or tenantID/blockIDs otherwise
func readBlocks(r io.Reader, tenant string) ([]inputBlock, error) {
	var blocks []inputBlock
	scanner := bufio.NewScanner(r)
	var parser func(string) (inputBlock, error)
	if tenant == "" {
		parser = parseTenantBlockLine
	} else {
		parser = func(block string) (inputBlock, error) {
			return parseBlock(tenant, block)
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		block, err := parser(line)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

func parseTenantBlockLine(line string) (inputBlock, error) {
	tenantID, blockString, found := strings.Cut(line, "/")
	if !found {
		return inputBlock{}, fmt.Errorf("invalid block format. when --tenant is empty each block must look like tenantID/blockID, found: %s", line)
	}
	if err := tenant.ValidTenantID(tenantID); err != nil {
		return inputBlock{}, fmt.Errorf("tenantID parsed from %s is invalid: %w", line, err)
	}
	blockString = strings.TrimSuffix(blockString, "/") // tolerate a trailing slash
	return parseBlock(
		tenantID,
		blockString,
	)
}

func parseBlock(tenant, block string) (inputBlock, error) {
	u, err := ulid.Parse(block)
	if err != nil {
		return inputBlock{}, fmt.Errorf("failed to parse a ULID from: %q", block)
	}
	return inputBlock{
		tenantID: tenant,
		blockID:  u,
	}, nil
}
