package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func main() {
	var markerCmd marker
	app := kingpin.New("markblocks", "A command-line tool to create and upload markers for Mimir TSDB blocks.")
	markerCmd.register(app)

	noCompactMarkCmd := &noCompactMark{marker: &markerCmd}
	noCompactMarkCmd.register(app)
	deleteMarkCmd := &deleteMark{marker: &markerCmd}
	deleteMarkCmd.register(app)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}

type marker struct {
	bucket   bucket.Config
	userID   string
	blockIDs []string

	dryRun bool
}

func (m *marker) register(app *kingpin.Application) {
	m.bucket.RegisterFlags(kingPinFlagSet{app})
	app.Flag("user", "User (tenant) owner of the block.").
		Required().
		StringVar(&m.userID)
	app.Flag("block", "The ULIDs of the blocks to be marked.").
		Required().
		StringsVar(&m.blockIDs)
	app.Flag("dry-run", "Don't upload the markers generated, just print the intentions on the screen.").
		BoolVar(&m.dryRun)
}

func (m *marker) run(mark blockMarker) error {
	ctx := context.Background()
	logger := log.WithPrefix(log.NewLogfmtLogger(os.Stderr), "time", log.DefaultTimestampUTC)

	var ulids []ulid.ULID
	for _, b := range m.blockIDs {
		blockID, err := ulid.Parse(b)
		if err != nil {
			return fmt.Errorf("can't parse %q as ULID: %w", b, err)
		}
		ulids = append(ulids, blockID)
	}

	bkt, err := bucket.NewClient(ctx, m.bucket, "bucket", logger, nil)
	if err != nil {
		return fmt.Errorf("can't instantiate bucket: %w", err)
	}
	userBucket := bucketindex.BucketWithGlobalMarkers(
		bucket.NewUserBucketClient(m.userID, bkt, nil),
	)

	for _, b := range ulids {
		blockMetaFilename := fmt.Sprintf("%s/meta.json", b)

		if exists, err := userBucket.Exists(ctx, blockMetaFilename); err != nil {
			return fmt.Errorf("can't check whether block meta.json exists for %s: %w", b, err)
		} else if !exists {
			logger.Log("msg", "Block does not exist, skipping", "block_id", b)
			continue
		}

		blockMarkFilename := fmt.Sprintf("%s/%s", b, mark.filename())
		if exists, err := userBucket.Exists(ctx, blockMarkFilename); err != nil {
			return fmt.Errorf("can't check whether block meta.json exists for %s: %w", b, err)
		} else if exists {
			logger.Log("msg", "Mark already exists, skipping", "block_id", b)
			continue
		}

		data, err := mark.block(b)
		if err != nil {
			return fmt.Errorf("can't create mark for block %s: %w", b, err)
		}
		if m.dryRun {
			logger.Log("msg", "Dry-run, so not making changes", "block_id", b, "mark", string(data))
			continue
		}

		if err := userBucket.Upload(ctx, blockMarkFilename, bytes.NewReader(data)); err != nil {
			logger.Log("msg", "Can't upload mark", "block_id", b, "err", err)
			return fmt.Errorf("can't upload mark for block %s: %w", b, err)
		}
	}

	return nil
}

type blockMarker interface {
	filename() string
	block(b ulid.ULID) ([]byte, error)
}

type noCompactMark struct {
	marker *marker

	reason  string
	details string
}

func (mark *noCompactMark) filename() string { return metadata.NoCompactMarkFilename }

func (mark *noCompactMark) register(app *kingpin.Application) {
	validReasons := []string{
		string(metadata.ManualNoCompactReason),
		string(metadata.IndexSizeExceedingNoCompactReason),
		string(metadata.OutOfOrderChunksNoCompactReason),
	}

	cmd := app.Command("no-compact", "Mark blocks as non-compactable.").
		Action(func(_ *kingpin.ParseContext) error { return mark.marker.run(mark) })
	cmd.Flag("reason", fmt.Sprintf("The reason field of the marker. Valid reasons are: %s.", strings.Join(validReasons, ", "))).
		Default(string(metadata.ManualNoCompactReason)).
		EnumVar(&mark.reason, validReasons...)
	cmd.Flag("details", "The details field of the marker.").
		StringVar(&mark.details)
}

func (mark *noCompactMark) block(b ulid.ULID) ([]byte, error) {
	return json.Marshal(metadata.NoCompactMark{
		ID:            b,
		Version:       metadata.NoCompactMarkVersion1,
		NoCompactTime: time.Now().Unix(),
		Reason:        metadata.NoCompactReason(mark.reason),
		Details:       mark.details,
	})
}

type deleteMark struct {
	marker *marker

	details string
}

func (mark *deleteMark) filename() string { return metadata.DeletionMarkFilename }

func (mark *deleteMark) register(app *kingpin.Application) {
	cmd := app.Command("delete", "Mark blocks for deletion.").
		Action(func(_ *kingpin.ParseContext) error { return mark.marker.run(mark) })
	cmd.Flag("details", "The details field of the marker.").
		StringVar(&mark.details)
}

func (mark *deleteMark) block(b ulid.ULID) ([]byte, error) {
	return json.Marshal(metadata.DeletionMark{
		ID:      b,
		Version: metadata.NoCompactMarkVersion1,
		Details: mark.details,
	})
}
