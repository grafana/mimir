package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type Converter struct {
	bucket            objstore.Bucket
	logger            log.Logger
	labelsCompression bool
	chunksCompression bool
	labelsCodec       string
	chunksCodec       string
}

func NewConverter(bucket objstore.Bucket, logger log.Logger, labelsCompression, chunksCompression bool, labelsCodec, chunksCodec string) *Converter {
	return &Converter{
		bucket:            bucket,
		logger:            logger,
		labelsCompression: labelsCompression,
		chunksCompression: chunksCompression,
		labelsCodec:       labelsCodec,
		chunksCodec:       chunksCodec,
	}
}

func (c *Converter) ConvertAll(ctx context.Context, userList []string) error {
	var users []string

	if len(userList) == 0 {
		var err error
		users, err = c.ListAllUsers(ctx)
		if err != nil {
			return fmt.Errorf("failed to list users: %w", err)
		}
	} else {
		users = userList
	}

	level.Info(c.logger).Log("msg", "Converting blocks to parquet", "users", len(users))

	for _, user := range users {
		if err := c.convertUserBlocks(ctx, user); err != nil {
			level.Error(c.logger).Log("msg", "Failed to convert blocks for user", "user", user, "err", err)
			return fmt.Errorf("failed to convert blocks for user %s: %w", user, err)
		}
	}

	level.Info(c.logger).Log("msg", "Conversion completed")
	return nil
}

func (c *Converter) convertUserBlocks(ctx context.Context, user string) error {
	level.Info(c.logger).Log("msg", "Converting blocks for user", "user", user)

	instrumentedBkt := objstore.WithNoopInstr(c.bucket)
	userBkt := bucket.NewUserBucketClient(user, instrumentedBkt, nil)
	ulogger := util_log.WithUserID(user, c.logger)

	blocks, err := c.listTSDBBlocks(ctx, user, userBkt, ulogger)
	if err != nil {
		return fmt.Errorf("failed to list TSDB blocks: %w", err)
	}

	level.Info(ulogger).Log("msg", "Found TSDB blocks", "blocks", len(blocks))

	tmpDir := filepath.Join(os.TempDir(), "mimir-converter-"+user)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			level.Warn(ulogger).Log("msg", "Failed to cleanup temp directory", "err", err)
		}
	}()

	for _, meta := range blocks {
		if err := c.convertBlock(ctx, user, meta, tmpDir, userBkt, ulogger); err != nil {
			level.Error(ulogger).Log("msg", "Failed to convert block", "block", meta.ULID, "err", err)
			return fmt.Errorf("failed to convert block %s: %w", meta.ULID, err)
		}
	}

	return nil
}

func (c *Converter) listTSDBBlocks(ctx context.Context, user string, userBkt objstore.InstrumentedBucket, logger log.Logger) ([]*block.Meta, error) {
	// Use the same block discovery logic as ParquetConverter
	fetcher, err := block.NewMetaFetcher(
		logger,
		20,
		userBkt,
		filepath.Join(os.TempDir(), "meta-"+user),
		prometheus.NewRegistry(),
		[]block.MetadataFilter{},
		nil, // No meta cache
		0,   // No lookback limit
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create meta fetcher: %w", err)
	}

	metas, _, err := fetcher.Fetch(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metas: %w", err)
	}

	var blocksToConvert []*block.Meta
	for _, meta := range metas {
		// Check if already converted using the same logic as ParquetConverter
		mark, err := parquetconverter.ReadConversionMark(ctx, meta.ULID, userBkt, logger)
		if err != nil {
			level.Debug(logger).Log("msg", "Failed to read conversion mark, assuming not converted", "block", meta.ULID.String(), "err", err)
			blocksToConvert = append(blocksToConvert, meta)
		} else if mark.Version != parquetconverter.CurrentVersion {
			blocksToConvert = append(blocksToConvert, meta)
		}
	}

	return blocksToConvert, nil
}

func (c *Converter) convertBlock(ctx context.Context, user string, meta *block.Meta, tmpDir string, userBkt objstore.InstrumentedBucket, logger log.Logger) error {
	level.Info(logger).Log("msg", "Converting block", "block", meta.ULID.String())

	localBlockDir := filepath.Join(tmpDir, meta.ULID.String())

	// Download block using the same logic as ParquetConverter
	if err := block.Download(ctx, logger, userBkt, meta.ULID, localBlockDir, objstore.WithFetchConcurrency(10)); err != nil {
		return fmt.Errorf("failed to download block: %w", err)
	}

	// Convert block using the same logic as ParquetConverter
	if err := c.convertBlockToParquet(ctx, meta, localBlockDir, userBkt, logger); err != nil {
		return fmt.Errorf("failed to convert block to parquet: %w", err)
	}

	// Write conversion mark
	if err := parquetconverter.WriteConversionMark(ctx, meta.ULID, userBkt); err != nil {
		level.Error(logger).Log("msg", "Failed to write conversion mark", "block", meta.ULID.String(), "err", err)
		return fmt.Errorf("failed to write conversion mark: %w", err)
	}

	level.Info(logger).Log("msg", "Successfully converted block", "block", meta.ULID.String())
	return nil
}

func (c *Converter) convertBlockToParquet(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger) error {
	// This is the same conversion logic as the ParquetConverter's defaultBlockConverter
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(logger), localBlockDir, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, tsdbBlock, "close tsdb block")

	convertOpts := []convert.ConvertOption{
		convert.WithName(meta.ULID.String()),
	}
	
	if c.labelsCompression {
		labelsCodec := parseCodec(c.labelsCodec)
		convertOpts = append(convertOpts, convert.WithLabelsCompression(
			schema.WithCompressionEnabled(true),
			schema.WithCompressionCodec(labelsCodec),
		))
	} else {
		convertOpts = append(convertOpts, convert.WithLabelsCompression(schema.WithCompressionEnabled(false)))
	}
	
	if c.chunksCompression {
		chunksCodec := parseCodec(c.chunksCodec)
		convertOpts = append(convertOpts, convert.WithChunksCompression(
			schema.WithCompressionEnabled(true),
			schema.WithCompressionCodec(chunksCodec),
		))
	} else {
		convertOpts = append(convertOpts, convert.WithChunksCompression(schema.WithCompressionEnabled(false)))
	}

	_, err = convert.ConvertTSDBBlock(
		ctx,
		bkt,
		meta.MinTime,
		meta.MaxTime,
		[]convert.Convertible{tsdbBlock},
		convertOpts...,
	)
	return err
}

func (c *Converter) ListAllUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bucket.Iter(ctx, "", func(name string) error {
		parts := strings.Split(name, "/")
		if len(parts) > 0 && parts[0] != "" {
			user := parts[0]
			for _, existingUser := range users {
				if existingUser == user {
					return nil
				}
			}
			users = append(users, user)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to iterate bucket: %w", err)
	}

	return users, nil
}
