package compactor

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type stagedBlock struct {
	meta     *block.Meta
	blockDir string

	// unstagedPostings tracks postings within this block that have been unstaged. It is used to clean staged blocks of
	// already-accessed data after a compaction run has completed.
	//
	// It should only be accessed with the unstagedPostingsLock held.
	unstagedPostings     map[string]timeRange
	unstagedPostingsLock *sync.Mutex
}

func newStagedBlock(meta *block.Meta, blockDir string) *stagedBlock {
	return &stagedBlock{
		meta:                 meta,
		blockDir:             blockDir,
		unstagedPostings:     make(map[string]timeRange),
		unstagedPostingsLock: &sync.Mutex{},
	}
}

func (s *stagedBlock) registerUnstaged(minT, maxT int64, lbls labels.Labels) {
	s.unstagedPostingsLock.Lock()
	defer s.unstagedPostingsLock.Unlock()

	s.unstagedPostings[lbls.String()] = timeRange{minTime: minT, maxTime: maxT}
}

type timeRange struct {
	minTime, maxTime int64
}

type stagedBlocks struct {
	bkt             objstore.Bucket
	logger          log.Logger
	stagedDir       string
	staged          []*stagedBlock
	targetLabelSets map[string]timeRange
}

func newStagedBlocks(bkt objstore.Bucket, logger log.Logger, stagedDir string) *stagedBlocks {
	return &stagedBlocks{
		bkt:       bkt,
		logger:    logger,
		stagedDir: stagedDir,
	}
}

func (sb *stagedBlocks) hasStagedBlocks() bool {
	return len(sb.staged) > 0
}

func (sb *stagedBlocks) addStagedBlocks(ctx context.Context, metas map[ulid.ULID]*block.Meta) error {
	for blockID, meta := range metas {
		blockDir := filepath.Join(sb.stagedDir, blockID.String())

		if err := block.Download(ctx, sb.logger, sb.bkt, blockID, blockDir); err != nil {
			return errors.Wrapf(err, "download block %s", blockID.String())
		}

		targetLabelSets, err := getTargetLabelSets(meta)
		if err != nil {
			return errors.Wrapf(err, "get target labels for staged block %s", blockID.String())
		}

		for _, targetLabelSet := range targetLabelSets {
			sb.targetLabelSets[targetLabelSet.String()] = timeRange{minTime: meta.MinTime, maxTime: meta.MaxTime}
		}

		sb.staged = append(sb.staged, newStagedBlock(meta, blockDir))
	}

	return nil
}

func getTargetLabelSets(meta *block.Meta) ([]labels.Labels, error) {
	stagedMarkFile := block.StagedMarkFilepath(meta.ULID)

	r, err := os.Open(stagedMarkFile)
	if err != nil {
		return nil, errors.Wrapf(err, "open staged mark file %s", stagedMarkFile)
	}
	stagedBuf, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrapf(err, "read staged mark file %s", stagedMarkFile)
	}
	_ = r.Close()

	var stagedInfo block.StagedMark
	if err = json.Unmarshal(stagedBuf, &stagedInfo); err != nil {
		return nil, errors.Wrapf(err, "unmarshal staged mark file %s", stagedMarkFile)
	}

	return stagedInfo.TargetLabelSets, nil
}
