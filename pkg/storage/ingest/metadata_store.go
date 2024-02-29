// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
)

type MetadataStore struct {
	services.Service

	cfg         PostgresqlConfig
	logger      log.Logger
	connections *pgxpool.Pool
}

func NewMetadataStore(cfg PostgresqlConfig, logger log.Logger) *MetadataStore {
	s := &MetadataStore{
		cfg:    cfg,
		logger: logger,
	}

	s.Service = services.NewIdleService(s.starting, s.stopping)
	return s
}

// TODO correctly handle reconnections
func (s *MetadataStore) starting(ctx context.Context) error {
	var err error

	// Create the connection pool.
	s.connections, err = pgxpool.New(ctx, s.cfg.Address)
	if err != nil {
		return errors.Wrap(err, "failed to created database connections pool")
	}

	return nil
}

func (s *MetadataStore) stopping(_ error) error {
	s.connections.Close()

	return nil
}

// AddSegment adds a Segment to the metadata store.
func (s *MetadataStore) AddSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (Segment, error) {
	var lastErr error

	try := backoff.New(ctx, backoff.Config{
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		MaxRetries: 10,
	})

	for try.Ongoing() {
		var commitID int64

		commitID, lastErr = s.commitSegment(ctx, partitionID, objectID)
		if lastErr != nil {
			try.Wait()
			continue
		}

		return Segment{
			PartitionID: partitionID,
			CommitID:    commitID,
			ObjectID:    objectID,
		}, nil
	}

	// If no error has been recorded yet, we fallback to the backoff error.
	if lastErr == nil {
		lastErr = try.Err()
	}

	return Segment{}, lastErr
}

func (s *MetadataStore) commitSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (int64, error) {
	lastCommitID, err := s.getLastCommitID(ctx, partitionID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get last commit ID for partition %d", partitionID)
	}

	nextCommitID := lastCommitID + 1

	// Attempt to insert the segment with the next commit ID, optimistically
	// hoping no other concurrent write on the same partition happened in the
	// meanwhile. If it did, the INSERT will fail.
	_, err = s.connections.Exec(ctx, "INSERT INTO segments (partition_id, commit_id, object_id) VALUES ($1, $2, $3)", partitionID, nextCommitID, objectID.String())
	if err != nil {
		return 0, errors.Wrapf(err, "failed to commit segment for partition %d and commit %d", partitionID, nextCommitID)
	}

	return nextCommitID, nil
}

func (s *MetadataStore) getLastCommitID(ctx context.Context, partitionID int32) (int64, error) {
	var lastCommitID *int64

	rows := s.connections.QueryRow(ctx, "SELECT MAX(commit_id) FROM segments WHERE partition_id = $1", partitionID)
	if err := rows.Scan(&lastCommitID); err != nil {
		return 0, err
	}

	// The value is nil if there's no segment for the partition yet.
	if lastCommitID == nil {
		return -1, nil
	}

	return *lastCommitID, nil
}
