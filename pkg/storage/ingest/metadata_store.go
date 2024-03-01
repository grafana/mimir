// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

type MetadataStoreClient interface {
	CommitSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (SegmentRef, error)
	WatchSegments(ctx context.Context, partitionID int32, lastOffsetID int64) []SegmentRef
	GetLastProducedOffsetID(ctx context.Context, partitionID int32) (int64, error)
	CommitLastConsumedOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error
	GetLastConsumedOffsetID(ctx context.Context, partitionID int32, consumerID string) (int64, error)
}

type MetadataStore struct {
	services.Service

	db     MetadataStoreDatabase
	logger log.Logger
}

func NewMetadataStore(db MetadataStoreDatabase, logger log.Logger) *MetadataStore {
	s := &MetadataStore{
		db:     db,
		logger: logger,
	}

	s.Service = services.NewIdleService(s.starting, s.stopping)
	return s
}

func (s *MetadataStore) starting(ctx context.Context) error {
	return s.db.Open(ctx)
}

func (s *MetadataStore) stopping(_ error) error {
	s.db.Close()
	return nil
}

// CommitSegment commits a Segment to the metadata store.
func (s *MetadataStore) CommitSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (SegmentRef, error) {
	var lastErr error

	try := backoff.New(ctx, backoff.Config{
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		MaxRetries: 10,
	})

	for try.Ongoing() {
		var offsetID int64

		offsetID, lastErr = s.commitSegment(ctx, partitionID, objectID)
		if lastErr != nil {
			try.Wait()
			continue
		}

		return SegmentRef{
			PartitionID: partitionID,
			OffsetID:    offsetID,
			ObjectID:    objectID,
		}, nil
	}

	// If no error has been recorded yet, we fallback to the backoff error.
	if lastErr == nil {
		lastErr = try.Err()
	}

	return SegmentRef{}, lastErr
}

func (s *MetadataStore) commitSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (int64, error) {
	lastOffsetID, err := s.GetLastProducedOffsetID(ctx, partitionID)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get last offset ID for partition %d", partitionID)
	}

	nextOffsetID := lastOffsetID + 1

	ref := SegmentRef{
		PartitionID: partitionID,
		OffsetID:    nextOffsetID,
		ObjectID:    objectID,
	}

	// Attempt to insert the segment with the next offset ID, optimistically
	// hoping no other concurrent write on the same partition happened in the
	// meanwhile. If it did, the INSERT will fail.
	if err := s.db.InsertSegment(ctx, ref); err != nil {
		return 0, errors.Wrapf(err, "failed to offset segment for partition %d and offset %d", partitionID, nextOffsetID)
	}

	return nextOffsetID, nil
}

// GetLastProducedOffsetID returns the last produced offset ID for a given partitionID.
// If the partition is empty this function returns -1 and no error.
func (s *MetadataStore) GetLastProducedOffsetID(ctx context.Context, partitionID int32) (int64, error) {
	lastOffsetID, err := s.db.MaxPartitionOffset(ctx, partitionID)
	if err != nil {
		return 0, err
	}

	// The value is nil if there's no segment for the partition yet.
	if lastOffsetID == nil {
		return -1, nil
	}

	return *lastOffsetID, nil
}

// WatchSegments blocks until more segments are available. To replay a partition from the beginning
// you can specify lastOffsetID set to -1.
func (s *MetadataStore) WatchSegments(ctx context.Context, partitionID int32, lastOffsetID int64) []SegmentRef {
	try := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond,
		MaxRetries: 0, // Retry indefinitely.
	})

	for try.Ongoing() {
		segments, err := s.db.ListSegments(ctx, partitionID, lastOffsetID)
		if err != nil {
			level.Warn(s.logger).Log("msg", "failed to fetch segments while watching", "partition_id", partitionID, "last_offset_id", lastOffsetID, "err", err)
		}
		if len(segments) > 0 {
			return segments
		}

		try.Wait()
	}

	return nil
}

// CommitLastConsumedOffset updates the last offset consumed by a given consumerID for a specific partitionID.
func (s *MetadataStore) CommitLastConsumedOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error {
	err := s.db.UpsertConsumerOffset(ctx, partitionID, consumerID, offsetID)
	return errors.Wrapf(err, "failed to commit consumer %q offset %d for partition %d", consumerID, offsetID, partitionID)
}

// GetLastConsumedOffsetID returns the last offset consumed but a given consumerID for a specific partitionID.
// If the consumer hasn't committed any offset yet, this function will return -1 and no error.
func (s *MetadataStore) GetLastConsumedOffsetID(ctx context.Context, partitionID int32, consumerID string) (int64, error) {
	offsetID, err := s.db.GetConsumerOffset(ctx, partitionID, consumerID)
	if err != nil {
		return 0, err
	}

	// The value is nil if there's offset recorded for a specific consumer and partition.
	if offsetID == nil {
		return -1, nil
	}

	return *offsetID, nil
}

type MetadataStoreDatabase interface {
	// Open the database client.
	Open(ctx context.Context) error

	// Close the database client.
	Close()

	InsertSegment(ctx context.Context, ref SegmentRef) error
	ListSegments(ctx context.Context, partitionID int32, lastOffsetID int64) (segments []SegmentRef, returnErr error)
	MaxPartitionOffset(ctx context.Context, partitionID int32) (*int64, error)
	UpsertConsumerOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error
	GetConsumerOffset(ctx context.Context, partitionID int32, consumerID string) (*int64, error)
}

// MetadataStorePostgresql implements MetadataStoreDatabase for PostgreSQL.
type MetadataStorePostgresql struct {
	cfg         PostgresqlConfig
	connections *pgxpool.Pool
}

func NewMetadataStorePostgresql(cfg PostgresqlConfig) *MetadataStorePostgresql {
	return &MetadataStorePostgresql{
		cfg: cfg,
	}
}

func (s *MetadataStorePostgresql) Open(ctx context.Context) error {
	var err error

	// Create the connection pool.
	s.connections, err = pgxpool.New(ctx, s.cfg.Address)
	if err != nil {
		return errors.Wrap(err, "failed to created database connections pool")
	}

	return nil
}

func (s *MetadataStorePostgresql) Close() {
	s.connections.Close()
}

func (s *MetadataStorePostgresql) InsertSegment(ctx context.Context, ref SegmentRef) error {
	_, err := s.connections.Exec(ctx, "INSERT INTO SEGMENTS (PARTITION_ID, OFFSET_ID, OBJECT_ID) VALUES ($1, $2, $3)", ref.PartitionID, ref.OffsetID, ref.ObjectID.String())
	return err
}

// ListSegments fetch all segments for the given partitionID committed after lastOffsetID.
// This function may return some segments even if an error occurred.
func (s *MetadataStorePostgresql) ListSegments(ctx context.Context, partitionID int32, lastOffsetID int64) (segments []SegmentRef, returnErr error) {
	rows, err := s.connections.Query(ctx, "SELECT OFFSET_ID, OBJECT_ID FROM SEGMENTS WHERE PARTITION_ID = $1 AND OFFSET_ID > $2 ORDER BY OFFSET_ID", partitionID, lastOffsetID)
	if err != nil {
		return nil, err
	}

	// Ensure the rows will be closed once done.
	defer rows.Close()

	// Parse rows (if any).
	for rows.Next() {
		var (
			offsetID    int64
			rawObjectID string
		)

		if err := rows.Scan(&offsetID, &rawObjectID); err != nil {
			returnErr = multierr.Append(returnErr, err)
			return
		}

		// Parse object ID.
		objectID, err := ulid.Parse(rawObjectID)
		if err != nil {
			// This is a critical permanent error. Retrying will not fix it, so we keep track of the
			// error and move on.
			returnErr = multierr.Append(returnErr, err)
			continue
		}

		segments = append(segments, SegmentRef{
			PartitionID: partitionID,
			OffsetID:    offsetID,
			ObjectID:    objectID,
		})
	}

	return
}

func (s *MetadataStorePostgresql) MaxPartitionOffset(ctx context.Context, partitionID int32) (*int64, error) {
	var value *int64

	rows := s.connections.QueryRow(ctx, "SELECT MAX(OFFSET_ID) FROM SEGMENTS WHERE PARTITION_ID = $1", partitionID)
	if err := rows.Scan(&value); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	return value, nil
}

func (s *MetadataStorePostgresql) UpsertConsumerOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error {
	_, err := s.connections.Exec(ctx,
		"INSERT INTO CONSUMER_OFFSETS (PARTITION_ID, CONSUMER_ID, OFFSET_ID) VALUES ($1, $2, $3) ON CONFLICT (partition_id, consumer_id) DO UPDATE SET offset_id = $4",
		partitionID, consumerID, offsetID, offsetID)
	return err
}

func (s *MetadataStorePostgresql) GetConsumerOffset(ctx context.Context, partitionID int32, consumerID string) (*int64, error) {
	var value *int64

	rows := s.connections.QueryRow(ctx, "SELECT OFFSET_ID FROM CONSUMER_OFFSETS WHERE PARTITION_ID = $1 AND CONSUMER_ID = $2", partitionID, consumerID)
	if err := rows.Scan(&value); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}

	return value, nil
}
