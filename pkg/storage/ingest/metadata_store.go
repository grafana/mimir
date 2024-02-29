// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/jackc/pgx/v5"
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

	lastCommitID, err := s.getLastCommitID(ctx, partitionID)
	fmt.Println("getLastCommitID() lastCommitID:", lastCommitID, "err:", err)

	//s.conn.Exec(ctx, `INSERT INTO segments `)
	//
	//var name string
	//var weight int64
	//err = conn.QueryRow(context.Background(), "select name, weight from widgets where id=$1", 42).Scan(&name, &weight)
	//if err != nil {
	//	fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
	//}
	//
	//fmt.Println(name, weight)

	return Segment{
		PartitionID: partitionID,
		CommitID:    0, // TODO
		ObjectID:    objectID,
	}, nil
}

func (s *MetadataStore) getLastCommitID(ctx context.Context, partitionID int32) (int64, error) {
	var lastCommitID int64

	row := s.connections.QueryRow(ctx, "SELECT MAX(commit_id) FROM segments WHERE partition_id = $1", partitionID)
	if err := row.Scan(&lastCommitID); errors.Is(err, pgx.ErrNoRows) {
		return -1, nil
	} else if err != nil {
		return 0, err
	}

	return lastCommitID, nil
}
