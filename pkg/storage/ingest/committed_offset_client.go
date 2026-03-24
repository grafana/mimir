// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CommittedOffsetClient fetches the last committed offset for a consumer group and partition.
type CommittedOffsetClient struct {
	admin *kadm.Client
	topic string
}

func NewCommittedOffsetClient(client *kgo.Client, topic string) *CommittedOffsetClient {
	return &CommittedOffsetClient{
		admin: kadm.NewClient(client),
		topic: topic,
	}
}

// FetchLastCommittedOffset returns the last committed offset for the given consumer group and partition.
// Returns exists=false if the consumer group or partition has no committed offset.
func (c *CommittedOffsetClient) FetchLastCommittedOffset(ctx context.Context, consumerGroup string, partitionID int32) (offset int64, exists bool, _ error) {
	offsets, err := c.admin.FetchOffsets(ctx, consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("unable to fetch group offsets: %w", err)
	}

	offsetRes, exists := offsets.Lookup(c.topic, partitionID)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, offsetRes.Err
	}

	return offsetRes.At, true, nil
}
