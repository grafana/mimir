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

	consumerGroup string
	topic         string
	partition     int32
}

func NewCommittedOffsetClient(client *kgo.Client, consumerGroup, topic string, partitionID int32) *CommittedOffsetClient {
	return &CommittedOffsetClient{
		admin: kadm.NewClient(client),

		consumerGroup: consumerGroup,
		topic:         topic,
		partition:     partitionID,
	}
}

// FetchLastCommittedOffset returns the last committed offset.
// Returns exists=false if the client's consumer group or partition has no committed offset.
func (c *CommittedOffsetClient) FetchLastCommittedOffset(ctx context.Context) (offset int64, exists bool, _ error) {
	offsets, err := c.admin.FetchOffsets(ctx, c.consumerGroup)
	if errors.Is(err, kerr.GroupIDNotFound) || errors.Is(err, kerr.UnknownTopicOrPartition) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("fetch group %s offsets: %w", c.consumerGroup, err)
	}

	offsetRes, exists := offsets.Lookup(c.topic, c.partition)
	if !exists {
		return 0, false, nil
	}
	if offsetRes.Err != nil {
		return 0, false, fmt.Errorf("fetch group %s offset: topic %s, partition %d: %w", c.consumerGroup, c.topic, c.partition, offsetRes.Err)
	}

	return offsetRes.At, true, nil
}
