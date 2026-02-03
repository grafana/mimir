// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

type InMemoryPostingsOffsetTableCache struct {
}

func (i InMemoryPostingsOffsetTableCache) StorePostingsOffset(userID string, blockID ulid.ULID, k labels.Label, v index.Range, ttl time.Duration) {
	//TODO implement me
	panic("implement me")
}

func (i InMemoryPostingsOffsetTableCache) FetchPostingsOffset(ctx context.Context, userID string, blockID ulid.ULID, k labels.Label) (index.Range, bool) {
	//TODO implement me
	panic("implement me")
}
