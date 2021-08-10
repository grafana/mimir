// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"context"
	"time"
)

// BucketClient is used to enforce retention on chunk buckets.
type BucketClient interface {
	DeleteChunksBefore(ctx context.Context, ts time.Time) error
}
