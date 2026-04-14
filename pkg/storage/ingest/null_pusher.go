// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// NullPusher implements Pusher by discarding all data without writing to any storage.
// It's designed for load testing the write path without incurring the cost of TSDB writes.
type NullPusher struct{}

// PushToStorageAndReleaseRequest implements Pusher.
func (n *NullPusher) PushToStorageAndReleaseRequest(_ context.Context, req *mimirpb.WriteRequest) error {
	req.FreeBuffer()
	return nil
}

// NotifyPreCommit implements PreCommitNotifier.
func (n *NullPusher) NotifyPreCommit(_ context.Context) error {
	return nil
}
