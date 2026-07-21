// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"time"

	"github.com/go-kit/log/level"
	"google.golang.org/grpc/peer"
)

// watchStreamObserver instruments one assignment watch stream: a
// connect log line with the subscriber's peer address, per-send
// metrics (snapshot vs delta messages/entries/bytes), and a
// disconnect log line with lifetime totals. Snapshot sends are the
// expensive ones — the full retention-bounded log per message — so
// the connect/disconnect lines plus the started_total counter are
// what identify stream churn (pod restarts, reconnect loops,
// max-connection-age recycling) when the rebalancer's TX is high.
type watchStreamObserver struct {
	r        *Rebalancer
	stream   string // "hash" | "readcache"
	peer     string
	deltas   bool
	start    time.Time
	gaugeDec func()

	snapshots, snapshotEntries int
	deltaMsgs, deltaEntries    int
	bytes                      int
	err                        error
}

func newWatchStreamObserver(r *Rebalancer, stream string, supportsDeltas bool, ctx context.Context) *watchStreamObserver {
	peerAddr := "unknown"
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		peerAddr = p.Addr.String()
	}
	protocol := "legacy"
	if supportsDeltas {
		protocol = "delta"
	}
	o := &watchStreamObserver{
		r:        r,
		stream:   stream,
		peer:     peerAddr,
		deltas:   supportsDeltas,
		start:    r.now(),
		gaugeDec: r.metrics.watchStreamStarted(stream, protocol),
	}
	if r.logger != nil {
		level.Info(r.logger).Log(
			"msg", "assignment watch stream connected",
			"stream", stream,
			"peer", peerAddr,
			"supports_deltas", supportsDeltas,
		)
	}
	return o
}

// recordSend accounts one outgoing message. bytes is the marshaled
// proto size, which approximates wire cost (excludes gRPC framing
// and compression).
func (o *watchStreamObserver) recordSend(reset bool, entries, bytes int) {
	kind := "delta"
	if reset {
		kind = "snapshot"
		o.snapshots++
		o.snapshotEntries += entries
	} else {
		o.deltaMsgs++
		o.deltaEntries += entries
	}
	o.bytes += bytes
	o.r.metrics.recordWatchSend(o.stream, kind, entries, bytes)
}

// fail records the error that is about to terminate the stream and
// returns it, so handlers can `return obs.fail(err)`.
func (o *watchStreamObserver) fail(err error) error {
	o.err = err
	return err
}

// finish decrements the active-streams gauge and logs the stream's
// lifetime totals. Deferred by the watch handlers.
func (o *watchStreamObserver) finish() {
	o.gaugeDec()
	if o.r.logger == nil {
		return
	}
	level.Info(o.r.logger).Log(
		"msg", "assignment watch stream closed",
		"stream", o.stream,
		"peer", o.peer,
		"supports_deltas", o.deltas,
		"duration", o.r.now().Sub(o.start).Round(time.Millisecond),
		"snapshots_sent", o.snapshots,
		"snapshot_entries", o.snapshotEntries,
		"deltas_sent", o.deltaMsgs,
		"delta_entries", o.deltaEntries,
		"bytes_sent", o.bytes,
		"err", o.err,
	)
}
