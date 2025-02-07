package stages

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/alertmanager/nflog"
	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
)

type Action string

const (
	Disabled     Action = "disabled"
	LogOnly      Action = "log-only"
	StopPipeline Action = "stop-pipeline"
)

type PipelineAndStateTimestampCoordinationStage struct {
	nflog        notify.NotificationLog
	recv         *nflogpb.Receiver
	stopPipeline bool
}

func NewPipelineAndStateTimestampCoordinationStage(l notify.NotificationLog, recv *nflogpb.Receiver, action Action) *PipelineAndStateTimestampCoordinationStage {
	var stop bool
	switch action {
	case LogOnly:
		stop = false
	case StopPipeline:
		stop = true
	default:
		return nil
	}
	return &PipelineAndStateTimestampCoordinationStage{
		nflog:        l,
		recv:         recv,
		stopPipeline: stop,
	}
}

// Exec implements the Stage interface.
func (n *PipelineAndStateTimestampCoordinationStage) Exec(ctx context.Context, l log.Logger, alerts ...*types.Alert) (context.Context, []*types.Alert, error) {
	gkey, ok := notify.GroupKey(ctx)
	if !ok {
		return ctx, nil, errors.New("group key missing")
	}

	entries, err := n.nflog.Query(nflog.QGroupKey(gkey), nflog.QReceiver(n.recv))
	if err != nil && !errors.Is(err, nflog.ErrNotFound) {
		return ctx, nil, err
	}

	var entry *nflogpb.Entry
	switch len(entries) {
	case 0:
		return ctx, alerts, nil
	case 1:
		entry = entries[0]
	default:
		return ctx, nil, fmt.Errorf("unexpected entry result size %d", len(entries))
	}

	// get the tick time from the context.
	timeNow, ok := notify.Now(ctx)
	// now make sure that the current state is from past
	if ok && entry.Timestamp.After(timeNow) {
		diff := entry.Timestamp.Sub(timeNow)
		// this usually means that the WaitStage took longer than the group_wait, and the subsequent node in the cluster sees the event from the first node
		_ = level.Warn(l).Log("msg", "timestamp of notification log entry is after the current pipeline timestamp.", "entry_time", entry.Timestamp, "pipeline_time", timeNow, "diff", diff, "dropped", n.stopPipeline)
		if n.stopPipeline {
			return ctx, nil, nil
		}
	}
	return ctx, alerts, nil
}
