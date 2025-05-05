// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
	commoncfg "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
)

type notifyHooksLimits interface {
	AlertmanagerNotifyHookURL(user string) string
	AlertmanagerNotifyHookReceivers(user string) []string
	AlertmanagerNotifyHookTimeout(user string) time.Duration
}

type notifyHooksNotifier struct {
	upstream notify.Notifier
	limits   notifyHooksLimits
	user     string
	logger   log.Logger
	client   *http.Client
}

func newNotifyHooksNotifier(upstream notify.Notifier, limits notifyHooksLimits, userID string, logger log.Logger) (*notifyHooksNotifier, error) {
	clientCfg := commoncfg.DefaultHTTPClientConfig

	// Inject user as X-Scope-OrgID into requests to hooks.
	clientCfg.HTTPHeaders = &commoncfg.Headers{
		Headers: map[string]commoncfg.Header{
			user.OrgIDHeaderName: {
				Values: []string{userID},
			},
		},
	}

	client, err := commoncfg.NewClientFromConfig(clientCfg, "notify_hooks")
	if err != nil {
		return nil, err
	}

	return &notifyHooksNotifier{
		upstream: upstream,
		limits:   limits,
		user:     userID,
		logger:   logger,
		client:   client,
	}, nil
}

func (n *notifyHooksNotifier) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
	newAlerts := n.apply(ctx, alerts)

	return n.upstream.Notify(ctx, newAlerts...)
}

func (n *notifyHooksNotifier) apply(ctx context.Context, alerts []*types.Alert) []*types.Alert {
	l := n.logger

	receiver, _ := notify.ReceiverName(ctx)
	l = log.With(l, "receiver", receiver)

	groupKey, _ := notify.GroupKey(ctx)
	l = log.With(l, "aggrGroup", groupKey)

	url := n.limits.AlertmanagerNotifyHookURL(n.user)
	if url == "" {
		level.Debug(l).Log("msg", "Notify hooks not applied, no URL configured")
		return alerts
	}

	receivers := n.limits.AlertmanagerNotifyHookReceivers(n.user)
	if len(receivers) > 0 && !slices.Contains(receivers, receiver) {
		level.Debug(l).Log("msg", "Notify hooks not applied, not enabled for receiver")
		return alerts
	}

	timeout := n.limits.AlertmanagerNotifyHookTimeout(n.user)

	newAlerts, err := n.invoke(ctx, l, url, timeout, alerts)
	if err != nil {
		level.Error(l).Log("msg", "Notify hooks failed", "err", err)
		return alerts
	}

	level.Debug(l).Log("msg", "Notify hooks applied successfully")

	return newAlerts
}

// hookData is the payload we send and receive from the notification hook.
type hookData struct {
	Receiver    string         `json:"receiver"`
	Status      string         `json:"status"`
	Alerts      []*types.Alert `json:"alerts"`
	GroupLabels model.LabelSet `json:"groupLabels"`
}

func (n *notifyHooksNotifier) getData(ctx context.Context, l log.Logger, alerts []*types.Alert) *hookData {
	recv, ok := notify.ReceiverName(ctx)
	if !ok {
		level.Error(l).Log("msg", "Missing receiver")
	}
	groupLabels, ok := notify.GroupLabels(ctx)
	if !ok {
		level.Error(l).Log("msg", "Missing group labels")
	}

	return &hookData{
		Receiver:    recv,
		Status:      string(types.Alerts(alerts...).Status()),
		Alerts:      alerts,
		GroupLabels: groupLabels,
	}
}

func (n *notifyHooksNotifier) invoke(ctx context.Context, l log.Logger, url string, timeout time.Duration, alerts []*types.Alert) ([]*types.Alert, error) {
	dat := n.getData(ctx, l, alerts)

	var reqBuf bytes.Buffer
	if err := json.NewEncoder(&reqBuf).Encode(dat); err != nil {
		return nil, err
	}

	if timeout > 0 {
		postCtx, cancel := context.WithTimeoutCause(ctx, timeout,
			fmt.Errorf("notify hook timeout reached (%s)", timeout))
		defer cancel()
		ctx = postCtx
	}

	level.Debug(l).Log("msg", "Hook started", "url", url, "timeout", timeout)

	resp, err := notify.PostJSON(ctx, n.client, url, &reqBuf)
	if err != nil {
		if ctx.Err() != nil {
			err = fmt.Errorf("%w: %w", err, context.Cause(ctx))
		}
		return nil, notify.RedactURL(err)
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result hookData
	err = json.Unmarshal(respBuf, &result)
	if err != nil {
		return nil, err
	}

	return result.Alerts, nil
}
