// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/alerting/receivers"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeNotifier struct {
	ctxForTesting context.Context
	calls         [][]*types.Alert
}

func (m *fakeNotifier) Notify(ctx context.Context, alerts ...*types.Alert) (bool, error) {
	m.ctxForTesting = ctx
	m.calls = append(m.calls, alerts)
	return false, nil
}

type fakeHookLimits struct {
	url       string
	receivers []string
	timeout   time.Duration
}

func (f *fakeHookLimits) AlertmanagerNotifyHookURL(_ string) string {
	return f.url
}

func (f *fakeHookLimits) AlertmanagerNotifyHookReceivers(_ string) []string {
	return f.receivers
}

func (f *fakeHookLimits) AlertmanagerNotifyHookTimeout(_ string) time.Duration {
	return f.timeout
}

type testHooksFixture struct {
	limits   *fakeHookLimits
	server   *httptest.Server
	upstream *fakeNotifier
	reg      *prometheus.Registry

	notifier *notifyHooksNotifier
}

func (f *testHooksFixture) assertMetricsSuccess(t *testing.T, total, noop int) {
	t.Helper()

	assert.NoError(t, testutil.GatherAndCompare(f.reg, strings.NewReader(fmt.Sprintf(`
		# HELP alertmanager_notify_hook_total Number of times a pre-notify hook was invoked.
		# TYPE alertmanager_notify_hook_total counter
		alertmanager_notify_hook_total %d
		# HELP alertmanager_notify_hook_noop_total Number of times a pre-notify hook was invoked successfully but did nothing.
		# TYPE alertmanager_notify_hook_noop_total counter
		alertmanager_notify_hook_noop_total %d
	`, total, noop)),
		"alertmanager_notify_hook_total",
		"alertmanager_notify_hook_noop_total",
		"alertmanager_notify_hook_failed_total",
		// Don't check duration.
	))
}

func (f *testHooksFixture) assertMetricsFailed(t *testing.T, code string, failed int) {
	t.Helper()

	assert.NoError(t, testutil.GatherAndCompare(f.reg, strings.NewReader(fmt.Sprintf(`
		# HELP alertmanager_notify_hook_total Number of times a pre-notify hook was invoked.
		# TYPE alertmanager_notify_hook_total counter
		alertmanager_notify_hook_total %d
		# HELP alertmanager_notify_hook_noop_total Number of times a pre-notify hook was invoked successfully but did nothing.
		# TYPE alertmanager_notify_hook_noop_total counter
		alertmanager_notify_hook_noop_total %d
		# HELP alertmanager_notify_hook_failed_total Number of times a pre-notify was attempted but failed.
		# TYPE alertmanager_notify_hook_failed_total counter
		alertmanager_notify_hook_failed_total{status_code="%s"} %d
	`, failed, 0, code, failed)),
		"alertmanager_notify_hook_total",
		"alertmanager_notify_hook_noop_total",
		"alertmanager_notify_hook_failed_total",
		// Don't check duration.
	))
}

func newTestHooksFixture(t *testing.T, handlerStatus int, handlerResponse string) *testHooksFixture {
	t.Helper()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test/hook", r.URL.Path)

		assert.Equal(t, "user", r.Header.Get("X-Scope-OrgID"))

		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)

		assert.Equal(t, `{`+
			`"receiver":"recv",`+
			`"status":"firing",`+
			`"alerts":[`+
			`{"labels":{"label":"foo"},`+
			`"annotations":null,`+
			`"startsAt":"0001-01-01T00:00:00Z",`+
			`"endsAt":"0001-01-01T00:00:00Z",`+
			`"generatorURL":"",`+
			`"UpdatedAt":"0001-01-01T00:00:00Z",`+
			`"Timeout":false}],`+
			`"groupLabels":{}}`+"\n", string(body))

		w.WriteHeader(handlerStatus)
		_, err = w.Write([]byte(handlerResponse))
		assert.NoError(t, err)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		server.Close()
	})

	limits := &fakeHookLimits{
		url:       server.URL + "/test/hook",
		receivers: []string{},
		timeout:   time.Minute,
	}

	upstream := &fakeNotifier{}

	reg := prometheus.NewPedanticRegistry()
	notifier, err := newNotifyHooksNotifier(upstream, limits, "user", log.NewLogfmtLogger(os.Stdout), newNotifyHooksMetrics(reg))
	require.NoError(t, err)

	return &testHooksFixture{
		limits:   limits,
		server:   server,
		upstream: upstream,
		reg:      reg,
		notifier: notifier,
	}
}

func makeAlert(value string) []*types.Alert {
	return []*types.Alert{
		{
			Alert: model.Alert{
				Labels: model.LabelSet{
					model.LabelName("label"): model.LabelValue(value),
				},
			},
		},
	}
}

func makeContext() context.Context {
	ctx := context.Background()
	ctx = notify.WithReceiverName(ctx, "recv")
	ctx = notify.WithGroupKey(ctx, "gk")
	ctx = notify.WithGroupLabels(ctx, model.LabelSet{})
	return ctx
}

func TestNotifyHooksNotifier(t *testing.T) {
	const okResponse = `{` +
		`"alerts":[` +
		`{"labels":{"label":"changed"},` +
		`"annotations":null,` +
		`"startsAt":"0001-01-01T00:00:00Z",` +
		`"endsAt":"0001-01-01T00:00:00Z",` +
		`"generatorURL":"",` +
		`"UpdatedAt":"0001-01-01T00:00:00Z",` +
		`"Timeout":false}],` +
		`"extraData": [{"one": "two"}, {"three": "four"}]` +
		`}`

	t.Run("hook invoked", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusOK, okResponse)

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("changed")}, f.upstream.calls)
		f.assertMetricsSuccess(t, 1, 0)
	})
	t.Run("hook invoked with extra data", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusOK, okResponse)
		ctx := makeContext()

		_, err := f.notifier.Notify(ctx, makeAlert("foo")...)
		require.NoError(t, err)

		extraDataRaw, ok := f.upstream.ctxForTesting.Value(receivers.ExtraDataKey).([]json.RawMessage)
		require.True(t, ok)
		require.Equal(t, 2, len(extraDataRaw))
		require.Equal(t, `{"one": "two"}`, string(extraDataRaw[0]))
		require.Equal(t, `{"three": "four"}`, string(extraDataRaw[1]))
	})
	t.Run("hook not invoked when empty url configured", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusOK, okResponse)
		f.limits.url = ""

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsSuccess(t, 0, 0)
	})
	t.Run("hook not invoked when matching receiver name configured ", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusOK, okResponse)
		f.limits.receivers = []string{"otherrecv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsSuccess(t, 0, 0)
	})
	t.Run("hook invoked when matching receiver name configured ", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusOK, okResponse)
		f.limits.receivers = []string{"recv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("changed")}, f.upstream.calls)
		f.assertMetricsSuccess(t, 1, 0)
	})

	t.Run("hook failing with 500 does not modify alerts", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusInternalServerError, ``)
		f.limits.receivers = []string{"recv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsFailed(t, "500", 1)
	})
	t.Run("hook failing with 500 but returning data does not modify alerts", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusInternalServerError, okResponse)
		f.limits.receivers = []string{"recv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsFailed(t, "500", 1)
	})
	t.Run("hook failing with 422 does not modify alerts", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusUnprocessableEntity, ``)
		f.limits.receivers = []string{"recv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsFailed(t, "422", 1)
	})
	t.Run("hook yielding 204 with empty response does not modify alerts", func(t *testing.T) {
		f := newTestHooksFixture(t, http.StatusNoContent, ``)
		f.limits.receivers = []string{"recv"}

		_, err := f.notifier.Notify(makeContext(), makeAlert("foo")...)
		require.NoError(t, err)

		require.Equal(t, [][]*types.Alert{makeAlert("foo")}, f.upstream.calls)
		f.assertMetricsSuccess(t, 1, 1)
	})
}
