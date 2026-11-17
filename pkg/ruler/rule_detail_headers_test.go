// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var ruleDetailHeaders = []string{ruleNameHeader, ruleTypeHeader, ruleSourceHeader, ruleNamespaceHeader, ruleGroupHeader}

// ruleEvaluationContext returns a context carrying the same origin data the Prometheus rules
// manager attaches when evaluating a rule. The group's file is built the same way the mapper
// names the rule files it writes to disk.
func ruleEvaluationContext(ruleName, kind, namespace, group string) context.Context {
	ctx := promql.NewOriginContext(context.Background(), map[string]any{
		"ruleGroup": map[string]string{
			"file": filepath.Join("/data/ruler", "user-1", url.PathEscape(namespace)),
			"name": group,
		},
	})
	return rules.NewOriginContext(ctx, rules.RuleDetail{Name: ruleName, Kind: kind})
}

func TestWithRuleDetailMiddleware(t *testing.T) {
	// "ü" is 2 bytes, so the leading "a" makes the cut land in the middle of a rune.
	multiByteName := "a" + strings.Repeat("ü", maxRuleDetailHeaderValueLen)

	tests := map[string]struct {
		ctx      context.Context
		expected map[string]string
	}{
		"alerting rule": {
			ctx: ruleEvaluationContext("HighErrorRate", rules.KindAlerting, "team-a", "service-x"),
			expected: map[string]string{
				ruleNameHeader:      "HighErrorRate",
				ruleTypeHeader:      "alerting",
				ruleSourceHeader:    "mimir-ruler",
				ruleNamespaceHeader: "team-a",
				ruleGroupHeader:     "service-x",
			},
		},
		"recording rule": {
			ctx: ruleEvaluationContext("job:requests:rate5m", rules.KindRecording, "team-a", "service-x"),
			expected: map[string]string{
				ruleNameHeader:      "job:requests:rate5m",
				ruleTypeHeader:      "recording",
				ruleSourceHeader:    "mimir-ruler",
				ruleNamespaceHeader: "team-a",
				ruleGroupHeader:     "service-x",
			},
		},
		"namespace and group containing slashes and spaces": {
			ctx: ruleEvaluationContext("HighErrorRate", rules.KindAlerting, "team a/service%x", "team-a/service-x"),
			expected: map[string]string{
				ruleNameHeader:      "HighErrorRate",
				ruleTypeHeader:      "alerting",
				ruleSourceHeader:    "mimir-ruler",
				ruleNamespaceHeader: "team a/service%x",
				ruleGroupHeader:     "team-a/service-x",
			},
		},
		"no rule or group in context, as when restoring the alerts' for state": {
			// RestoreForState queries with the rules manager context.
			// It has the tenant, but no rule and no group.
			ctx:      user.InjectOrgID(context.Background(), "user-1"),
			expected: map[string]string{},
		},
		"group in context but no rule": {
			ctx: promql.NewOriginContext(context.Background(), map[string]any{
				"ruleGroup": map[string]string{"file": "/data/ruler/user-1/team-a", "name": "service-x"},
			}),
			expected: map[string]string{},
		},
		"rule in context but no group": {
			ctx: rules.NewOriginContext(context.Background(), rules.RuleDetail{Name: "HighErrorRate", Kind: rules.KindAlerting}),
			expected: map[string]string{
				ruleNameHeader:   "HighErrorRate",
				ruleTypeHeader:   "alerting",
				ruleSourceHeader: "mimir-ruler",
			},
		},
		"control characters are stripped": {
			ctx: ruleEvaluationContext("High\r\nX-Injected: yes\x7f", rules.KindAlerting, "team\ta", "service\x00x"),
			expected: map[string]string{
				ruleNameHeader:      "HighX-Injected: yes",
				ruleTypeHeader:      "alerting",
				ruleSourceHeader:    "mimir-ruler",
				ruleNamespaceHeader: "teama",
				ruleGroupHeader:     "servicex",
			},
		},
		"values made only of control characters are omitted": {
			ctx: ruleEvaluationContext("HighErrorRate", rules.KindAlerting, "\n", "\r\n"),
			expected: map[string]string{
				ruleNameHeader:   "HighErrorRate",
				ruleTypeHeader:   "alerting",
				ruleSourceHeader: "mimir-ruler",
			},
		},
		"long values are truncated without splitting runes": {
			ctx: ruleEvaluationContext(multiByteName, rules.KindAlerting, strings.Repeat("n", 200), "service-x"),
			expected: map[string]string{
				ruleNameHeader:      "a" + strings.Repeat("ü", (maxRuleDetailHeaderValueLen-1)/2),
				ruleTypeHeader:      "alerting",
				ruleSourceHeader:    "mimir-ruler",
				ruleNamespaceHeader: strings.Repeat("n", maxRuleDetailHeaderValueLen),
				ruleGroupHeader:     "service-x",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "http://query-frontend/prometheus/api/v1/query", nil)
			require.NoError(t, err)

			require.NoError(t, WithRuleDetailMiddleware(tc.ctx, req))

			actual := map[string]string{}
			for _, h := range ruleDetailHeaders {
				require.LessOrEqual(t, len(req.Header.Values(h)), 1)
				if v := req.Header.Get(h); v != "" {
					require.True(t, utf8.ValidString(v))
					require.LessOrEqual(t, len(v), maxRuleDetailHeaderValueLen)
					actual[h] = v
				}
			}
			require.Equal(t, tc.expected, actual)
			require.Empty(t, req.Header.Get("X-Injected"))
		})
	}
}

// TestDefaultTenantManagerFactory_RuleDetailHeaders checks that the rule and rule group the
// Prometheus rules manager stores in the evaluation context reach the remote querier when rules are
// evaluated by the ruler's own manager, rather than only with a context built by the test.
func TestDefaultTenantManagerFactory_RuleDetailHeaders(t *testing.T) {
	const (
		userID    = "tenant-1"
		namespace = "team-a/service-x"
		groupName = "service-x-rules"
	)

	received := make(chan http.Header, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- r.Header.Clone():
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status": "success","data": {"resultType":"vector","result":[]}}`))
	}))
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	remoteQuerier := NewRemoteQuerier(server.Client().Transport, time.Minute, 1, formatJSON, serverURL, log.NewNopLogger(), WithOrgIDMiddleware, WithRuleDetailMiddleware)

	cfg := defaultRulerConfig(t)
	options := applyPrepareOptions(t, cfg.Ring.Common.InstanceID)
	notifierManager := notifier.NewManager(&notifier.Options{
		Do: func(context.Context, *http.Client, *http.Request) (*http.Response, error) { return nil, nil },
	}, model.UTF8Validation, util_log.SlogFromGoKit(options.logger))

	fs := afero.NewMemMapFs()
	_, files, err := newMapper(cfg.RulePath, fs, options.logger).MapRules(userID, map[string][]rulefmt.RuleGroup{
		namespace: {{
			Name: groupName,
			Rules: []rulefmt.Rule{
				{Record: "job:up:sum", Expr: "sum(up)"},
				{Alert: "InstanceDown", Expr: "up == 0"},
			},
		}},
	})
	require.NoError(t, err)

	pusher := newPusherMock()
	pusher.MockPush(&mimirpb.WriteResponse{}, nil)
	factory := DefaultTenantManagerFactory(cfg, pusher, newMockQueryable(), remoteQuerier.Query, fs, &NoopMultiTenantConcurrencyController{}, options.limits, nil)
	manager := factory(t.Context(), userID, notifierManager, options.logger, nil)
	require.NoError(t, manager.Update(time.Millisecond, files, labels.EmptyLabels(), "", nil))
	go manager.Run()
	t.Cleanup(manager.Stop)

	expected := map[string]map[string]string{
		"job:up:sum": {
			ruleNameHeader:      "job:up:sum",
			ruleTypeHeader:      "recording",
			ruleSourceHeader:    "mimir-ruler",
			ruleNamespaceHeader: namespace,
			ruleGroupHeader:     groupName,
		},
		"InstanceDown": {
			ruleNameHeader:      "InstanceDown",
			ruleTypeHeader:      "alerting",
			ruleSourceHeader:    "mimir-ruler",
			ruleNamespaceHeader: namespace,
			ruleGroupHeader:     groupName,
		},
	}

	actual := map[string]map[string]string{}
	timeout := time.After(5 * time.Second)
	for len(actual) < len(expected) {
		select {
		case headers := <-received:
			values := map[string]string{}
			for _, h := range ruleDetailHeaders {
				if v := headers.Get(h); v != "" {
					values[h] = v
				}
			}
			actual[headers.Get(ruleNameHeader)] = values
		case <-timeout:
			require.FailNow(t, "timed out waiting for rule evaluation queries", "received: %v", actual)
		}
	}
	require.Equal(t, expected, actual)
}

func TestRemoteQuerier_QueryWithRuleDetailMiddleware(t *testing.T) {
	expected := map[string]string{
		ruleNameHeader:      "HighErrorRate",
		ruleTypeHeader:      "alerting",
		ruleSourceHeader:    "mimir-ruler",
		ruleNamespaceHeader: "team-a/service-x",
		ruleGroupHeader:     "service-x",
	}
	ctx := user.InjectOrgID(ruleEvaluationContext("High\r\nErrorRate", rules.KindAlerting, "team-a/service-x", "service-x"), "user-1")
	tm := time.Unix(1649092025, 515834)

	t.Run("httpgrpc", func(t *testing.T) {
		var received *httpgrpc.HTTPRequest
		mockClientFn := func(_ context.Context, req *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
			received = req
			return &httpgrpc.HTTPResponse{
				Code:    http.StatusOK,
				Headers: []*httpgrpc.Header{{Key: "Content-Type", Values: []string{"application/json"}}},
				Body:    []byte(`{"status": "success","data": {"resultType":"vector","result":[]}}`),
			}, nil
		}
		q := NewRemoteQuerier(newGrpcRoundTripper(mockHTTPGRPCClient(mockClientFn)), time.Minute, 1, formatJSON, prometheusGrpcURL, log.NewNopLogger(), WithOrgIDMiddleware, WithRuleDetailMiddleware)

		_, err := q.Query(ctx, "up", tm)
		require.NoError(t, err)
		for name, value := range expected {
			require.Equal(t, value, getGrpcHeader(received.Headers, name), name)
		}
	})

	t.Run("http", func(t *testing.T) {
		received := make(chan http.Header, 1)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received <- r.Header.Clone()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status": "success","data": {"resultType":"vector","result":[]}}`))
		}))
		t.Cleanup(server.Close)

		serverURL, err := url.Parse(server.URL)
		require.NoError(t, err)
		q := NewRemoteQuerier(server.Client().Transport, time.Minute, 1, formatJSON, serverURL, log.NewNopLogger(), WithOrgIDMiddleware, WithRuleDetailMiddleware)

		// Without stripping control characters, the HTTP client would reject the request.
		_, err = q.Query(ctx, "up", tm)
		require.NoError(t, err)
		headers := <-received
		for name, value := range expected {
			require.Equal(t, value, headers.Get(name), name)
		}
	})
}
