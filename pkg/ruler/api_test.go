// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/api_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestRuler_PrometheusRules(t *testing.T) {
	const (
		userID   = "user1"
		interval = time.Minute
	)

	testCases := map[string]struct {
		configuredRules rulespb.RuleGroupList
		limits          RulesLimits
		expectedRules   []*RuleGroup
	}{
		"should load and evaluate the configured rules": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits: validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: "group1",
					File: "namespace1",
					Rules: []rule{
						&recordingRule{
							Name:   "UP_RULE",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
						&alertingRule{
							Name:   "UP_ALERT",
							Query:  "up < 1",
							State:  "inactive",
							Health: "unknown",
							Type:   "alerting",
							Alerts: []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
		"should load and evaluate only recording rules if alerting rules evaluation is disabled for the tenant": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits[userID] = validation.MockDefaultLimits()
				tenantLimits[userID].RulerRecordingRulesEvaluationEnabled = true
				tenantLimits[userID].RulerAlertingRulesEvaluationEnabled = false
			}),
			expectedRules: []*RuleGroup{
				{
					Name: "group1",
					File: "namespace1",
					Rules: []rule{
						&recordingRule{
							Name:   "UP_RULE",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
					},
					Interval: 60,
				},
			},
		},
		"should load and evaluate only alerting rules if recording rules evaluation is disabled for the tenant": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits[userID] = validation.MockDefaultLimits()
				tenantLimits[userID].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits[userID].RulerAlertingRulesEvaluationEnabled = true
			}),
			expectedRules: []*RuleGroup{
				{
					Name: "group1",
					File: "namespace1",
					Rules: []rule{
						&alertingRule{
							Name:   "UP_ALERT",
							Query:  "up < 1",
							State:  "inactive",
							Health: "unknown",
							Type:   "alerting",
							Alerts: []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
		"should load and evaluate no rules if rules evaluation is disabled for the tenant": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits: validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits[userID] = validation.MockDefaultLimits()
				tenantLimits[userID].RulerRecordingRulesEvaluationEnabled = false
				tenantLimits[userID].RulerAlertingRulesEvaluationEnabled = false
			}),
			expectedRules: []*RuleGroup{},
		},
		"should load and evaluate the configured rules with special characters": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      ")(_+?/|group1+/?",
					Namespace: ")(_+?/|namespace1+/?",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits: validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: ")(_+?/|group1+/?",
					File: ")(_+?/|namespace1+/?",
					Rules: []rule{
						&recordingRule{
							Name:   "UP_RULE",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
						&alertingRule{
							Name:   "UP_ALERT",
							Query:  "up < 1",
							State:  "inactive",
							Health: "unknown",
							Type:   "alerting",
							Alerts: []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
		"should support federated rules": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:          "group1",
					Namespace:     "namespace1",
					User:          userID,
					SourceTenants: []string{"tenant-1"},
					Rules:         []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
					Interval:      interval,
				},
			},
			limits: validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name:          "group1",
					File:          "namespace1",
					SourceTenants: []string{"tenant-1"},
					Rules: []rule{
						&recordingRule{
							Name:   "UP_RULE",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
						&alertingRule{
							Name:   "UP_ALERT",
							Query:  "up < 1",
							State:  "inactive",
							Health: "unknown",
							Type:   "alerting",
							Alerts: []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
		"should load alerting rules with keep_firing_for": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules: []*rulespb.RuleDesc{{
						Alert:         "UP_ALERT_WITH_KEEP_FIRING_FOR",
						Expr:          "up < 1",
						For:           time.Minute,
						KeepFiringFor: 2 * time.Minute,
					}},
					Interval: interval,
				},
			},
			limits: validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: "group1",
					File: "namespace1",
					Rules: []rule{
						&alertingRule{
							Name:          "UP_ALERT_WITH_KEEP_FIRING_FOR",
							Query:         "up < 1",
							State:         "inactive",
							Health:        "unknown",
							Type:          "alerting",
							Duration:      time.Minute.Seconds(),
							KeepFiringFor: (2 * time.Minute).Seconds(),
							Alerts:        []*Alert{},
						},
					},
					Interval: 60,
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Pre-condition check: ensure all rules have the user set.
			for _, rule := range tc.configuredRules {
				assert.Equal(t, userID, rule.User)
			}

			cfg := defaultRulerConfig(t)
			cfg.TenantFederation.Enabled = true

			rulerAddrMap := map[string]*Ruler{}

			storageRules := map[string]rulespb.RuleGroupList{
				userID: tc.configuredRules,
			}

			r := prepareRuler(t, cfg, newMockRuleStore(storageRules), withRulerAddrMap(rulerAddrMap), withLimits(tc.limits), withStart())

			// Make sure mock grpc client can find this instance, based on instance address registered in the ring.
			rulerAddrMap[r.lifecycler.GetInstanceAddr()] = r

			// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
			// has been synched.
			test.Poll(t, 5*time.Second, len(tc.expectedRules), func() interface{} {
				ctx := user.InjectOrgID(context.Background(), userID)
				rls, _ := r.Rules(ctx, &RulesRequest{})
				return len(rls.Groups)
			})

			a := NewAPI(r, r.store, log.NewNopLogger())

			req := requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/api/v1/rules", nil, userID)
			w := httptest.NewRecorder()
			a.PrometheusRules(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)

			// Check status code and status response
			responseJSON := response{}
			err := json.Unmarshal(body, &responseJSON)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, responseJSON.Status, "success")

			// Testing the running rules
			expectedResponse, err := json.Marshal(response{
				Status: "success",
				Data: &RuleDiscovery{
					RuleGroups: tc.expectedRules,
				},
			})

			require.NoError(t, err)
			require.Equal(t, string(expectedResponse), string(body))
		})

	}
}

func TestRuler_PrometheusAlerts(t *testing.T) {
	cfg := defaultRulerConfig(t)

	rulerAddrMap := map[string]*Ruler{}

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules), withRulerAddrMap(rulerAddrMap))
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))
	})

	// Make sure mock grpc client can find this instance, based on instance address registered in the ring.
	rulerAddrMap[r.lifecycler.GetInstanceAddr()] = r

	// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
	// has been synched.
	test.Poll(t, 5*time.Second, len(mockRules["user1"]), func() interface{} {
		ctx := user.InjectOrgID(context.Background(), "user1")
		rls, _ := r.Rules(ctx, &RulesRequest{})
		return len(rls.Groups)
	})

	a := NewAPI(r, r.store, log.NewNopLogger())

	req := requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/api/v1/alerts", nil, "user1")
	w := httptest.NewRecorder()
	a.PrometheusAlerts(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	// Check status code and status response
	responseJSON := response{}
	err := json.Unmarshal(body, &responseJSON)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, responseJSON.Status, "success")

	// Currently there is not an easy way to mock firing alerts. The empty
	// response case is tested instead.
	expectedResponse, _ := json.Marshal(response{
		Status: "success",
		Data: &AlertDiscovery{
			Alerts: []*Alert{},
		},
	})

	require.Equal(t, string(expectedResponse), string(body))
}

func TestRuler_Create(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart())
	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "with an empty payload",
			input:  "",
			status: 400,
			err:    errors.New("invalid rules config: rule group name must not be empty"),
		},
		{
			name: "with no rule group name",
			input: `
interval: 15s
rules:
- record: up_rule
  expr: up
`,
			status: 400,
			err:    errors.New("invalid rules config: rule group name must not be empty"),
		},
		{
			name: "with no rules",
			input: `
name: rg_name
interval: 15s
`,
			status: 400,
			err:    errors.New("invalid rules config: rule group 'rg_name' has no rules"),
		},
		{
			name:   "with a a valid rules file",
			status: 202,
			input: `
name: test
interval: 15s
rules:
- record: up_rule
  expr: up{}
- alert: up_alert
  expr: sum(up{}) > 1
  for: 30s
  annotations:
    test: test
  labels:
    test: test
`,
			output: "name: test\ninterval: 15s\nrules:\n    - record: up_rule\n      expr: up{}\n    - alert: up_alert\n      expr: sum(up{}) > 1\n      for: 30s\n      labels:\n        test: test\n      annotations:\n        test: test\n",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			router.Path("/prometheus/config/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)
			router.Path("/prometheus/config/v1/rules/{namespace}/{groupName}").Methods("GET").HandlerFunc(a.GetRuleGroup)
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/prometheus/config/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)

			if tt.err == nil {
				// GET
				req = requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/config/v1/rules/namespace/test", nil, "user1")
				w = httptest.NewRecorder()

				router.ServeHTTP(w, req)
				require.Equal(t, 200, w.Code)
				require.Equal(t, tt.output, w.Body.String())
			} else {
				require.Equal(t, tt.err.Error()+"\n", w.Body.String())
			}
		})
	}
}

func TestRuler_DeleteNamespace(t *testing.T) {
	cfg := defaultRulerConfig(t)

	// Keep this inside the test, not as global var, otherwise running tests with -count higher than 1 fails,
	// as newMockRuleStore modifies the underlying map.
	mockRulesNamespaces := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP_RULE", "up"), mockAlertingRuleDesc("UP_ALERT", "up < 1")},
				Interval:  interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "fail",
				Namespace: "namespace2",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{mockRecordingRuleDesc("UP2_RULE", "up"), mockAlertingRuleDesc("UP2_ALERT", "up < 1")},
				Interval:  interval,
			},
		},
	}

	r := prepareRuler(t, cfg, newMockRuleStore(mockRulesNamespaces), withStart())
	a := NewAPI(r, r.store, log.NewNopLogger())

	router := mux.NewRouter()
	router.Path("/prometheus/config/v1/rules/{namespace}").Methods(http.MethodDelete).HandlerFunc(a.DeleteNamespace)
	router.Path("/prometheus/config/v1/rules/{namespace}/{groupName}").Methods(http.MethodGet).HandlerFunc(a.GetRuleGroup)

	// Verify namespace1 rules are there.
	req := requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/config/v1/rules/namespace1/group1", nil, "user1")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "name: group1\ninterval: 1m\nrules:\n    - record: UP_RULE\n      expr: up\n    - alert: UP_ALERT\n      expr: up < 1\n", w.Body.String())

	// Delete namespace1
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/prometheus/config/v1/rules/namespace1", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, "{\"status\":\"success\",\"data\":null,\"errorType\":\"\",\"error\":\"\"}", w.Body.String())

	// On Partial failures
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/prometheus/config/v1/rules/namespace2", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "{\"status\":\"error\",\"data\":null,\"errorType\":\"server_error\",\"error\":\"unable to delete rg\"}", w.Body.String())
}

func TestRuler_LimitsPerGroup(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart(), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		defaults.RulerMaxRuleGroupsPerTenant = 1
		defaults.RulerMaxRulesPerRuleGroup = 1
	})))

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when exceeding the rules per rule group limit",
			status: 400,
			input: `
name: test
interval: 15s
rules:
- record: up_rule
  expr: up{}
- alert: up_alert
  expr: sum(up{}) > 1
  for: 30s
  annotations:
    test: test
  labels:
    test: test
`,
			output: "per-user rules per rule group limit (limit: 1 actual: 2) exceeded\n",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter()
			router.Path("/prometheus/config/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/prometheus/config/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)
			require.Equal(t, tt.output, w.Body.String())
		})
	}
}

func TestRuler_RulerGroupLimits(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart(), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		defaults.RulerMaxRuleGroupsPerTenant = 1
		defaults.RulerMaxRulesPerRuleGroup = 1
	})))

	a := NewAPI(r, r.store, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when pushing the first group within bounds of the limit",
			status: 202,
			input: `
name: test_first_group_will_succeed
interval: 15s
rules:
- record: up_rule
  expr: up{}
`,
			output: "{\"status\":\"success\",\"data\":null,\"errorType\":\"\",\"error\":\"\"}",
		},
		{
			name:   "when exceeding the rule group limit after sending the first group",
			status: 400,
			input: `
name: test_second_group_will_fail
interval: 15s
rules:
- record: up_rule
  expr: up{}
`,
			output: "per-user rule groups limit (limit: 1 actual: 2) exceeded\n",
		},
	}

	// define once so the requests build on each other so the number of rules can be tested
	router := mux.NewRouter()
	router.Path("/prometheus/config/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			// POST
			req := requestFor(t, http.MethodPost, "https://localhost:8080/prometheus/config/v1/rules/namespace", strings.NewReader(tt.input), "user1")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)
			require.Equal(t, tt.status, w.Code)
			require.Equal(t, tt.output, w.Body.String())
		})
	}
}

func TestAlertStateDescToPrometheusAlert(t *testing.T) {
	t.Run("should not export KeepFiringSince if it's the zero value", func(t *testing.T) {
		actual := alertStateDescToPrometheusAlert(&AlertStateDesc{})
		assert.Nil(t, actual.KeepFiringSince)
	})

	t.Run("should export KeepFiringSince if it's not the zero value", func(t *testing.T) {
		ts := time.Now()
		actual := alertStateDescToPrometheusAlert(&AlertStateDesc{KeepFiringSince: ts})
		require.NotNil(t, actual.KeepFiringSince)
		assert.Equal(t, ts, *actual.KeepFiringSince)
	})
}

func requestFor(t *testing.T, method string, url string, body io.Reader, userID string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, url, body)
	ctx := user.InjectOrgID(req.Context(), userID)

	return req.WithContext(ctx)
}
