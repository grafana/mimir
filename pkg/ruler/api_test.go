// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/api_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestRuler_ListRules(t *testing.T) {
	const (
		userID   = "user1"
		interval = time.Minute
	)

	testCases := map[string]struct {
		requestPath        string
		configuredRules    rulespb.RuleGroupList
		missingRules       rulespb.RuleGroupList
		expectedStatusCode int
		expectedRules      map[string][]rulefmt.RuleGroup
		expectedErr        string
	}{
		"should list all rule groups of an user if the namespace parameter is missing": {
			requestPath: "/prometheus/config/v1/rules",
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace2",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("COUNT_UP_RULE", "count(up)")},
					Interval:  interval,
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRules: map[string][]rulefmt.RuleGroup{
				"namespace1": {
					rulespb.FromProto(&rulespb.RuleGroupDesc{
						Name:      "group1",
						Namespace: "namespace1",
						User:      userID,
						Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
						Interval:  interval,
					}),
				},
				"namespace2": {
					rulespb.FromProto(&rulespb.RuleGroupDesc{
						Name:      "group1",
						Namespace: "namespace2",
						User:      userID,
						Rules:     []*rulespb.RuleDesc{createRecordingRule("COUNT_UP_RULE", "count(up)")},
						Interval:  interval,
					}),
				},
			},
		},
		"should list all rule groups of an user belonging to the input namespace": {
			requestPath: "/prometheus/config/v1/rules/namespace1",
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace2",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("COUNT_UP_RULE", "count(up)")},
					Interval:  interval,
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedRules: map[string][]rulefmt.RuleGroup{
				"namespace1": {
					rulespb.FromProto(&rulespb.RuleGroupDesc{
						Name:      "group1",
						Namespace: "namespace1",
						User:      userID,
						Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
						Interval:  interval,
					}),
				},
			},
		},
		"should fail if some rule groups were missing when loading them": {
			requestPath: "/prometheus/config/v1/rules",
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace2",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("COUNT_UP_RULE", "count(up)")},
					Interval:  interval,
				},
			},
			missingRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace3",
					User:      userID,
				},
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "an error occurred while loading 1 rule groups",
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

			store := newMockRuleStore(map[string]rulespb.RuleGroupList{userID: tc.configuredRules})
			store.setMissingRuleGroups(tc.missingRules)

			r := prepareRuler(t, cfg, store, withStart())
			a := NewAPI(r, r.directStore, log.NewNopLogger())

			router := mux.NewRouter()
			router.Path("/prometheus/config/v1/rules").Methods("GET").HandlerFunc(a.ListRules)
			router.Path("/prometheus/config/v1/rules/{namespace}").Methods("GET").HandlerFunc(a.ListRules)
			req := requestFor(t, http.MethodGet, "https://localhost:8080"+tc.requestPath, nil, userID)

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)
			require.Equal(t, tc.expectedStatusCode, resp.StatusCode)

			if tc.expectedStatusCode >= 200 && tc.expectedStatusCode < 300 {
				expectedYAML, err := yaml.Marshal(tc.expectedRules)
				require.NoError(t, err)
				require.YAMLEq(t, string(expectedYAML), string(body))
			} else {
				require.Contains(t, string(body), tc.expectedErr)
			}
		})

	}
}

func TestRuler_PrometheusRules(t *testing.T) {
	const (
		userID   = "user1"
		interval = time.Minute
	)

	groupName := func(group int) string {
		return fmt.Sprintf(")(_+?/|group%d+/?", group)
	}

	namespaceName := func(ns int) string {
		return fmt.Sprintf(")(_+?/|namespace%d+/?", ns)
	}

	makeFilterTestRules := func() rulespb.RuleGroupList {
		result := rulespb.RuleGroupList{}
		for ns := 1; ns <= 3; ns++ {
			for group := 1; group <= 3; group++ {
				g := &rulespb.RuleGroupDesc{
					Name:      groupName(group),
					Namespace: namespaceName(ns),
					User:      userID,
					Rules: []*rulespb.RuleDesc{
						createRecordingRule("NonUniqueNamedRule", "up"),
						createAlertingRule(fmt.Sprintf("UniqueNamedRuleN%dG%d", ns, group), "up < 1"),
					},
					Interval: interval,
				}
				result = append(result, g)
			}
		}
		return result
	}

	filterTestExpectedRule := func(name string) *recordingRule {
		return &recordingRule{
			Name:   name,
			Query:  "up",
			Health: "unknown",
			Type:   "recording",
		}
	}
	filterTestExpectedAlert := func(name string) *alertingRule {
		return &alertingRule{
			Name:   name,
			Query:  "up < 1",
			State:  "inactive",
			Health: "unknown",
			Type:   "alerting",
			Alerts: []*Alert{},
		}
	}

	testCases := map[string]struct {
		configuredRules    rulespb.RuleGroupList
		limits             RulesLimits
		expectedConfigured int
		expectedStatusCode int
		expectedErrorType  v1.ErrorType
		expectedRules      []*RuleGroup
		queryParams        string
	}{
		"should load and evaluate the configured rules": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			limits:             validation.MockDefaultOverrides(),
			expectedConfigured: 1,
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
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 1,
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
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
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 1,
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
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
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 0,
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
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
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 1,
			limits:             validation.MockDefaultOverrides(),
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
					Rules:         []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:      interval,
				},
			},
			expectedConfigured: 1,
			limits:             validation.MockDefaultOverrides(),
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
			expectedConfigured: 1,
			limits:             validation.MockDefaultOverrides(),
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
		"API returns only alerts": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 1,
			queryParams:        "?type=alert",
			limits:             validation.MockDefaultOverrides(),
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
		"API returns only rules": {
			configuredRules: rulespb.RuleGroupList{
				&rulespb.RuleGroupDesc{
					Name:      "group1",
					Namespace: "namespace1",
					User:      userID,
					Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
					Interval:  interval,
				},
			},
			expectedConfigured: 1,
			queryParams:        "?type=record",
			limits:             validation.MockDefaultOverrides(),
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
		"Invalid type param": {
			configuredRules:    rulespb.RuleGroupList{},
			expectedConfigured: 0,
			queryParams:        "?type=foo",
			limits:             validation.MockDefaultOverrides(),
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorType:  v1.ErrBadData,
			expectedRules:      []*RuleGroup{},
		},
		"when filtering by an unknown namespace then the API returns nothing": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?file=unknown",
			limits:             validation.MockDefaultOverrides(),
			expectedRules:      []*RuleGroup{},
		},
		"when filtering by a single known namespace then the API returns only rules from that namespace": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"file": []string{namespaceName(1)}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(1),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G1"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G3"),
					},
					Interval: 60,
				},
			},
		},
		"when filtering by a multiple known namespaces then the API returns rules from both namespaces": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"file": []string{namespaceName(1), namespaceName(2)}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(1),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G1"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G3"),
					},
					Interval: 60,
				},
				{
					Name: groupName(1),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G1"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G3"),
					},
					Interval: 60,
				},
			},
		},
		"when filtering by an unknown group then the API returns nothing": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?rule_group=unknown",
			limits:             validation.MockDefaultOverrides(),
			expectedRules:      []*RuleGroup{},
		},
		"when filtering by a known group then the API returns only rules from that group": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"rule_group": []string{groupName(2)}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(3),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN3G2"),
					},
					Interval: 60,
				},
			},
		},
		"when filtering by multiple known groups then the API returns rules from both groups": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"rule_group": []string{groupName(2), groupName(3)}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN1G3"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN2G3"),
					},
					Interval: 60,
				},
				{
					Name: groupName(2),
					File: namespaceName(3),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN3G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(3),
					Rules: []rule{
						filterTestExpectedRule("NonUniqueNamedRule"),
						filterTestExpectedAlert("UniqueNamedRuleN3G3"),
					},
					Interval: 60,
				},
			},
		},

		"when filtering by an unknown rule name then the API returns all empty groups": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?rule_name=unknown",
			limits:             validation.MockDefaultOverrides(),
			expectedRules:      []*RuleGroup{},
		},
		"when filtering by a known rule name then the API returns only rules with that name": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"rule_name": []string{"UniqueNamedRuleN1G2"}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
			},
		},
		"when filtering by multiple known rule names then the API returns both rules": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams:        "?" + url.Values{"rule_name": []string{"UniqueNamedRuleN1G2", "UniqueNamedRuleN2G3"}}.Encode(),
			limits:             validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(2),
					File: namespaceName(1),
					Rules: []rule{
						filterTestExpectedAlert("UniqueNamedRuleN1G2"),
					},
					Interval: 60,
				},
				{
					Name: groupName(3),
					File: namespaceName(2),
					Rules: []rule{
						filterTestExpectedAlert("UniqueNamedRuleN2G3"),
					},
					Interval: 60,
				},
			},
		},
		"when filtering by a known namespace and group then the API returns only rules from that namespace and group": {
			configuredRules:    makeFilterTestRules(),
			expectedConfigured: len(makeFilterTestRules()),
			queryParams: "?" + url.Values{
				"file":       []string{namespaceName(3)},
				"rule_group": []string{groupName(2)},
			}.Encode(),
			limits: validation.MockDefaultOverrides(),
			expectedRules: []*RuleGroup{
				{
					Name: groupName(2),
					File: namespaceName(3),
					Rules: []rule{
						&recordingRule{
							Name:   "NonUniqueNamedRule",
							Query:  "up",
							Health: "unknown",
							Type:   "recording",
						},
						&alertingRule{
							Name:   "UniqueNamedRuleN3G2",
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
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Pre-condition check: ensure all rules have the user set.
			for _, rule := range tc.configuredRules {
				assert.Equal(t, userID, rule.User)
			}

			cfg := defaultRulerConfig(t)
			cfg.TenantFederation.Enabled = true

			storageRules := map[string]rulespb.RuleGroupList{
				userID: tc.configuredRules,
			}

			r := prepareRuler(t, cfg, newMockRuleStore(storageRules), withRulerAddrAutomaticMapping(), withLimits(tc.limits), withStart())

			// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
			// has been synched.
			test.Poll(t, 5*time.Second, tc.expectedConfigured, func() interface{} {
				ctx := user.InjectOrgID(context.Background(), userID)
				rls, _ := r.Rules(ctx, &RulesRequest{})
				return len(rls.Groups)
			})

			a := NewAPI(r, r.directStore, log.NewNopLogger())

			req := requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/api/v1/rules"+tc.queryParams, nil, userID)
			w := httptest.NewRecorder()
			a.PrometheusRules(w, req)

			resp := w.Result()
			body, _ := io.ReadAll(resp.Body)
			if tc.expectedStatusCode != 0 {
				require.Equal(t, tc.expectedStatusCode, resp.StatusCode)
			} else {
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			responseJSON := response{}
			err := json.Unmarshal(body, &responseJSON)
			require.NoError(t, err)

			if tc.expectedErrorType != "" {
				assert.Equal(t, "error", responseJSON.Status)
				assert.Equal(t, tc.expectedErrorType, responseJSON.ErrorType)
				return
			}
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

func TestRuler_PrometheusRulesPagination(t *testing.T) {
	const (
		userID   = "user1"
		interval = time.Minute
	)

	ruleGroups := rulespb.RuleGroupList{}
	for ns := 0; ns < 3; ns++ {
		for group := 0; group < 3; group++ {
			g := &rulespb.RuleGroupDesc{
				Name:      fmt.Sprintf("test-group-%d", group),
				Namespace: fmt.Sprintf("test-namespace-%d", ns),
				User:      userID,
				Rules: []*rulespb.RuleDesc{
					createAlertingRule("testalertingrule", "up < 1"),
				},
				Interval: interval,
			}
			ruleGroups = append(ruleGroups, g)
		}
	}

	cfg := defaultRulerConfig(t)
	cfg.TenantFederation.Enabled = true

	storageRules := map[string]rulespb.RuleGroupList{
		userID: ruleGroups,
	}

	r := prepareRuler(t, cfg, newMockRuleStore(storageRules), withRulerAddrAutomaticMapping(), withLimits(validation.MockDefaultOverrides()), withStart())

	// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
	// has been synched.
	test.Poll(t, 5*time.Second, len(ruleGroups), func() interface{} {
		ctx := user.InjectOrgID(context.Background(), userID)
		rls, _ := r.Rules(ctx, &RulesRequest{})
		return len(rls.Groups)
	})

	a := NewAPI(r, r.directStore, log.NewNopLogger())

	getRulesResponse := func(groupSize int, nextToken string) response {
		queryParams := "?" + url.Values{
			"max_groups": []string{strconv.Itoa(groupSize)},
			"next_token": []string{nextToken},
		}.Encode()
		req := requestFor(t, http.MethodGet, "https://localhost:8080/prometheus/api/v1/rules"+queryParams, nil, userID)
		w := httptest.NewRecorder()
		a.PrometheusRules(w, req)

		resp := w.Result()
		body, _ := io.ReadAll(resp.Body)

		r := response{}
		err := json.Unmarshal(body, &r)
		require.NoError(t, err)

		return r
	}

	getRulesFromResponse := func(resp response) RuleDiscovery {
		jsonRules, err := json.Marshal(resp.Data)
		require.NoError(t, err)
		returnedRules := RuleDiscovery{}
		require.NoError(t, json.Unmarshal(jsonRules, &returnedRules))

		return returnedRules
	}

	// No page size limit
	resp := getRulesResponse(0, "")
	require.Equal(t, "success", resp.Status)
	rd := getRulesFromResponse(resp)
	require.Len(t, rd.RuleGroups, len(ruleGroups))
	require.Empty(t, rd.NextToken)

	// We have 9 groups, keep fetching rules with a group page size of 2. The final
	// page should have size 1 and an empty nextToken. Also check the groups returned
	// in order
	var nextToken string
	returnedRuleGroups := make([]*RuleGroup, 0, len(ruleGroups))
	for i := 0; i < 4; i++ {
		resp := getRulesResponse(2, nextToken)
		require.Equal(t, "success", resp.Status)

		rd := getRulesFromResponse(resp)
		require.Len(t, rd.RuleGroups, 2)
		require.NotEmpty(t, rd.NextToken)

		returnedRuleGroups = append(returnedRuleGroups, rd.RuleGroups[0], rd.RuleGroups[1])
		nextToken = rd.NextToken
	}
	resp = getRulesResponse(2, nextToken)
	require.Equal(t, "success", resp.Status)

	rd = getRulesFromResponse(resp)
	require.Len(t, rd.RuleGroups, 1)
	require.Empty(t, rd.NextToken)
	returnedRuleGroups = append(returnedRuleGroups, rd.RuleGroups[0])

	// Check the returned rules match the rules written
	require.Equal(t, len(ruleGroups), len(returnedRuleGroups))
	for i := 0; i < len(ruleGroups); i++ {
		require.Equal(t, ruleGroups[i].Namespace, returnedRuleGroups[i].File)
		require.Equal(t, ruleGroups[i].Name, returnedRuleGroups[i].Name)
		require.Equal(t, len(ruleGroups[i].Rules), len(returnedRuleGroups[i].Rules))
		for j := 0; j < len(ruleGroups[i].Rules); j++ {
			jsonRule, err := json.Marshal(returnedRuleGroups[i].Rules[j])
			require.NoError(t, err)
			rule := alertingRule{}
			require.NoError(t, json.Unmarshal(jsonRule, &rule))
			require.Equal(t, ruleGroups[i].Rules[j].Alert, rule.Name)
		}
	}

	// Invalid max groups value
	resp = getRulesResponse(-1, "")
	require.Equal(t, "error", resp.Status)
	require.Equal(t, v1.ErrBadData, resp.ErrorType)
	require.Equal(t, "invalid max groups value", resp.Error)

	// Bad token should return no groups
	resp = getRulesResponse(0, "bad-token")
	require.Equal(t, "success", resp.Status)

	rd = getRulesFromResponse(resp)
	require.Len(t, rd.RuleGroups, 0)
}

func TestRuler_PrometheusAlerts(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules), withRulerAddrAutomaticMapping())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))
	})

	// Rules will be synchronized asynchronously, so we wait until the expected number of rule groups
	// has been synched.
	test.Poll(t, 5*time.Second, len(mockRules["user1"]), func() interface{} {
		ctx := user.InjectOrgID(context.Background(), "user1")
		rls, _ := r.Rules(ctx, &RulesRequest{})
		return len(rls.Groups)
	})

	a := NewAPI(r, r.directStore, log.NewNopLogger())

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

func TestAPI_CreateRuleGroup(t *testing.T) {
	defaultCfg := defaultRulerConfig(t)

	cfgWithTenantFederation := defaultRulerConfig(t)
	cfgWithTenantFederation.TenantFederation.Enabled = true

	tc := []struct {
		name   string
		cfg    Config
		input  string
		err    error
		status int
	}{
		{
			name:   "with an empty payload",
			cfg:    defaultCfg,
			input:  "",
			status: 400,
			err:    errors.New("invalid rules configuration: rule group name must not be empty"),
		},
		{
			name: "with no rule group name",
			cfg:  defaultCfg,
			input: `
interval: 15s
rules:
- record: up_rule
  expr: up
`,
			status: 400,
			err:    errors.New("invalid rules configuration: rule group name must not be empty"),
		},
		{
			name: "with no rules",
			cfg:  defaultCfg,
			input: `
name: rg_name
interval: 15s
`,
			status: 400,
			err:    errors.New("invalid rules configuration: rule group 'rg_name' has no rules"),
		},
		{

			name:   "with federated rules without enabled federation",
			cfg:    defaultCfg,
			status: 400,
			input: `
name: test
interval: 15s
source_tenants: [t1, t2]
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
			err: errors.New("invalid rules configuration: rule group 'test' is a federated rule group, but rules federation is disabled; please contact your service administrator to have it enabled"),
		},
		{
			name:   "with valid rules with enabled federation",
			cfg:    cfgWithTenantFederation,
			status: 202,
			input: `
name: test
interval: 15s
source_tenants: [t1, t2]
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
		},
		{
			name: "with valid rules and evaluation delay",
			cfg:  defaultCfg,
			input: `
name: test
interval: 15s
evaluation_delay: 5m
rules:
- record: up_rule
  expr: up{}
`,
			status: 202,
		},
		{
			name: "with valid rules and query offset",
			cfg:  defaultCfg,
			input: `
name: test
interval: 15s
query_offset: 2m
rules:
- record: up_rule
  expr: up{}
`,
			status: 202,
		},
		{
			name: "with valid rules and both evaluation delay and query offset set to the same value",
			cfg:  defaultCfg,
			input: `
name: test
interval: 15s
evaluation_delay: 5m
query_offset: 5m
rules:
- record: up_rule
  expr: up{}
`,
			status: 202,
		},
		{
			name: "with valid rules but evaluation delay and query offset set to different values",
			cfg:  defaultCfg,
			input: `
name: test
interval: 15s
evaluation_delay: 2m
query_offset: 5m
rules:
- record: up_rule
  expr: up{}
`,
			status: 400,
			err:    errors.New("invalid rules configuration: rule group 'test' has both query_offset and (deprecated) evaluation_delay set, but to different values; please remove the deprecated evaluation_delay and use query_offset instead"),
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			// Configure the ruler to only sync the rules based on notifications upon API changes.
			rulerCfg := tt.cfg
			rulerCfg.PollInterval = time.Hour
			rulerCfg.InboundSyncQueuePollInterval = 100 * time.Millisecond
			rulerCfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond

			reg := prometheus.NewPedanticRegistry()
			r := prepareRuler(t, rulerCfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart(), withRulerAddrAutomaticMapping(), withPrometheusRegisterer(reg))
			a := NewAPI(r, r.directStore, log.NewNopLogger())

			router := mux.NewRouter()
			router.Path("/prometheus/config/v1/rules/{namespace}").Methods("POST").HandlerFunc(a.CreateRuleGroup)
			router.Path("/prometheus/config/v1/rules/{namespace}/{groupName}").Methods("GET").HandlerFunc(a.GetRuleGroup)

			// Pre-condition check: the ruler should have run the initial rules sync but not done a sync
			// based on API mutations.
			verifySyncRulesMetric(t, reg, 1, 0)

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
				require.YAMLEq(t, tt.input, w.Body.String())

				// Ensure it triggered a rules sync notification.
				verifySyncRulesMetric(t, reg, 1, 1)
			} else {
				require.Equal(t, tt.err.Error()+"\n", w.Body.String())
			}
		})
	}
}

func TestAPI_DeleteNamespace(t *testing.T) {
	// Configure the ruler to only sync the rules based on notifications upon API changes.
	cfg := defaultRulerConfig(t)
	cfg.PollInterval = time.Hour
	cfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
	cfg.InboundSyncQueuePollInterval = 100 * time.Millisecond

	// Keep this inside the test, not as global var, otherwise running tests with -count higher than 1 fails,
	// as newMockRuleStore modifies the underlying map.
	mockRulesNamespaces := map[string]rulespb.RuleGroupList{
		"user1": {
			&rulespb.RuleGroupDesc{
				Name:      "group1",
				Namespace: "namespace1",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{createRecordingRule("UP_RULE", "up"), createAlertingRule("UP_ALERT", "up < 1")},
				Interval:  interval,
			},
			&rulespb.RuleGroupDesc{
				Name:      "fail",
				Namespace: "namespace2",
				User:      "user1",
				Rules:     []*rulespb.RuleDesc{createRecordingRule("UP2_RULE", "up"), createAlertingRule("UP2_ALERT", "up < 1")},
				Interval:  interval,
			},
		},
	}

	reg := prometheus.NewPedanticRegistry()
	r := prepareRuler(t, cfg, newMockRuleStore(mockRulesNamespaces), withStart(), withRulerAddrAutomaticMapping(), withPrometheusRegisterer(reg))
	a := NewAPI(r, r.directStore, log.NewNopLogger())

	router := mux.NewRouter()
	router.Path("/prometheus/config/v1/rules/{namespace}").Methods(http.MethodDelete).HandlerFunc(a.DeleteNamespace)
	router.Path("/prometheus/config/v1/rules/{namespace}/{groupName}").Methods(http.MethodGet).HandlerFunc(a.GetRuleGroup)

	// Pre-condition check: the ruler should have run the initial rules sync.
	verifySyncRulesMetric(t, reg, 1, 0)

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

	// Ensure the namespace deletion triggered a rules sync notification.
	verifySyncRulesMetric(t, reg, 1, 1)

	// On Partial failures
	req = requestFor(t, http.MethodDelete, "https://localhost:8080/prometheus/config/v1/rules/namespace2", nil, "user1")
	w = httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.Code)
	require.Equal(t, "{\"status\":\"error\",\"data\":null,\"errorType\":\"server_error\",\"error\":\"unable to delete rg\"}", w.Body.String())
}

func TestAPI_DeleteRuleGroup(t *testing.T) {
	const userID = "user-1"

	// Configure the ruler to only sync the rules based on notifications upon API changes.
	cfg := defaultRulerConfig(t)
	cfg.PollInterval = time.Hour
	cfg.OutboundSyncQueuePollInterval = 100 * time.Millisecond
	cfg.InboundSyncQueuePollInterval = 100 * time.Millisecond

	// Keep this inside the test, not as global var, otherwise running tests with -count higher than 1 fails,
	// as newMockRuleStore modifies the underlying map.
	mockRulesNamespaces := map[string]rulespb.RuleGroupList{
		userID: {
			createRuleGroup("group-1", userID, createRecordingRule("UP_RULE", "up")),
			createRuleGroup("group-2", userID, createRecordingRule("SUM_RULE", "sum")),
		},
	}

	reg := prometheus.NewPedanticRegistry()
	r := prepareRuler(t, cfg, newMockRuleStore(mockRulesNamespaces), withStart(), withRulerAddrAutomaticMapping(), withPrometheusRegisterer(reg))
	a := NewAPI(r, r.directStore, log.NewNopLogger())

	router := mux.NewRouter()
	router.Path("/prometheus/config/v1/rules/{namespace}/{groupName}").Methods(http.MethodDelete).HandlerFunc(a.DeleteRuleGroup)

	// Pre-condition check: the ruler should have run the initial rules sync.
	verifySyncRulesMetric(t, reg, 1, 0)

	// Pre-condition check: the tenant should have 2 rule groups.
	test.Poll(t, time.Second, 2, func() interface{} {
		actualRuleGroups, err := r.GetRules(user.InjectOrgID(context.Background(), userID), RulesRequest{Filter: AnyRule})
		require.NoError(t, err)
		return len(actualRuleGroups)
	})

	// Delete group-1.
	req := requestFor(t, http.MethodDelete, "https://localhost:8080/prometheus/config/v1/rules/test/group-1", nil, userID)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	require.Equal(t, `{"status":"success","data":null,"errorType":"","error":""}`, w.Body.String())

	// Ensure the namespace deletion triggered a rules sync notification.
	verifySyncRulesMetric(t, reg, 1, 1)

	// Ensure the rule group has been deleted.
	test.Poll(t, time.Second, 1, func() interface{} {
		actualRuleGroups, err := r.GetRules(user.InjectOrgID(context.Background(), userID), RulesRequest{Filter: AnyRule})
		require.NoError(t, err)
		return len(actualRuleGroups)
	})
}

func TestRuler_LimitsPerGroup(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart(), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		defaults.RulerMaxRuleGroupsPerTenant = 1
		defaults.RulerMaxRulesPerRuleGroup = 1
	})))

	a := NewAPI(r, r.directStore, log.NewNopLogger())

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

	a := NewAPI(r, r.directStore, log.NewNopLogger())

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

func TestRuler_RulerGroupLimitsDisabled(t *testing.T) {
	cfg := defaultRulerConfig(t)

	r := prepareRuler(t, cfg, newMockRuleStore(make(map[string]rulespb.RuleGroupList)), withStart(), withLimits(validation.MockOverrides(func(defaults *validation.Limits, _ map[string]*validation.Limits) {
		defaults.RulerMaxRuleGroupsPerTenant = 0
		defaults.RulerMaxRulesPerRuleGroup = 0
	})))

	a := NewAPI(r, r.directStore, log.NewNopLogger())

	tc := []struct {
		name   string
		input  string
		output string
		err    error
		status int
	}{
		{
			name:   "when pushing the first group with disabled limit",
			status: 202,
			input: `
name: test_first_group_will_succeed
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
			output: "{\"status\":\"success\",\"data\":null,\"errorType\":\"\",\"error\":\"\"}",
		},
		{
			name:   "when pushing the second group with disabled limit",
			status: 202,
			input: `
name: test_second_group_will_also_succeed
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
			output: "{\"status\":\"success\",\"data\":null,\"errorType\":\"\",\"error\":\"\"}",
		},
	}

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

func TestAPIRoutesCorrectlyHandleInvalidOrgID(t *testing.T) {
	tcs := []struct {
		route  string
		method string
	}{
		{route: "/api/v1/rules", method: http.MethodGet},
		{route: "/api/v1/alerts", method: http.MethodGet},
		{route: "/config/v1/rules", method: http.MethodGet},
		{route: "/config/v1/rules/{namespace}", method: http.MethodGet},
		{route: "/config/v1/rules/{namespace}/{groupName}", method: http.MethodGet},
		{route: "/config/v1/rules/{namespace}", method: http.MethodPost},
		{route: "/config/v1/rules/{namespace}/{groupName}", method: http.MethodDelete},
		{route: "/config/v1/rules/{namespace}", method: http.MethodDelete},
	}

	for _, tc := range tcs {
		const invalidOrgID = ""

		cfg := defaultRulerConfig(t)
		cfg.TenantFederation.Enabled = true

		r := prepareRuler(t, cfg, newMockRuleStore(map[string]rulespb.RuleGroupList{}), withStart())

		a := NewAPI(r, r.directStore, log.NewNopLogger())

		router := mux.NewRouter()
		router.Path("/api/v1/rules").Methods(http.MethodGet).HandlerFunc(a.PrometheusRules)
		router.Path("/api/v1/alerts").Methods(http.MethodGet).HandlerFunc(a.PrometheusAlerts)
		router.Path("/config/v1/rules").Methods(http.MethodGet).HandlerFunc(a.ListRules)
		router.Path("/config/v1/rules/{namespace}").Methods(http.MethodGet).HandlerFunc(a.ListRules)
		router.Path("/config/v1/rules/{namespace}/{groupName}").Methods(http.MethodGet).HandlerFunc(a.GetRuleGroup)
		router.Path("/config/v1/rules/{namespace}").Methods(http.MethodPost).HandlerFunc(a.CreateRuleGroup)
		router.Path("/config/v1/rules/{namespace}/{groupName}").Methods(http.MethodDelete).HandlerFunc(a.DeleteRuleGroup)
		router.Path("/config/v1/rules/{namespace}").Methods(http.MethodDelete).HandlerFunc(a.DeleteNamespace)

		req := requestFor(t, tc.method, "https://localhost:8080"+tc.route, nil, invalidOrgID)

		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		resp := w.Result()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	}
}

func requestFor(t *testing.T, method string, url string, body io.Reader, userID string) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, url, body)
	ctx := user.InjectOrgID(req.Context(), userID)

	return req.WithContext(ctx)
}
