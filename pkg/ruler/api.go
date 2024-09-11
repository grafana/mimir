// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// errNoValidOrgIDFound is returned when no valid org id is found in the request context.
var errNoValidOrgIDFound = errors.New("no valid org id found")

// In order to reimplement the prometheus rules API, a large amount of code was copied over
// This is required because the prometheus api implementation does not allow us to return errors
// on rule lookups, which might fail in Mimir's case.

type response struct {
	Status    string       `json:"status"`
	Data      interface{}  `json:"data"`
	ErrorType v1.ErrorType `json:"errorType"`
	Error     string       `json:"error"`
}

// AlertDiscovery has info for all active alerts.
type AlertDiscovery struct {
	Alerts []*Alert `json:"alerts"`
}

// Alert has info for an alert.
type Alert struct {
	Labels          labels.Labels `json:"labels"`
	Annotations     labels.Labels `json:"annotations"`
	State           string        `json:"state"`
	ActiveAt        *time.Time    `json:"activeAt,omitempty"`
	KeepFiringSince *time.Time    `json:"keepFiringSince,omitempty"`
	Value           string        `json:"value"`
}

// RuleDiscovery has info for all rules
type RuleDiscovery struct {
	RuleGroups []*RuleGroup `json:"groups"`
	NextToken  string       `json:"nextToken,omitempty"`
}

// RuleGroup has info for rules which are part of a group
type RuleGroup struct {
	Name string `json:"name"`
	File string `json:"file"`
	// In order to preserve rule ordering, while exposing type (alerting or recording)
	// specific properties, both alerting and recording rules are exposed in the
	// same array.
	Rules          []rule    `json:"rules"`
	Interval       float64   `json:"interval"`
	LastEvaluation time.Time `json:"lastEvaluation"`
	EvaluationTime float64   `json:"evaluationTime"`
	SourceTenants  []string  `json:"sourceTenants"`
}

type rule interface{}

type alertingRule struct {
	// State can be "pending", "firing", "inactive".
	State          string        `json:"state"`
	Name           string        `json:"name"`
	Query          string        `json:"query"`
	Duration       float64       `json:"duration"`
	KeepFiringFor  float64       `json:"keepFiringFor"`
	Labels         labels.Labels `json:"labels"`
	Annotations    labels.Labels `json:"annotations"`
	Alerts         []*Alert      `json:"alerts"`
	Health         string        `json:"health"`
	LastError      string        `json:"lastError"`
	Type           v1.RuleType   `json:"type"`
	LastEvaluation time.Time     `json:"lastEvaluation"`
	EvaluationTime float64       `json:"evaluationTime"`
}

type recordingRule struct {
	Name           string        `json:"name"`
	Query          string        `json:"query"`
	Labels         labels.Labels `json:"labels"`
	Health         string        `json:"health"`
	LastError      string        `json:"lastError"`
	Type           v1.RuleType   `json:"type"`
	LastEvaluation time.Time     `json:"lastEvaluation"`
	EvaluationTime float64       `json:"evaluationTime"`
}

func respondError(logger log.Logger, w http.ResponseWriter, status int, errorType v1.ErrorType, msg string) {
	b, err := json.Marshal(&response{
		Status:    "error",
		ErrorType: errorType,
		Error:     msg,
		Data:      nil,
	})

	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func respondInvalidRequest(logger log.Logger, w http.ResponseWriter, msg string) {
	respondError(logger, w, http.StatusBadRequest, v1.ErrBadData, msg)
}

func respondServerError(logger log.Logger, w http.ResponseWriter, msg string) {
	respondError(logger, w, http.StatusInternalServerError, v1.ErrServer, msg)
}

// API is used to handle HTTP requests for the ruler service
type API struct {
	ruler *Ruler
	store rulestore.RuleStore

	logger log.Logger
}

// NewAPI returns a new API struct with the provided ruler and rule store
func NewAPI(r *Ruler, s rulestore.RuleStore, logger log.Logger) *API {
	return &API{
		ruler:  r,
		store:  s,
		logger: logger,
	}
}

func (a *API) PrometheusRules(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.PrometheusRules")
	defer logger.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		respondInvalidRequest(logger, w, errNoValidOrgIDFound.Error())
		return
	}

	nextToken := req.URL.Query().Get("next_token")
	var maxGroups int
	if maxGroupsVal := req.URL.Query().Get("max_groups"); maxGroupsVal != "" {
		maxGroups, err = strconv.Atoi(maxGroupsVal)
		if err != nil || maxGroups < 0 {
			respondInvalidRequest(logger, w, "invalid max groups value")
			return
		}
	}

	rulesReq := RulesRequest{
		Filter:    AnyRule,
		RuleName:  req.URL.Query()["rule_name"],
		RuleGroup: req.URL.Query()["rule_group"],
		File:      req.URL.Query()["file"],
	}

	ruleTypeFilter := strings.ToLower(req.URL.Query().Get("type"))
	if ruleTypeFilter != "" {
		switch ruleTypeFilter {
		case "alert":
			rulesReq.Filter = AlertingRule
		case "record":
			rulesReq.Filter = RecordingRule
		default:
			respondInvalidRequest(logger, w, fmt.Sprintf("not supported value %q", ruleTypeFilter))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := a.ruler.GetRules(ctx, rulesReq)

	if err != nil {
		respondServerError(logger, w, err.Error())
		return
	}

	groups := make([]*RuleGroup, 0, len(rgs))
	var newToken string
	foundToken := false
	for _, g := range rgs {
		if nextToken != "" && !foundToken {
			if nextToken != getRuleGroupNextToken(g.Group.Namespace, g.Group.Name) {
				continue
			}
			foundToken = true
		}

		if maxGroups > 0 && len(groups) == maxGroups {
			newToken = getRuleGroupNextToken(g.Group.Namespace, g.Group.Name)
			break
		}

		grp := RuleGroup{
			Name:           g.Group.Name,
			File:           g.Group.Namespace,
			Rules:          make([]rule, len(g.ActiveRules)),
			Interval:       g.Group.Interval.Seconds(),
			LastEvaluation: g.GetEvaluationTimestamp(),
			EvaluationTime: g.GetEvaluationDuration().Seconds(),
			SourceTenants:  g.Group.GetSourceTenants(),
		}

		for i, rl := range g.ActiveRules {
			if g.ActiveRules[i].Rule.Alert != "" {
				alerts := make([]*Alert, 0, len(rl.Alerts))
				for _, a := range rl.Alerts {
					alerts = append(alerts, alertStateDescToPrometheusAlert(a))
				}
				grp.Rules[i] = alertingRule{
					State:          rl.GetState(),
					Name:           rl.Rule.GetAlert(),
					Query:          rl.Rule.GetExpr(),
					Duration:       rl.Rule.For.Seconds(),
					KeepFiringFor:  rl.Rule.KeepFiringFor.Seconds(),
					Labels:         mimirpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Annotations:    mimirpb.FromLabelAdaptersToLabels(rl.Rule.Annotations),
					Alerts:         alerts,
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeAlerting,
				}
			} else {
				grp.Rules[i] = recordingRule{
					Name:           rl.Rule.GetRecord(),
					Query:          rl.Rule.GetExpr(),
					Labels:         mimirpb.FromLabelAdaptersToLabels(rl.Rule.Labels),
					Health:         rl.GetHealth(),
					LastError:      rl.GetLastError(),
					LastEvaluation: rl.GetEvaluationTimestamp(),
					EvaluationTime: rl.GetEvaluationDuration().Seconds(),
					Type:           v1.RuleTypeRecording,
				}
			}
		}

		groups = append(groups, &grp)
	}

	b, err := json.Marshal(&response{
		Status: "success",
		Data:   &RuleDiscovery{RuleGroups: groups, NextToken: newToken},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		respondServerError(logger, w, "unable to marshal the requested data")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

func getRuleGroupNextToken(file, group string) string {
	h := xxhash.New()
	_, _ = h.Write([]byte(file + ":" + group))

	return hex.EncodeToString(h.Sum(nil))
}

func (a *API) PrometheusAlerts(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.PrometheusAlerts")
	defer logger.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil || userID == "" {
		level.Error(logger).Log("msg", "error extracting org id from context", "err", err)
		respondInvalidRequest(logger, w, errNoValidOrgIDFound.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	rgs, err := a.ruler.GetRules(ctx, RulesRequest{Filter: AlertingRule})

	if err != nil {
		respondServerError(logger, w, err.Error())
		return
	}

	alerts := []*Alert{}

	for _, g := range rgs {
		for _, rl := range g.ActiveRules {
			if rl.Rule.Alert != "" {
				for _, a := range rl.Alerts {
					alerts = append(alerts, alertStateDescToPrometheusAlert(a))
				}
			}
		}
	}

	b, err := json.Marshal(&response{
		Status: "success",
		Data:   &AlertDiscovery{Alerts: alerts},
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		respondServerError(logger, w, "unable to marshal the requested data")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

var (
	// ErrNoNamespace signals that no namespace was specified in the request
	ErrNoNamespace = errors.New("a namespace must be provided in the request")
	// ErrNoGroupName signals a group name url parameter was not found
	ErrNoGroupName = errors.New("a matching group name must be provided in the request")
	// ErrNoRuleGroups signals the rule group requested does not exist
	ErrNoRuleGroups = errors.New("no rule groups found")
	// ErrBadRuleGroup is returned when the provided rule group can not be unmarshalled
	ErrBadRuleGroup = errors.New("unable to decode rule group")
)

func marshalAndSend(output interface{}, w http.ResponseWriter, logger log.Logger, headers ...http.Header) {
	d, err := yaml.Marshal(&output)
	if err != nil {
		level.Error(logger).Log("msg", "error marshalling yaml rule groups", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/yaml")
	if len(headers) > 0 {
		for _, v := range headers {
			for headerKey, headerValue := range v {
				w.Header().Set(headerKey, strings.Join(headerValue, ","))
			}
		}
	}

	if _, err := w.Write(d); err != nil {
		level.Error(logger).Log("msg", "error writing yaml response", "err", err)
		return
	}
}

func respondAccepted(w http.ResponseWriter, logger log.Logger, headers ...http.Header) {
	b, err := json.Marshal(&response{
		Status: "success",
	})
	if err != nil {
		level.Error(logger).Log("msg", "error marshaling json response", "err", err)
		respondServerError(logger, w, "unable to marshal the requested data")
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// Return a status accepted because the rule has been stored and queued for polling, but is not currently active
	w.WriteHeader(http.StatusAccepted)
	if len(headers) > 0 {
		for _, v := range headers {
			for headerKey, headerValue := range v {
				w.Header().Set(headerKey, strings.Join(headerValue, ","))
			}
		}
	}

	if n, err := w.Write(b); err != nil {
		level.Error(logger).Log("msg", "error writing response", "bytesWritten", n, "err", err)
	}
}

// parseNamespace parses the namespace from the provided set of params, in this
// api these params are derived from the url path
func parseNamespace(params map[string]string) (string, error) {
	namespace, exists := params["namespace"]
	if !exists {
		return "", ErrNoNamespace
	}

	namespace, err := url.PathUnescape(namespace)
	if err != nil {
		return "", err
	}

	return namespace, nil
}

// parseGroupName parses the group name from the provided set of params, in this
// api these params are derived from the url path
func parseGroupName(params map[string]string) (string, error) {
	groupName, exists := params["groupName"]
	if !exists {
		return "", ErrNoGroupName
	}

	groupName, err := url.PathUnescape(groupName)
	if err != nil {
		return "", err
	}

	return groupName, nil
}

// parseRequest parses the incoming request to parse out the userID, rules namespace, and rule group name
// and returns them in that order. It also allows users to require a namespace or group name and return
// an error if it they can not be parsed.
func (a *API) parseRequest(req *http.Request, requireNamespace, requireGroup bool) (string, string, string, error) {
	userID, err := tenant.TenantID(req.Context())
	if err != nil || userID == "" {
		level.Error(a.logger).Log("msg", "error extracting org id from context", "err", err)
		return "", "", "", errNoValidOrgIDFound
	}

	vars := mux.Vars(req)

	namespace, err := parseNamespace(vars)
	if err != nil {
		if !errors.Is(err, ErrNoNamespace) || requireNamespace {
			return "", "", "", err
		}
	}

	group, err := parseGroupName(vars)
	if err != nil {
		if !errors.Is(err, ErrNoGroupName) || requireGroup {
			return "", "", "", err
		}
	}

	return userID, namespace, group, nil
}

func (a *API) ListRules(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.ListRules")
	defer logger.Finish()

	userID, namespace, _, err := a.parseRequest(req, false, false)
	if err != nil {
		if errors.Is(err, errNoValidOrgIDFound) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	level.Debug(logger).Log("msg", "retrieving rule groups with namespace", "userID", userID, "namespace", namespace)
	rgs, err := a.store.ListRuleGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(rgs) == 0 {
		level.Info(logger).Log("msg", "no rule groups found", "userID", userID)
		// No rule groups, short-circuit and just return an empty map with HTTP 200
		marshalAndSend(map[string]interface{}{}, w, logger)
		return
	}

	level.Debug(logger).Log("msg", "retrieved rule groups from rule store", "userID", userID, "num_groups", len(rgs))
	missing, err := a.store.LoadRuleGroups(ctx, map[string]rulespb.RuleGroupList{userID: rgs})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(missing) > 0 {
		// This API is expected to be strongly consistent, so it's an error if any rule group was missing.
		http.Error(w, fmt.Sprintf("an error occurred while loading %d rule groups", len(missing)), http.StatusInternalServerError)
		return
	}

	numRules := 0
	protectedNamespaces := map[string]struct{}{}
	for _, rg := range rgs {
		numRules += len(rg.Rules)

		if a.ruler.IsNamespaceProtected(userID, rg.Namespace) {
			protectedNamespaces[rg.Namespace] = struct{}{}
		}
	}

	level.Debug(logger).Log("msg", "retrieved rules for rule groups from rule store", "userID", userID, "num_groups", len(rgs), "num_rules", numRules)

	formatted := rgs.Formatted()
	marshalAndSend(formatted, w, logger, ProtectedNamespacesHeaderFromSet(protectedNamespaces))
}

func (a *API) GetRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.GetRuleGroup")
	defer logger.Finish()

	userID, namespace, groupName, err := a.parseRequest(req, true, true)
	if err != nil {
		if errors.Is(err, errNoValidOrgIDFound) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	rg, err := a.store.GetRuleGroup(ctx, userID, namespace, groupName)
	if err != nil {
		if errors.Is(err, rulestore.ErrGroupNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var header http.Header
	if a.ruler.IsNamespaceProtected(userID, namespace) {
		header = ProtectedNamespacesHeaderFromString(namespace)
	}

	formatted := rulespb.FromProto(rg)
	marshalAndSend(formatted, w, logger, header)
}

func (a *API) CreateRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.CreateRuleGroup")
	defer logger.Finish()

	userID, namespace, _, err := a.parseRequest(req, true, false)
	if err != nil {
		if errors.Is(err, errNoValidOrgIDFound) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	if a.ruler.IsNamespaceProtected(userID, namespace) {
		if err = AllowProtectionOverride(req.Header, namespace); err != nil {
			level.Warn(logger).Log("msg", "not allowed to create rule group under namespace", "err", err.Error())
			http.Error(w, "namespace is protected, no modification allowed", http.StatusForbidden)
			return
		}
	}

	payload, err := io.ReadAll(req.Body)
	if err != nil {
		level.Error(logger).Log("msg", "unable to read rule group payload", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Debug(logger).Log("msg", "attempting to unmarshal rulegroup", "userID", userID, "group", string(payload))

	rg := rulefmt.RuleGroup{}
	err = yaml.Unmarshal(payload, &rg)
	if err != nil {
		level.Error(logger).Log("msg", "unable to unmarshal rule group payload", "err", err.Error())
		http.Error(w, ErrBadRuleGroup.Error(), http.StatusBadRequest)
		return
	}

	errs := a.ruler.manager.ValidateRuleGroup(rg)
	if len(errs) > 0 {
		e := []string{}
		for _, err := range errs {
			level.Error(logger).Log("msg", "unable to validate rule group payload", "err", err.Error())
			e = append(e, err.Error())
		}

		http.Error(w, strings.Join(e, ", "), http.StatusBadRequest)
		return
	}

	if err := a.ruler.AssertMaxRulesPerRuleGroup(userID, namespace, len(rg.Rules)); err != nil {
		level.Error(logger).Log("msg", "limit validation failure", "err", err.Error(), "user", userID)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Only list rule groups when enforcing a max number of groups for this tenant and namespace.
	if a.ruler.IsMaxRuleGroupsLimited(userID, namespace) {
		rgs, err := a.store.ListRuleGroupsForUserAndNamespace(ctx, userID, "")
		if err != nil {
			level.Error(logger).Log("msg", "unable to fetch current rule groups for validation", "err", err.Error(), "user", userID)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := a.ruler.AssertMaxRuleGroups(userID, namespace, len(rgs)+1); err != nil {
			level.Error(logger).Log("msg", "limit validation failure", "err", err.Error(), "user", userID)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	rgProto := rulespb.ToProto(userID, namespace, rg)

	level.Debug(logger).Log("msg", "attempting to store rulegroup", "userID", userID, "group", rgProto.String())
	err = a.store.SetRuleGroup(ctx, userID, namespace, rgProto)
	if err != nil {
		level.Error(logger).Log("msg", "unable to store rule group", "err", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.ruler.NotifySyncRulesAsync(userID)

	respondAccepted(w, logger)
}

func (a *API) DeleteNamespace(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.DeleteNamespace")
	defer logger.Finish()

	userID, namespace, _, err := a.parseRequest(req, true, false)
	if err != nil {
		if errors.Is(err, errNoValidOrgIDFound) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	if a.ruler.IsNamespaceProtected(userID, namespace) {
		if err = AllowProtectionOverride(req.Header, namespace); err != nil {
			level.Warn(logger).Log("msg", "not allowed to delete namespace", "err", err.Error())
			http.Error(w, "namespace is protected, no modification allowed", http.StatusForbidden)
			return
		}
	}

	err = a.store.DeleteNamespace(ctx, userID, namespace)
	if err != nil {
		if errors.Is(err, rulestore.ErrGroupNamespaceNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	a.ruler.NotifySyncRulesAsync(userID)

	respondAccepted(w, logger)
}

func (a *API) DeleteRuleGroup(w http.ResponseWriter, req *http.Request) {
	logger, ctx := spanlogger.NewWithLogger(req.Context(), a.logger, "API.DeleteRuleGroup")
	defer logger.Finish()

	userID, namespace, groupName, err := a.parseRequest(req, true, true)
	if err != nil {
		if errors.Is(err, errNoValidOrgIDFound) {
			respondInvalidRequest(logger, w, err.Error())
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	if a.ruler.IsNamespaceProtected(userID, namespace) {
		if err = AllowProtectionOverride(req.Header, namespace); err != nil {
			level.Warn(logger).Log("msg", "not allowed to delete rule group under namespace", "err", err.Error())
			http.Error(w, "namespace is protected, no modification allowed", http.StatusForbidden)
			return
		}
	}

	err = a.store.DeleteRuleGroup(ctx, userID, namespace, groupName)
	if err != nil {
		if errors.Is(err, rulestore.ErrGroupNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		respondServerError(logger, w, err.Error())
		return
	}

	a.ruler.NotifySyncRulesAsync(userID)

	respondAccepted(w, logger)
}

// alertStateDescToPrometheusAlert converts AlertStateDesc to Alert. The returned data structure is suitable
// to be exported by the user-facing API.
func alertStateDescToPrometheusAlert(d *AlertStateDesc) *Alert {
	a := &Alert{
		Labels:      mimirpb.FromLabelAdaptersToLabels(d.Labels),
		Annotations: mimirpb.FromLabelAdaptersToLabels(d.Annotations),
		State:       d.GetState(),
		ActiveAt:    &d.ActiveAt,
		Value:       strconv.FormatFloat(d.Value, 'e', -1, 64),
	}

	if !d.KeepFiringSince.IsZero() {
		a.KeepFiringSince = &d.KeepFiringSince
	}

	return a
}
