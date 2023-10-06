// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/json"
	"fmt"
	tmplhtml "html/template"
	"net/url"
	"strings"
	tmpltext "text/template"

	"github.com/prometheus/alertmanager/template"
)

type grafanaDatasource struct {
	Type string `json:"type,omitempty"`
	UID  string `json:"uid,omitempty"`
}

type grafanaExploreRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type grafanaExploreQuery struct {
	Datasource grafanaDatasource `json:"datasource"`
	Expr       string            `json:"expr"`
	Instant    bool              `json:"instant"`
	Range      bool              `json:"range"`
	RefID      string            `json:"refId"`
}

type grafanaExploreParams struct {
	Range   grafanaExploreRange   `json:"range"`
	Queries []grafanaExploreQuery `json:"queries"`
}

// grafanaExploreURL is a template helper function to generate Grafana range query explore URL in the alert template.
func grafanaExploreURL(grafanaURL, datasource, from, to, expr string) (string, error) {
	grafanaExploreQueryPrefix := "/explore?left="
	var res []byte
	var err error
	var grafanaExplore grafanaExploreParams

	if strings.HasPrefix(expr, grafanaExploreQueryPrefix) {
		// remove begging to get queries params
		expr = strings.ReplaceAll(expr, grafanaExploreQueryPrefix, "")
		if err = json.Unmarshal([]byte(expr), &grafanaExplore); err != nil {
			return "", err
		}
		grafanaExplore.Queries[0].Range = true
		grafanaExplore.Queries[0].RefID = "A"
		grafanaExplore.Range.From = from
		grafanaExplore.Range.To = to
	} else {
		grafanaExplore = grafanaExploreParams{
			Range: grafanaExploreRange{
				From: from,
				To:   to,
			},
			Queries: []grafanaExploreQuery{
				{
					Datasource: grafanaDatasource{
						Type: "prometheus",
						UID:  datasource,
					},
					Expr:    expr,
					Instant: false,
					Range:   true,
					RefID:   "A",
				},
			},
		}
	}
	res, err = json.Marshal(grafanaExplore)
	return grafanaURL + grafanaExploreQueryPrefix + url.QueryEscape(string(res)), err
}

// queryFromGeneratorURL returns a PromQL expression parsed out from a GeneratorURL in Prometheus alert
func queryFromGeneratorURL(generatorURL string) (string, error) {
	// if generator url source is a grafana product
	if strings.HasPrefix(generatorURL, "/explore?left=") {
		return generatorURL, nil
	}
	u, err := url.Parse(generatorURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse generator URL: %w", err)
	}
	// See https://github.com/prometheus/prometheus/blob/259bb5c69263635887541964d1bfd7acc46682c6/util/strutil/strconv.go#L28
	queryParam, ok := u.Query()["g0.expr"]
	if !ok || len(queryParam) < 1 {
		return "", fmt.Errorf("query not found in the generator URL")
	}
	query, err := url.QueryUnescape(queryParam[0])
	if err != nil {
		return "", fmt.Errorf("failed to URL decode the query: %w", err)
	}
	return query, nil
}

// withCustomFunctions returns template.Option which adds additional template functions
// to the default ones.
func withCustomFunctions(userID string) template.Option {
	funcs := tmpltext.FuncMap{
		"tenantID":              func() string { return userID },
		"grafanaExploreURL":     grafanaExploreURL,
		"queryFromGeneratorURL": queryFromGeneratorURL,
	}
	return func(text *tmpltext.Template, html *tmplhtml.Template) {
		text.Funcs(funcs)
		html.Funcs(funcs)
	}
}
