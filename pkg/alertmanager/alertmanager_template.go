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

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
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
	res, err := json.Marshal(&grafanaExploreParams{
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
	})
	return grafanaURL + "/explore?left=" + url.QueryEscape(string(res)), err
}

// queryFromGeneratorURL returns a PromQL expression parsed out from a GeneratorURL in Prometheus alert
// the GeneratorURL can either be in Prometheus' /query format or Grafana's /explore format
func queryFromGeneratorURL(generatorURL string) (string, error) {
	u, err := url.Parse(generatorURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse generator URL: %w", err)
	}
	queryValues := u.Query()
	// See https://github.com/prometheus/prometheus/blob/259bb5c69263635887541964d1bfd7acc46682c6/util/strutil/strconv.go#L28
	queryParam, ok := queryValues["g0.expr"]
	if ok && len(queryParam) > 0 {
		return queryParam[0], nil
	}
	// If there was no match, try Loki-ruler style explore URL
	// See https://github.com/grafana/loki/blob/3868af26763bd6a836b9163dfd302d46ac72acb8/pkg/ruler/base/ruler.go#L399
	queryParam, ok = queryValues["left"]
	if !ok || len(queryParam) < 1 {
		return "", fmt.Errorf(`query not found in the generator URL, no "g0.expr" or "left" parameter`)
	}
	type query struct {
		Expr string `json:"expr"`
	}
	type response struct {
		Queries []query `json:"queries"`
	}
	var resp response
	err = json.Unmarshal([]byte(queryParam[0]), &resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse generator URL query: %w", err)
	}
	if len(resp.Queries) == 0 || len(resp.Queries[0].Expr) == 0 {
		return "", fmt.Errorf("query not found in the generator URL")
	}
	return resp.Queries[0].Expr, nil
}

// WithCustomFunctions returns template.Option which adds additional template functions
// to the default ones.
func WithCustomFunctions(userID string) template.Option {
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

// loadTemplates produces a template.Template from several in-memory template files.
func loadTemplates(tmpls []*alertspb.TemplateDesc, options ...template.Option) (*template.Template, error) {
	t, err := template.FromGlobs([]string{}, options...)
	if err != nil {
		return nil, err
	}

	for _, tp := range tmpls {
		if err := t.Parse(strings.NewReader(tp.Body)); err != nil {
			return nil, err
		}
	}
	return t, nil
}
