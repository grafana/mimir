// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/json"
	"fmt"
	tmplhtml "html/template"
	"net/url"
	"path"
	"strings"
	tmpltext "text/template"

	"github.com/prometheus/alertmanager/asset"
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
func queryFromGeneratorURL(generatorURL string) (string, error) {
	u, err := url.Parse(generatorURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse generator URL: %w", err)
	}
	// See https://github.com/prometheus/prometheus/blob/259bb5c69263635887541964d1bfd7acc46682c6/util/strutil/strconv.go#L28
	queryParam, ok := u.Query()["g0.expr"]
	if !ok || len(queryParam) < 1 {
		return "", fmt.Errorf("query not found in the generator URL")
	}
	return queryParam[0], nil
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
// It is adapted from FromGlobs in prometheus/alertmanager: https://github.com/prometheus/alertmanager/blob/9de8ef36755298a68b6ab20244d4369d38bdea99/template/template.go#L67-L95
func loadTemplates(tmpls []string, options ...template.Option) (*template.Template, error) {
	t, err := template.New(options...)
	if err != nil {
		return nil, err
	}

	// Prometheus keeps its default templates in a virtual filesystem.
	// Ensure these are included - this does not actually hit the disk.
	defaultTemplates := []string{"default.tmpl", "email.tmpl"}

	for _, file := range defaultTemplates {
		f, err := asset.Assets.Open(path.Join("/templates", file))
		if err != nil {
			return nil, err
		}
		if err := t.Parse(f); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}

	for _, tp := range tmpls {
		if err := t.Parse(strings.NewReader(tp)); err != nil {
			return nil, err
		}
	}
	return t, nil
}
