// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/json"
	tmplhtml "html/template"
	"net/url"
	tmpltext "text/template"

	"github.com/prometheus/alertmanager/template"
)

type grafanaExploreRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type grafanaExploreQuery struct {
	Datasource string `json:"datasource"`
	Expr       string `json:"expr"`
	Instant    bool   `json:"instant"`
	Range      bool   `json:"range"`
	RefID      string `json:"refId"`
}

type grafanaExploreParams struct {
	Datasource string                `json:"datasource"`
	Range      grafanaExploreRange   `json:"range"`
	Queries    []grafanaExploreQuery `json:"queries"`
}

// grafanaURL is a template helper function to generate Grafana explore URL in the alert template.
func grafanaURL(grafanaURL, datasource, from, to, expr string) (string, error) {
	res, err := json.Marshal(&grafanaExploreParams{
		Datasource: datasource,
		Range: grafanaExploreRange{
			From: from,
			To:   to,
		},
		Queries: []grafanaExploreQuery{
			{
				Datasource: datasource,
				Expr:       expr,
				Instant:    false,
				Range:      true,
				RefID:      "A",
			},
		},
	})
	return grafanaURL + "/explore?left=" + url.QueryEscape(string(res)), err
}

// withCustomFunctions returns template.Option which adds additional template functions
// to the default ones.
func withCustomFunctions(userID string) template.Option {
	funcs := tmpltext.FuncMap{
		"tenantID":          func() string { return userID },
		"grafanaExploreURL": grafanaURL,
	}
	return func(text *tmpltext.Template, html *tmplhtml.Template) {
		text.Funcs(funcs)
		html.Funcs(funcs)
	}
}
