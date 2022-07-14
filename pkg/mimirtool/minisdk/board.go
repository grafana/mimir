// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/board.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016 Alexander I.Grafov <grafov@gmail.com>.
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk

type (
	// Board represents Grafana dashboard.
	Board struct {
		ID          uint       `json:"id,omitempty"`
		UID         string     `json:"uid,omitempty"`
		Slug        string     `json:"slug"`
		Title       string     `json:"title"`
		Tags        []string   `json:"tags"`
		Panels      []*Panel   `json:"panels"`
		Rows        []*Row     `json:"rows"`
		Templating  Templating `json:"templating"`
		Annotations struct {
			List []Annotation `json:"list"`
		} `json:"annotations"`
	}
	Templating struct {
		List []TemplateVar `json:"list"`
	}
	TemplateVar struct {
		Name       string         `json:"name"`
		Type       string         `json:"type"`
		Datasource *DatasourceRef `json:"datasource"`
		Refresh    BoolInt        `json:"refresh"`
		Query      interface{}    `json:"query"`
	}
	Annotation struct {
		Name       string         `json:"name"`
		Datasource *DatasourceRef `json:"datasource"`
		Query      string         `json:"query"`
		Expr       string         `json:"expr"`
		Type       string         `json:"type"`
	}
)
