// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/alertmanager/blob/main/cli/template_render.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.
package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/alertmanager/template"

	mimiram "github.com/grafana/mimir/pkg/alertmanager"
)

var defaultData = template.Data{
	Receiver: "receiver",
	Status:   "alertstatus",
	Alerts: template.Alerts{
		template.Alert{
			Status: "alertstatus",
			Labels: template.KV{
				"label1":          "value1",
				"label2":          "value2",
				"instance":        "foo.bar:1234",
				"commonlabelkey1": "commonlabelvalue1",
				"commonlabelkey2": "commonlabelvalue2",
			},
			Annotations: template.KV{
				"annotation1":          "value1",
				"annotation2":          "value2",
				"commonannotationkey1": "commonannotationvalue1",
				"commonannotationkey2": "commonannotationvalue2",
			},
			StartsAt:     time.Now().Add(-5 * time.Minute),
			EndsAt:       time.Now(),
			GeneratorURL: "https://example.com",
			Fingerprint:  "fingerprint1",
		},
		template.Alert{
			Status: "alertstatus",
			Labels: template.KV{
				"foo":             "bar",
				"baz":             "qux",
				"commonlabelkey1": "commonlabelvalue1",
				"commonlabelkey2": "commonlabelvalue2",
			},
			Annotations: template.KV{
				"aaa":                  "bbb",
				"ccc":                  "ddd",
				"commonannotationkey1": "commonannotationvalue1",
				"commonannotationkey2": "commonannotationvalue2",
			},
			StartsAt:     time.Now().Add(-10 * time.Minute),
			EndsAt:       time.Now(),
			GeneratorURL: "https://example.com",
			Fingerprint:  "fingerprint2",
		},
	},
	GroupLabels: template.KV{
		"grouplabelkey1": "grouplabelvalue1",
		"grouplabelkey2": "grouplabelvalue2",
	},
	CommonLabels: template.KV{
		"alertname":       "AlertNameExample",
		"customer":        "testing_purpose",
		"environment":     "lab",
		"commonlabelkey1": "commonlabelvalue1",
		"commonlabelkey2": "commonlabelvalue2",
	},
	CommonAnnotations: template.KV{
		"commonannotationkey1": "commonannotationvalue1",
		"commonannotationkey2": "commonannotationvalue2",
	},
	ExternalURL: "https://example.com",
}

// TemplateRenderCmd Render a given definition in a template file to standard output
type TemplateRenderCmd struct {
	TemplateFilesGlobs []string
	TemplateType       string
	TemplateText       string
	TemplateData       *os.File
	TenantID           string // Needed in the function WithCustomFunctions
}

func (cmd *TemplateRenderCmd) render(_ *kingpin.ParseContext) error {
	rendered, err := TemplateRender(cmd)
	if err != nil {
		return err
	}
	fmt.Print(rendered)
	return nil
}

func TemplateRender(cmd *TemplateRenderCmd) (string, error) {
	tmpl, err := template.FromGlobs(cmd.TemplateFilesGlobs, mimiram.WithCustomFunctions(cmd.TenantID))
	if err != nil {
		return "", err
	}

	f := tmpl.ExecuteTextString
	if cmd.TemplateType == "html" {
		f = tmpl.ExecuteHTMLString
	}

	var data template.Data
	if cmd.TemplateData == nil {
		data = defaultData
	} else {
		content, err := io.ReadAll(cmd.TemplateData)
		if err != nil {
			return "", err
		}
		if err := json.Unmarshal(content, &data); err != nil {
			return "", err
		}
	}

	rendered, err := f(cmd.TemplateText, data)
	if err != nil {
		return "", err
	}
	return rendered, nil
}
