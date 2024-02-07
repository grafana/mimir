// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"github.com/alecthomas/kingpin/v2"
)

type TemplateCommand struct{}

func (cmd *TemplateCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	templateCmd := app.Command("template", "Render template files.")
	trCmd := &TemplateRenderCmd{}
	renderCmd := templateCmd.Command("render", "Render a given definition in a template file to standard output.").Action(trCmd.render)
	renderCmd.Flag("template.glob", "Glob of paths that will be expanded and used for rendering.").Required().StringsVar(&trCmd.templateFilesGlobs)
	renderCmd.Flag("template.text", "The template that will be rendered.").Required().StringVar(&trCmd.templateText)
	renderCmd.Flag("template.type", "The type of the template. Can be either text (default) or html.").EnumVar(&trCmd.templateType, "html", "text")
	renderCmd.Flag("template.data", "Full path to a file which contains the data of the alert(-s) with which the --template.text will be rendered. Must be in JSON. File must be formatted according to the following layout: https://pkg.go.dev/github.com/prometheus/alertmanager/template#Data. If none has been specified then a predefined, simple alert will be used for rendering.").FileVar(&trCmd.templateData)
	renderCmd.Flag("id", "Basic auth username to use when contacting Prometheus or Grafana Mimir, also set as tenant ID; alternatively, set "+envVars.TenantID+".").
		Envar(envVars.TenantID).
		Default("").
		StringVar(&trCmd.tenantID)
}
