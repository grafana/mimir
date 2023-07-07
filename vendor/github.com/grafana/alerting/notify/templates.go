package notify

import (
	"bytes"
	"context"
	tmplhtml "html/template"
	"path/filepath"
	tmpltext "text/template"

	"github.com/grafana/alerting/templates"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/common/model"
)

type TestTemplatesConfigBodyParams struct {
	// Alerts to use as data when testing the template.
	Alerts []*PostableAlert

	// Template string to test.
	Template string

	// Name of the template file.
	Name string
}

type TestTemplatesResults struct {
	Results []TestTemplatesResult
	Errors  []TestTemplatesErrorResult
}

type TestTemplatesResult struct {
	// Name of the associated template definition for this result.
	Name string

	// Interpolated value of the template.
	Text string
}

type TestTemplatesErrorResult struct {
	// Name of the associated template for this error. Will be empty if the Kind is "invalid_template".
	Name string

	// Kind of template error that occurred.
	Kind TemplateErrorKind

	// Error cause.
	Error error
}

type TemplateErrorKind string

const (
	InvalidTemplate TemplateErrorKind = "invalid_template"
	ExecutionError  TemplateErrorKind = "execution_error"
)

const (
	DefaultReceiverName    = "TestReceiver"
	DefaultGroupLabel      = "group_label"
	DefaultGroupLabelValue = "group_label_value"
)

// TestTemplate tests the given template string against the given alerts. Existing templates are used to provide context for the test.
// If an existing template of the same filename as the one being tested is found, it will not be used as context.
func (am *GrafanaAlertmanager) TestTemplate(ctx context.Context, c TestTemplatesConfigBodyParams) (*TestTemplatesResults, error) {
	definitions, err := parseTestTemplate(c.Name, c.Template)
	if err != nil {
		return &TestTemplatesResults{
			Errors: []TestTemplatesErrorResult{{
				Kind:  InvalidTemplate,
				Error: err,
			}},
		}, nil
	}

	// Recreate the current template without the definition blocks that are being tested. This is so that any blocks that were removed don't get defined.
	paths := make([]string, 0)
	for _, name := range am.templates {
		if name == c.Name {
			// Skip the existing template of the same name as we're going to parse the one for testing instead.
			continue
		}
		paths = append(paths, filepath.Join(am.workingDirectory, name))
	}

	// Parse current templates.
	var newTextTmpl *tmpltext.Template
	var captureTemplate template.Option = func(text *tmpltext.Template, _ *tmplhtml.Template) {
		newTextTmpl = text
	}
	newTmpl, err := am.TemplateFromPaths(paths, captureTemplate)
	if err != nil {
		return nil, err
	}

	// Parse test template.
	_, err = newTextTmpl.New(c.Name).Parse(c.Template)
	if err != nil {
		// This shouldn't happen since we already parsed the template above.
		return nil, err
	}

	// Prepare the context.
	alerts := OpenAPIAlertsToAlerts(c.Alerts)
	ctx = notify.WithReceiverName(ctx, DefaultReceiverName)
	ctx = notify.WithGroupLabels(ctx, model.LabelSet{DefaultGroupLabel: DefaultGroupLabelValue})

	promTmplData := notify.GetTemplateData(ctx, newTmpl, alerts, am.logger)
	data := templates.ExtendData(promTmplData, am.logger)

	// Iterate over each definition in the template and evaluate it.
	var results TestTemplatesResults
	for _, def := range definitions {
		var buf bytes.Buffer
		err := newTextTmpl.ExecuteTemplate(&buf, def, data)
		if err != nil {
			results.Errors = append(results.Errors, TestTemplatesErrorResult{
				Name:  def,
				Kind:  ExecutionError,
				Error: err,
			})
		} else {
			results.Results = append(results.Results, TestTemplatesResult{
				Name: def,
				Text: buf.String(),
			})
		}
	}

	return &results, nil
}

// parseTestTemplate parses the test template and returns the top-level definitions that should be interpolated as results.
func parseTestTemplate(name string, text string) ([]string, error) {
	tmpl, err := tmpltext.New(name).Funcs(tmpltext.FuncMap(template.DefaultFuncs)).Parse(text)
	if err != nil {
		return nil, err
	}

	topLevel, err := templates.TopTemplates(tmpl)
	if err != nil {
		return nil, err
	}

	return topLevel, nil
}
