package notify

import (
	"bytes"
	"context"
	tmplhtml "html/template"
	"net/url"
	tmpltext "text/template"

	"github.com/go-kit/log/level"
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

	// Name of the template.
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

	// Recreate the current template replacing the definition blocks that are being tested. This is so that any blocks that were removed don't get defined.
	var found bool
	templateContents := make([]string, 0, len(am.templates)+1)
	for _, td := range am.templates {
		if td.Name == c.Name {
			// Template already exists, test with the new definition replacing the old one.
			templateContents = append(templateContents, c.Template)
			found = true
			continue
		}
		templateContents = append(templateContents, td.Template)
	}

	if !found {
		// Template is a new one, add it to the list.
		templateContents = append(templateContents, c.Template)
	}

	// Capture the underlying text template so we can use ExecuteTemplate.
	var newTextTmpl *tmpltext.Template
	var captureTemplate template.Option = func(text *tmpltext.Template, _ *tmplhtml.Template) {
		newTextTmpl = text
	}
	newTmpl, err := templateFromContent(templateContents, am.ExternalURL(), captureTemplate)
	if err != nil {
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

func (am *GrafanaAlertmanager) GetTemplate() (*template.Template, error) {
	am.reloadConfigMtx.RLock()

	seen := make(map[string]struct{})
	tmpls := make([]string, 0, len(am.templates))
	for _, tc := range am.templates {
		if _, ok := seen[tc.Name]; ok {
			level.Warn(am.logger).Log("msg", "template with same name is defined multiple times, skipping...", "template_name", tc.Name)
			continue
		}
		tmpls = append(tmpls, tc.Template)
		seen[tc.Name] = struct{}{}
	}

	am.reloadConfigMtx.RUnlock()

	tmpl, err := templateFromContent(tmpls, am.ExternalURL())
	if err != nil {
		return nil, err
	}

	return tmpl, nil
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

// TemplateFromContent returns a *Template based on defaults and the provided template contents.
func templateFromContent(tmpls []string, externalURL string, options ...template.Option) (*templates.Template, error) {
	tmpl, err := templates.FromContent(tmpls, options...)
	if err != nil {
		return nil, err
	}
	extURL, err := url.Parse(externalURL)
	if err != nil {
		return nil, err
	}
	tmpl.ExternalURL = extURL
	return tmpl, nil
}
