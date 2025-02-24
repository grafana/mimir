package notify

import (
	"context"
	"net/url"
	tmpltext "text/template"

	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/templates"
	"github.com/prometheus/alertmanager/template"
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
	Results []TestTemplatesResult      `json:"results"`
	Errors  []TestTemplatesErrorResult `json:"errors"`
}

type TestTemplatesResult struct {
	// Name of the associated template definition for this result.
	Name string `json:"name"`

	// Interpolated value of the template.
	Text string `json:"text"`
}

type TestTemplatesErrorResult struct {
	// Name of the associated template for this error. Will be empty if the Kind is "invalid_template".
	Name string `json:"name"`

	// Kind of template error that occurred.
	Kind TemplateErrorKind `json:"kind"`

	// Error cause.
	Error string `json:"error"`
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
	am.reloadConfigMtx.RLock()
	tmpls := make([]templates.TemplateDefinition, len(am.templates))
	copy(tmpls, am.templates)
	am.reloadConfigMtx.RUnlock()

	return TestTemplate(ctx, c, tmpls, am.ExternalURL(), am.logger)
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
