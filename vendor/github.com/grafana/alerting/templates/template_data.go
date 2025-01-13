package templates

import (
	"context"
	"encoding/json"
	"fmt"
	tmplhtml "html/template"
	"net/url"
	"path"
	"slices"
	"sort"
	"strings"
	tmpltext "text/template"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/prometheus/alertmanager/asset"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/template"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/models"
)

type Template = template.Template
type KV = template.KV
type Data = template.Data

var newTemplate = template.New

type TemplateDefinition struct {
	// Name of the template. Used to identify the template in the UI and when testing.
	Name string
	// Template string that contains the template text.
	Template string
}

type ExtendedAlert struct {
	Status        string             `json:"status"`
	Labels        KV                 `json:"labels"`
	Annotations   KV                 `json:"annotations"`
	StartsAt      time.Time          `json:"startsAt"`
	EndsAt        time.Time          `json:"endsAt"`
	GeneratorURL  string             `json:"generatorURL"`
	Fingerprint   string             `json:"fingerprint"`
	SilenceURL    string             `json:"silenceURL"`
	DashboardURL  string             `json:"dashboardURL"`
	PanelURL      string             `json:"panelURL"`
	Values        map[string]float64 `json:"values"`
	ValueString   string             `json:"valueString"` // TODO: Remove in Grafana 10
	ImageURL      string             `json:"imageURL,omitempty"`
	EmbeddedImage string             `json:"embeddedImage,omitempty"`
}

type ExtendedAlerts []ExtendedAlert

type ExtendedData struct {
	Receiver string         `json:"receiver"`
	Status   string         `json:"status"`
	Alerts   ExtendedAlerts `json:"alerts"`

	GroupLabels       KV `json:"groupLabels"`
	CommonLabels      KV `json:"commonLabels"`
	CommonAnnotations KV `json:"commonAnnotations"`

	ExternalURL string `json:"externalURL"`
}

var DefaultTemplateName = "__default__"

// DefaultTemplate returns a new Template with all default templates parsed.
func DefaultTemplate(options ...template.Option) (TemplateDefinition, error) {
	// We cannot simply append the text of each default file together as there can be (and are) duplicate template
	// names. Duplicate templates should override when parsed from separate files but will fail to parse if both are in
	// the same file.
	// So, instead we allow tmpltext to combine the templates and then convert it to a string afterwards.
	// The underlying template is not accessible, so we capture it via template.Option.
	var newTextTmpl *tmpltext.Template
	var captureTemplate template.Option = func(text *tmpltext.Template, _ *tmplhtml.Template) {
		newTextTmpl = text
	}

	// Call FromContent without any user-provided templates to get the combined default template.
	_, err := FromContent(nil, append(options, captureTemplate)...)
	if err != nil {
		return TemplateDefinition{}, err
	}

	var combinedTemplate strings.Builder
	tmpls := newTextTmpl.Templates()
	// Sort for a consistent order.
	slices.SortFunc(tmpls, func(a, b *tmpltext.Template) int {
		return strings.Compare(a.Name(), b.Name())
	})

	// Recreate the "define" blocks for all templates. Would be nice to have a more direct way to do this.
	for _, tmpl := range tmpls {
		if tmpl.Name() != "" {
			def := tmpl.Tree.Root.String()
			if tmpl.Name() == "__text_values_list" {
				// Temporary fix for https://github.com/golang/go/commit/6fea4094242fe4e7be8bd7ec0b55df9f6df3f025.
				// TODO: Can remove with GO v1.24.
				def = strings.Replace(def, "$first := false", "$first = false", 1)
			}

			combinedTemplate.WriteString(fmt.Sprintf("{{ define \"%s\" }}%s{{ end }}\n\n", tmpl.Name(), def))
		}
	}
	return TemplateDefinition{
		Name:     DefaultTemplateName,
		Template: combinedTemplate.String(),
	}, nil
}

// FromContent calls Parse on all provided template content and returns the resulting Template. Content equivalent to templates.FromGlobs.
func FromContent(tmpls []string, options ...template.Option) (*Template, error) {
	t, err := newTemplate(options...)
	if err != nil {
		return nil, err
	}

	// Parse prometheus default templates. Copied from template.FromGlobs.
	defaultPrometheusTemplates := []string{"default.tmpl", "email.tmpl"}
	for _, file := range defaultPrometheusTemplates {
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

	// Parse default template string.
	err = t.Parse(strings.NewReader(DefaultTemplateString))
	if err != nil {
		return nil, err
	}

	// Parse all provided templates.
	for _, tc := range tmpls {
		err := t.Parse(strings.NewReader(tc))
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func removePrivateItems(kv template.KV) template.KV {
	for key := range kv {
		if strings.HasPrefix(key, "__") && strings.HasSuffix(key, "__") {
			kv = kv.Remove([]string{key})
		}
	}
	return kv
}

func extendAlert(alert template.Alert, externalURL string, logger log.Logger) *ExtendedAlert {
	// remove "private" annotations & labels so they don't show up in the template
	extended := &ExtendedAlert{
		Status:       alert.Status,
		Labels:       removePrivateItems(alert.Labels),
		Annotations:  removePrivateItems(alert.Annotations),
		StartsAt:     alert.StartsAt,
		EndsAt:       alert.EndsAt,
		GeneratorURL: alert.GeneratorURL,
		Fingerprint:  alert.Fingerprint,
	}

	// fill in some grafana-specific urls
	if len(externalURL) == 0 {
		return extended
	}
	u, err := url.Parse(externalURL)
	if err != nil {
		level.Debug(logger).Log("msg", "failed to parse external URL while extending template data", "url", externalURL, "error", err.Error())
		return extended
	}
	externalPath := u.Path

	generatorURL, err := url.Parse(extended.GeneratorURL)
	if err != nil {
		level.Debug(logger).Log("msg", "failed to parse generator URL while extending template data", "url", extended.GeneratorURL, "error", err.Error())
		return extended
	}

	orgID := alert.Annotations[models.OrgIDAnnotation]
	if len(orgID) > 0 {
		extended.GeneratorURL = setOrgIDQueryParam(generatorURL, orgID)
	}

	dashboardUID := alert.Annotations[models.DashboardUIDAnnotation]
	if len(dashboardUID) > 0 {
		u.Path = path.Join(externalPath, "/d/", dashboardUID)
		extended.DashboardURL = u.String()
		panelID := alert.Annotations[models.PanelIDAnnotation]
		if len(panelID) > 0 {
			u.RawQuery = "viewPanel=" + panelID
			extended.PanelURL = u.String()
		}
		dashboardURL, err := url.Parse(extended.DashboardURL)
		if err != nil {
			level.Debug(logger).Log("msg", "failed to parse dashboard URL while extending template data", "url", extended.DashboardURL, "error", err.Error())
			return extended
		}
		if len(orgID) > 0 {
			extended.DashboardURL = setOrgIDQueryParam(dashboardURL, orgID)
			extended.PanelURL = setOrgIDQueryParam(u, orgID)
		}
	}

	if alert.Annotations != nil {
		if s, ok := alert.Annotations[models.ValuesAnnotation]; ok {
			if err := json.Unmarshal([]byte(s), &extended.Values); err != nil {
				level.Warn(logger).Log("msg", "failed to unmarshal values annotation", "error", err.Error())
			}
		}

		// TODO: Remove in Grafana 10
		extended.ValueString = alert.Annotations[models.ValueStringAnnotation]
	}

	matchers := make([]string, 0)
	for key, value := range alert.Labels {
		if !(strings.HasPrefix(key, "__") && strings.HasSuffix(key, "__")) {
			matchers = append(matchers, key+"="+value)
		}
	}
	sort.Strings(matchers)
	u.Path = path.Join(externalPath, "/alerting/silence/new")

	query := make(url.Values)
	query.Add("alertmanager", "grafana")
	for _, matcher := range matchers {
		query.Add("matcher", matcher)
	}

	u.RawQuery = query.Encode()
	if len(orgID) > 0 {
		extended.SilenceURL = setOrgIDQueryParam(u, orgID)
	} else {
		extended.SilenceURL = u.String()
	}
	return extended
}

func setOrgIDQueryParam(url *url.URL, orgID string) string {
	q := url.Query()
	q.Set("orgId", orgID)
	url.RawQuery = q.Encode()

	return url.String()
}

func ExtendData(data *Data, logger log.Logger) *ExtendedData {
	alerts := make([]ExtendedAlert, 0, len(data.Alerts))

	for _, alert := range data.Alerts {
		extendedAlert := extendAlert(alert, data.ExternalURL, logger)
		alerts = append(alerts, *extendedAlert)
	}

	extended := &ExtendedData{
		Receiver:          data.Receiver,
		Status:            data.Status,
		Alerts:            alerts,
		GroupLabels:       data.GroupLabels,
		CommonLabels:      removePrivateItems(data.CommonLabels),
		CommonAnnotations: removePrivateItems(data.CommonAnnotations),

		ExternalURL: data.ExternalURL,
	}
	return extended
}

func TmplText(ctx context.Context, tmpl *Template, alerts []*types.Alert, l log.Logger, tmplErr *error) (func(string) string, *ExtendedData) {
	promTmplData := notify.GetTemplateData(ctx, tmpl, alerts, l)
	data := ExtendData(promTmplData, l)

	return func(name string) (s string) {
		if *tmplErr != nil {
			return
		}
		s, *tmplErr = tmpl.ExecuteTextString(name, data)
		return s
	}, data
}

// Firing returns the subset of alerts that are firing.
func (as ExtendedAlerts) Firing() []ExtendedAlert {
	res := []ExtendedAlert{}
	for _, a := range as {
		if a.Status == string(model.AlertFiring) {
			res = append(res, a)
		}
	}
	return res
}

// Resolved returns the subset of alerts that are resolved.
func (as ExtendedAlerts) Resolved() []ExtendedAlert {
	res := []ExtendedAlert{}
	for _, a := range as {
		if a.Status == string(model.AlertResolved) {
			res = append(res, a)
		}
	}
	return res
}
