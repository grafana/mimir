package email

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"io"

	"github.com/Masterminds/sprig/v3"
)

//go:embed *.html *.txt
var defaultEmailTemplate embed.FS

// EmailTemplate wraps the parsed email templates and provides thread-safe access.
type EmailTemplate struct {
	t *template.Template
}

// singleton holds the parsed email templates, initialized once at startup.
var singleton *EmailTemplate

func init() {
	tmpl, err := template.New("templates").
		Funcs(template.FuncMap{
			"Subject":                 subjectTemplateFunc,
			"__dangerouslyInjectHTML": __dangerouslyInjectHTML,
		}).Funcs(sprig.FuncMap()).
		ParseFS(defaultEmailTemplate, "*.html", "*.txt")
	if err != nil {
		panic(fmt.Errorf("failed to parse email templates: %w", err))
	}

	singleton = &EmailTemplate{t: tmpl}
}

// Template returns the singleton EmailTemplate instance.
// The template is guaranteed to be initialized as the application panics on startup if parsing fails.
func Template() *EmailTemplate {
	return singleton
}

// ExecuteTemplate executes the named template with the provided data and writes the result to wr.
func (e *EmailTemplate) ExecuteTemplate(wr io.Writer, name string, data any) error {
	return e.t.ExecuteTemplate(wr, name, data)
}

// subjectTemplateFunc sets the subject template (value) on the map represented by `.Subject.` (obj) so that it can be compiled and executed later.
// In addition, it executes and returns the subject template using the data represented in `.TemplateData` (data).
// This results in the template being replaced by the subject string.
func subjectTemplateFunc(obj map[string]any, data map[string]any, value string) string {
	obj["value"] = value

	titleTmpl, err := template.New("title").Parse(value)
	if err != nil {
		return ""
	}

	var buf bytes.Buffer
	err = titleTmpl.ExecuteTemplate(&buf, "title", data)
	if err != nil {
		return ""
	}

	subj := buf.String()
	// Since we have already executed the template, save it to subject data so we don't have to do it again later on
	obj["executed_template"] = subj
	return subj
}

// __dangerouslyInjectHTML allows marking areas of am email template as HTML safe, this will _not_ sanitize the string and will allow HTML snippets to be rendered verbatim.
// Use with absolute care as this _could_ allow for XSS attacks when used in an insecure context.
//
// It's safe to ignore gosec warning G203 when calling this function in an HTML template because we assume anyone who has write access
// to the email templates folder is an administrator.
//
// nolint:gosec,revive
func __dangerouslyInjectHTML(s string) template.HTML {
	return template.HTML(s)
}
