package templates

import (
	"github.com/prometheus/alertmanager/template"
)

var (
	DefaultFuncs = template.DefaultFuncs
)

func defaultTemplatesPerKind(kind Kind) []string {
	switch kind {
	case GrafanaKind:
		return []string{
			DefaultTemplateString,
		}
	default:
		return nil
	}
}

func defaultOptionsPerKind(kind Kind) []template.Option {
	switch kind {
	case GrafanaKind:
		return []template.Option{
			addFuncs,
		}
	default:
		return nil
	}
}
