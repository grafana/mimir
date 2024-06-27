// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertspb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertspb

import "errors"

var (
	ErrNotFound = errors.New("alertmanager storage object not found")
)

// AlertConfigDescs is a wrapper for a Mimir and a Grafana Alertmanager configurations.
type AlertConfigDescs struct {
	Mimir   AlertConfigDesc
	Grafana GrafanaAlertConfigDesc
}

// ToProto transforms a yaml Alertmanager config and map of template files to an AlertConfigDesc.
func ToProto(cfg string, templates map[string]string, user string) AlertConfigDesc {
	tmpls := make([]*TemplateDesc, 0, len(templates))
	for fn, body := range templates {
		tmpls = append(tmpls, &TemplateDesc{
			Body:     body,
			Filename: fn,
		})
	}
	return AlertConfigDesc{
		User:      user,
		RawConfig: cfg,
		Templates: tmpls,
	}
}

// ToGrafanaProto transforms a Grafana Alertmanager config to a GrafanaAlertConfigDesc.
func ToGrafanaProto(cfg, user, hash string, createdAtTimestamp int64, isDefault, isPromoted bool, externalURL string, staticHeaders map[string]string) GrafanaAlertConfigDesc {
	return GrafanaAlertConfigDesc{
		User:               user,
		RawConfig:          cfg,
		Hash:               hash,
		CreatedAtTimestamp: createdAtTimestamp,
		Default:            isDefault,
		Promoted:           isPromoted,
		ExternalUrl:        externalURL,
		StaticHeaders:      staticHeaders,
	}
}

// ParseTemplates returns an Alertmanager config object.
func ParseTemplates(cfg AlertConfigDesc) map[string]string {
	templates := map[string]string{}
	for _, t := range cfg.Templates {
		templates[t.Filename] = t.Body
	}
	return templates
}
