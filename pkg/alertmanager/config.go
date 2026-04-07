// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"net/url"

	"github.com/grafana/alerting/definition"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func amConfigFromMimirConfig(dec alertspb.AlertConfigDesc, url *url.URL) amConfig {
	return amConfig{
		User:               dec.User,
		RawConfig:          dec.RawConfig,
		Templates:          templateDescToPostableApiTemplate(dec.Templates, definition.MimirTemplateKind),
		TmplExternalURL:    url,
		UsingGrafanaConfig: false,
	}
}

func templateDescToPostableApiTemplate(t []*alertspb.TemplateDesc, kind definition.TemplateKind) []definition.PostableApiTemplate {
	result := make([]definition.PostableApiTemplate, 0, len(t))
	for _, desc := range t {
		result = append(result, definition.PostableApiTemplate{
			Name:    desc.Filename,
			Content: desc.Body,
			Kind:    kind,
		})
	}
	return result
}
