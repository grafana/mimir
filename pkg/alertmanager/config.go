// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"github.com/grafana/alerting/definition"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

func amConfigFromMimirConfig(desc alertspb.AlertConfigDesc) amConfig {
	tmpls := make([]definition.PostableApiTemplate, 0, len(desc.Templates))
	for _, desc := range desc.Templates {
		tmpls = append(tmpls, definition.PostableApiTemplate{
			Name:    desc.Filename,
			Content: desc.Body,
			Kind:    definition.MimirTemplateKind,
		})
	}

	return amConfig{
		User:      desc.User,
		RawConfig: desc.RawConfig,
		Templates: tmpls,
	}
}
