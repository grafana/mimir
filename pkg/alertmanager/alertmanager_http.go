// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_http.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	_ "embed" // Used to embed html template
	"net/http"
	"text/template"

	"github.com/go-kit/log/level"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	//go:embed status.gohtml
	statusPageHTML     string
	statusPageTemplate = template.Must(template.New("statusPage").Parse(statusPageHTML))
)

type statusPageContents struct {
	State string
}

// GetStatusHandler returns the status handler for this multi-tenant
// alertmanager.
func (am *MultitenantAlertmanager) GetStatusHandler() StatusHandler {
	return StatusHandler{
		am: am,
	}
}

// StatusHandler shows the status of the alertmanager.
type StatusHandler struct {
	am *MultitenantAlertmanager
}

// ServeHTTP serves the status of the alertmanager.
func (s StatusHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	err := statusPageTemplate.Execute(w, statusPageContents{
		State: s.am.State().String(),
	})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "unable to serve alertmanager status page", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
