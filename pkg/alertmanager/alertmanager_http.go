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
	"github.com/grafana/dskit/services"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	//go:embed ring_status.gohtml
	ringStatusPageHTML     string
	ringStatusPageTemplate = template.Must(template.New("ringStatusPage").Parse(ringStatusPageHTML))

	//go:embed status.gohtml
	statusPageHTML     string
	statusPageTemplate = template.Must(template.New("statusPage").Parse(statusPageHTML))
)

type ringStatusPageContents struct {
	Message string
}

type statusPageContents struct {
	State string
}

func writeRingStatusMessage(w http.ResponseWriter, message string) {
	w.WriteHeader(http.StatusOK)
	err := ringStatusPageTemplate.Execute(w, ringStatusPageContents{Message: message})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "unable to serve alertmanager ring page", "err", err)
	}
}

func (am *MultitenantAlertmanager) RingHandler(w http.ResponseWriter, req *http.Request) {
	if am.State() != services.Running {
		// we cannot read the ring before the alertmanager is in Running state,
		// because that would lead to race condition.
		writeRingStatusMessage(w, "Alertmanager is not running yet.")
		return
	}

	am.ring.ServeHTTP(w, req)
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
