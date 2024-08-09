// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/ha_tracker_http.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	_ "embed" // Used to embed html template
	"html/template"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/util"
)

//go:embed ha_tracker_status.gohtml
var haTrackerStatusPageHTML string
var haTrackerStatusPageTemplate = template.Must(template.New("ha-tracker").Parse(haTrackerStatusPageHTML))

type haTrackerStatusPageContents struct {
	Elected        []haTrackerReplica            `json:"elected"`
	ElectedPerUser map[string][]haTrackerReplica `json:"electedPerUser"`
	Now            time.Time                     `json:"now"`
}

type haTrackerReplica struct {
	UserID         string        `json:"userID"`
	Cluster        string        `json:"cluster"`
	Replica        string        `json:"replica"`
	ElectedAt      time.Time     `json:"electedAt"`
	UpdateTime     time.Duration `json:"updateDuration"`
	TimeToFailover time.Duration `json:"failoverDuration"`

	FailoverTimeoutForUser time.Duration `json:"failoverTimeoutForUser"`
}

func (h *haTracker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.electedLock.RLock()

	electedRepicasPerUser := map[string][]haTrackerReplica{}
	var electedReplicas []haTrackerReplica
	for userID, clusters := range h.clusters {
		failoverTimeout := h.getFailoverTimeoutForUser(userID)

		for cluster, entry := range clusters {
			desc := &entry.elected
			r := haTrackerReplica{
				UserID:         userID,
				Cluster:        cluster,
				Replica:        desc.Replica,
				ElectedAt:      timestamp.Time(desc.ReceivedAt),
				UpdateTime:     time.Until(timestamp.Time(desc.ReceivedAt).Add(h.cfg.UpdateTimeout)),
				TimeToFailover: time.Until(timestamp.Time(desc.ReceivedAt).Add(failoverTimeout)),

				FailoverTimeoutForUser: failoverTimeout,
			}

			electedReplicas = append(electedReplicas, r)
			electedRepicasPerUser[userID] = append(electedRepicasPerUser[userID], r)
		}
	}
	h.electedLock.RUnlock()

	sort.Slice(electedReplicas, func(i, j int) bool {
		first := electedReplicas[i]
		second := electedReplicas[j]

		if first.UserID != second.UserID {
			return first.UserID < second.UserID
		}
		return first.Cluster < second.Cluster
	})

	util.RenderHTTPResponse(w, haTrackerStatusPageContents{
		Elected:        electedReplicas,
		ElectedPerUser: electedRepicasPerUser,
		Now:            time.Now(),
	}, haTrackerStatusPageTemplate, req)
}
