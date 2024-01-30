// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/http_admin.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	_ "embed" // Used to embed html template
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/util"
)

//go:embed ingester_stats.gohtml
var ingesterStatsPageHTML string
var ingesterStatsPageTemplate = template.Must(template.New("webpage").Parse(ingesterStatsPageHTML))

type ingesterStatsPageContents struct {
	Now               time.Time     `json:"now"`
	Stats             []UserIDStats `json:"stats"`
	ReplicationFactor int           `json:"replicationFactor"`
}

type userStatsByTimeseries []UserIDStats

func (s userStatsByTimeseries) Len() int      { return len(s) }
func (s userStatsByTimeseries) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s userStatsByTimeseries) Less(i, j int) bool {
	return s[i].NumSeries > s[j].NumSeries ||
		(s[i].NumSeries == s[j].NumSeries && s[i].UserID < s[j].UserID)
}

// AllUserStatsHandler shows stats for all users.
func (d *Distributor) AllUserStatsHandler(w http.ResponseWriter, r *http.Request) {
	stats, err := d.AllUserStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(userStatsByTimeseries(stats))

	if encodings, found := r.Header["Accept"]; found &&
		len(encodings) > 0 && strings.Contains(encodings[0], "json") {
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, fmt.Sprintf("Error marshalling response: %v", err), http.StatusInternalServerError)
		}
		return
	}

	var replicationFactor int
	if d.cfg.IngestStorageConfig.Enabled {
		replicationFactor = d.partitionsInstanceRing.ReplicationFactor()
	} else {
		replicationFactor = d.ingestersRing.ReplicationFactor()
	}

	util.RenderHTTPResponse(w, ingesterStatsPageContents{
		Now:               time.Now(),
		Stats:             stats,
		ReplicationFactor: replicationFactor,
	}, ingesterStatsPageTemplate, r)
}
