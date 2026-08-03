// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	_ "embed" // Used to embed html templates.
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util"
)

//go:embed partitions.gohtml
var partitionsPageHTML string
var partitionsPageTemplate = template.Must(template.New("partitions").Parse(partitionsPageHTML))

//go:embed partition.gohtml
var partitionPageHTML string
var partitionPageTemplate = template.Must(template.New("partition").Parse(partitionPageHTML))

type partitionsPageContents struct {
	Now        time.Time `json:"now"`
	StaticRoot string    `json:"-"`
	InstanceID string    `json:"instance_id"`
	Partitions []int32   `json:"partitions"`
}

type partitionPageContents struct {
	Now        time.Time    `json:"now"`
	StaticRoot string       `json:"-"`
	InstanceID string       `json:"instance_id"`
	Partition  int32        `json:"partition"`
	Tenant     string       `json:"tenant,omitempty"`
	Shards     []ShardStats `json:"shards"`
}

// PartitionsHandler renders the list of partitions currently owned by this usage-tracker instance.
func (t *UsageTracker) PartitionsHandler(w http.ResponseWriter, r *http.Request) {
	if t.State() != services.Running {
		util.WriteHTMLResponse(w, "Usage-tracker is not running yet.")
		return
	}

	t.partitionsMtx.RLock()
	partitions := make([]int32, 0, len(t.partitions))
	for id := range t.partitions {
		partitions = append(partitions, id)
	}
	t.partitionsMtx.RUnlock()
	slices.Sort(partitions)

	util.RenderHTTPResponse(w, partitionsPageContents{
		Now:        time.Now(),
		StaticRoot: "../static/",
		InstanceID: t.cfg.InstanceRing.InstanceID,
		Partitions: partitions,
	}, partitionsPageTemplate, r)
}

// PartitionHandler renders the per-(tenant, shard) debug stats of a partition's tracker store.
func (t *UsageTracker) PartitionHandler(w http.ResponseWriter, r *http.Request) {
	if t.State() != services.Running {
		util.WriteHTMLResponse(w, "Usage-tracker is not running yet.")
		return
	}

	idStr := mux.Vars(r)["partition"]
	id, err := strconv.ParseInt(idStr, 10, 32)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid partition %q: %s", idStr, err), http.StatusBadRequest)
		return
	}

	t.partitionsMtx.RLock()
	ph, ok := t.partitions[int32(id)]
	t.partitionsMtx.RUnlock()
	if !ok {
		http.Error(w, fmt.Sprintf("partition %d is not owned by this usage-tracker instance", id), http.StatusNotFound)
		return
	}

	shards := ph.store.shardStats()

	// An optional ?tenant= query parameter narrows the view to a single tenant.
	tenant := r.URL.Query().Get("tenant")
	if tenant != "" {
		filtered := make([]ShardStats, 0, len(shards))
		for _, s := range shards {
			if s.Tenant == tenant {
				filtered = append(filtered, s)
			}
		}
		shards = filtered
	}

	util.RenderHTTPResponse(w, partitionPageContents{
		Now:        time.Now(),
		StaticRoot: "../../static/",
		InstanceID: t.cfg.InstanceRing.InstanceID,
		Partition:  int32(id),
		Tenant:     tenant,
		Shards:     shards,
	}, partitionPageTemplate, r)
}
