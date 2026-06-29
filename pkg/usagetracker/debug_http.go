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
	Partitions []int32   `json:"partitions"`
}

type partitionPageContents struct {
	Now        time.Time    `json:"now"`
	StaticRoot string       `json:"-"`
	Partition  int32        `json:"partition"`
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

	util.RenderHTTPResponse(w, partitionPageContents{
		Now:        time.Now(),
		StaticRoot: "../../static/",
		Partition:  int32(id),
		Shards:     ph.store.shardStats(),
	}, partitionPageTemplate, r)
}
