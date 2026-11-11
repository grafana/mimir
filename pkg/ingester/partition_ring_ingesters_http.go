// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"cmp"
	_ "embed"
	"html/template"
	"net/http"
	"slices"
	"time"

	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

type partitionRingIngestersPageContent struct {
	Now       time.Time
	Ingesters []partitionRingIngester
}

type partitionRingIngester struct {
	IngesterID     string
	PartitionID    int32
	PartitionState string
}

//go:embed partition_ring_ingesters.gohtml
var partitionRingIngestersPageHTML string
var partitionRingIngestersTemplate = template.Must(template.New("webpage").Parse(partitionRingIngestersPageHTML))

// PartitionRingIngestersPageHandler shows the ingesters that own a partition in the partition ring.
// The page links each ingester to "<ingester ID>/ingester/tenants", relative to the page.
// These links work only through a proxy that sends "<ingester ID>/" to that ingester.
type PartitionRingIngestersPageHandler struct {
	partitionRing ring.PartitionRingReader
}

func NewPartitionRingIngestersPageHandler(partitionRing ring.PartitionRingReader) *PartitionRingIngestersPageHandler {
	return &PartitionRingIngestersPageHandler{partitionRing: partitionRing}
}

func (h *PartitionRingIngestersPageHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	partitionRing := h.partitionRing.PartitionRing()

	var ingesters []partitionRingIngester
	for _, partition := range partitionRing.Partitions() {
		for _, ingesterID := range partitionRing.PartitionOwnerIDs(partition.Id) {
			ingesters = append(ingesters, partitionRingIngester{
				IngesterID:     ingesterID,
				PartitionID:    partition.Id,
				PartitionState: partition.State.String(),
			})
		}
	}

	slices.SortFunc(ingesters, func(a, b partitionRingIngester) int {
		return cmp.Or(cmp.Compare(a.PartitionID, b.PartitionID), cmp.Compare(a.IngesterID, b.IngesterID))
	})

	util.RenderHTTPResponse(w, partitionRingIngestersPageContent{
		Now:       time.Now(),
		Ingesters: ingesters,
	}, partitionRingIngestersTemplate, req)
}
