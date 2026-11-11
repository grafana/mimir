// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"cmp"
	_ "embed"
	"html/template"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

const (
	ingesterIDPlaceholder   = "{ingester}"
	ingesterZonePlaceholder = "{zone}"
)

type partitionRingIngestersPageContent struct {
	Now       time.Time
	Ingesters []partitionRingIngester
}

type partitionRingIngester struct {
	IngesterID     string
	Zone           string
	Link           string
	PartitionID    int32
	PartitionState string
}

//go:embed partition_ring_ingesters.gohtml
var partitionRingIngestersPageHTML string
var partitionRingIngestersTemplate = template.Must(template.New("webpage").Parse(partitionRingIngestersPageHTML))

// PartitionRingIngestersPageHandler shows the ingesters that own a partition in the partition ring.
// The page links each ingester to the expansion of linkTemplate, see -ingester.partition-ring.ingesters-page-link-template.
type PartitionRingIngestersPageHandler struct {
	partitionInstanceRing *ring.PartitionInstanceRing
	linkTemplate          string
}

func NewPartitionRingIngestersPageHandler(partitionInstanceRing *ring.PartitionInstanceRing, linkTemplate string) *PartitionRingIngestersPageHandler {
	return &PartitionRingIngestersPageHandler{partitionInstanceRing: partitionInstanceRing, linkTemplate: linkTemplate}
}

func (h *PartitionRingIngestersPageHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	partitionRing := h.partitionInstanceRing.PartitionRing()
	instanceRing := h.partitionInstanceRing.InstanceRing()

	var ingesters []partitionRingIngester
	for _, partition := range partitionRing.Partitions() {
		for _, ingesterID := range partitionRing.PartitionOwnerIDs(partition.Id) {
			// An ingester that is not in the instance ring has no zone.
			instance, _ := instanceRing.GetInstance(ingesterID)
			ingesters = append(ingesters, partitionRingIngester{
				IngesterID:     ingesterID,
				Zone:           instance.Zone,
				Link:           strings.NewReplacer(ingesterIDPlaceholder, ingesterID, ingesterZonePlaceholder, instance.Zone).Replace(h.linkTemplate),
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
