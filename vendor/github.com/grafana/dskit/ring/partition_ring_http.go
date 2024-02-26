package ring

import (
	_ "embed"
	"html/template"
	"net/http"
	"sort"
	"time"

	"golang.org/x/exp/slices"
)

//go:embed partition_ring_status.gohtml
var partitionRingPageContent string
var partitionRingPageTemplate = template.Must(template.New("webpage").Funcs(template.FuncMap{
	"mod": func(i, j int32) bool {
		return i%j == 0
	},
	"formatTimestamp": func(ts time.Time) string {
		return ts.Format("2006-01-02 15:04:05 MST")
	},
}).Parse(partitionRingPageContent))

type PartitionRingPageHandler struct {
	ring PartitionRingReader
}

func NewPartitionRingPageHandler(ring PartitionRingReader) *PartitionRingPageHandler {
	return &PartitionRingPageHandler{
		ring: ring,
	}
}

func (h *PartitionRingPageHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var (
		ring     = h.ring.PartitionRing()
		ringDesc = ring.desc
	)

	// Prepare the data to render partitions in the page.
	partitionsByID := make(map[int32]partitionPageData, len(ringDesc.Partitions))
	for id, partition := range ringDesc.Partitions {
		owners := ring.PartitionOwnerIDsCopy(id)
		slices.Sort(owners)

		partitionsByID[id] = partitionPageData{
			ID:             id,
			State:          partition.State.CleanName(),
			StateTimestamp: partition.GetStateTime(),
			OwnerIDs:       owners,
		}
	}

	// Look for owners of non-existing partitions. We want to provide visibility for such case
	// and we report the partition in corrupted state.
	for ownerID, owner := range ringDesc.Owners {
		partition, exists := partitionsByID[owner.OwnedPartition]

		if !exists {
			partition = partitionPageData{
				ID:             owner.OwnedPartition,
				State:          "Corrupt",
				StateTimestamp: time.Time{},
				OwnerIDs:       []string{ownerID},
			}

			partitionsByID[owner.OwnedPartition] = partition
		}

		if !slices.Contains(partition.OwnerIDs, ownerID) {
			partition.OwnerIDs = append(partition.OwnerIDs, ownerID)
			partitionsByID[owner.OwnedPartition] = partition
		}
	}

	// Covert partitions to a list and sort it by ID.
	partitions := make([]partitionPageData, 0, len(partitionsByID))

	for _, partition := range partitionsByID {
		partitions = append(partitions, partition)
	}

	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].ID < partitions[j].ID
	})

	renderHTTPResponse(w, partitionRingPageData{
		Partitions: partitions,
	}, partitionRingPageTemplate, req)
}

type partitionRingPageData struct {
	Partitions []partitionPageData `json:"partitions"`
}

type partitionPageData struct {
	ID             int32     `json:"id"`
	State          string    `json:"state"`
	StateTimestamp time.Time `json:"state_timestamp"`
	OwnerIDs       []string  `json:"owner_ids"`
}
