package ring

import (
	_ "embed"
	"html/template"
	"net/http"
	"sort"
	"time"
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

type PartitionRingReader interface {
	PartitionRing() *PartitionRing
}

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
		ring = h.ring.PartitionRing()
		// TODO Add a function to get it. Consider to clone it like we do in Ring.getRing()
		ringDesc   = ring.desc
		ringOwners = ring.PartitionOwners()
	)

	// Prepare the data to render partitions in the page.
	partitionsData := make([]partitionPageData, 0, len(ringDesc.Partitions))
	for id, data := range ringDesc.Partitions {
		partitionsData = append(partitionsData, partitionPageData{
			ID:             id,
			State:          data.State.String(),
			StateTimestamp: time.Unix(data.StateTimestamp, 0),
			OwnerIDs:       ringOwners[id],
		})
	}

	// Sort partitions by ID.
	sort.Slice(partitionsData, func(i, j int) bool {
		return partitionsData[i].ID < partitionsData[j].ID
	})

	renderHTTPResponse(w, partitionRingPageData{
		Partitions: partitionsData,
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
