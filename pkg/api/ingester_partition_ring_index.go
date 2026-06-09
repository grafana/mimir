// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	_ "embed"
	"html/template"
	"net/http"
)

//go:embed ingester_partition_ring_index.gohtml
var ingesterPartitionRingIndexContent string

var ingesterPartitionRingIndexTemplate = template.Must(template.New("webpage").Parse(ingesterPartitionRingIndexContent))

type ingesterPartitionRingIndexData struct {
	CompartmentIDs []int
}

func newIngesterPartitionRingIndexHandler(numCompartments int) http.Handler {
	ids := make([]int, numCompartments)
	for i := range ids {
		ids[i] = i
	}
	data := ingesterPartitionRingIndexData{CompartmentIDs: ids}

	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := ingesterPartitionRingIndexTemplate.Execute(w, data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
