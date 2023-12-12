// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	_ "embed" // Used to embed html template
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/model/labels"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/listblocks"
)

//go:embed blocks.gohtml
var blocksPageHTML string
var blocksPageTemplate = template.Must(template.New("webpage").Parse(blocksPageHTML))

type blocksPageContents struct {
	Now             time.Time            `json:"now"`
	Tenant          string               `json:"tenant,omitempty"`
	RichMetas       []richMeta           `json:"metas"`
	FormattedBlocks []formattedBlockData `json:"-"`
	ShowDeleted     bool                 `json:"-"`
	ShowSources     bool                 `json:"-"`
	ShowParents     bool                 `json:"-"`
	SplitCount      int                  `json:"-"`
}

type formattedBlockData struct {
	ULID             string
	ULIDTime         string
	SplitID          *uint32
	MinTime          string
	MaxTime          string
	Duration         string
	DeletedTime      string
	CompactionLevel  int
	BlockSize        string
	Labels           string
	NoCompactDetails []string
	Sources          []string
	Parents          []string
	Stats            prom_tsdb.BlockStats
}

type richMeta struct {
	*block.Meta
	DeletedTime *int64  `json:"deletedTime,omitempty"`
	SplitID     *uint32 `json:"splitId,omitempty"`
}

func (s *StoreGateway) BlocksHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	tenantID := vars["tenant"]
	if tenantID == "" {
		util.WriteTextResponse(w, "Tenant ID can't be empty")
		return
	}

	if err := req.ParseForm(); err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Can't parse form: %s", err))
		return
	}

	showDeleted := req.Form.Get("show_deleted") == "on"
	showSources := req.Form.Get("show_sources") == "on"
	showParents := req.Form.Get("show_parents") == "on"
	var splitCount int
	if sc := req.Form.Get("split_count"); sc != "" {
		splitCount, _ = strconv.Atoi(sc)
		if splitCount < 0 {
			splitCount = 0
		}
	}

	metasMap, deleteMarkerDetails, noCompactMarkerDetails, err := listblocks.LoadMetaFilesAndMarkers(req.Context(), s.stores.bucket, tenantID, showDeleted, time.Time{})
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
		return
	}
	metas := listblocks.SortBlocks(metasMap)

	formattedBlocks := make([]formattedBlockData, 0, len(metas))
	richMetas := make([]richMeta, 0, len(metas))

	for _, m := range metas {
		if !showDeleted && deleteMarkerDetails[m.ULID].DeletionTime != 0 {
			continue
		}
		var parents []string
		for _, pb := range m.Compaction.Parents {
			parents = append(parents, pb.ULID.String())
		}
		var sources []string
		for _, pb := range m.Compaction.Sources {
			sources = append(sources, pb.String())
		}
		var blockSplitID *uint32
		if splitCount > 0 {
			bsc := tsdb.HashBlockID(m.ULID) % uint32(splitCount)
			blockSplitID = &bsc
		}
		lbls := labels.FromMap(m.Thanos.Labels)
		noCompactDetails := []string{}
		if val, ok := noCompactMarkerDetails[m.ULID]; ok {
			noCompactDetails = []string{
				fmt.Sprintf("Time: %s", formatTimeIfNotZero(val.NoCompactTime, time.RFC3339)),
				fmt.Sprintf("Reason: %s", val.Reason),
			}
		}

		formattedBlocks = append(formattedBlocks, formattedBlockData{
			ULID:             m.ULID.String(),
			ULIDTime:         util.TimeFromMillis(int64(m.ULID.Time())).UTC().Format(time.RFC3339),
			SplitID:          blockSplitID,
			MinTime:          util.TimeFromMillis(m.MinTime).UTC().Format(time.RFC3339),
			MaxTime:          util.TimeFromMillis(m.MaxTime).UTC().Format(time.RFC3339),
			Duration:         util.TimeFromMillis(m.MaxTime).Sub(util.TimeFromMillis(m.MinTime)).String(),
			DeletedTime:      formatTimeIfNotZero(deleteMarkerDetails[m.ULID].DeletionTime, time.RFC3339),
			NoCompactDetails: noCompactDetails,
			CompactionLevel:  m.Compaction.Level,
			BlockSize:        listblocks.GetFormattedBlockSize(m),
			Labels:           lbls.String(),
			Sources:          sources,
			Parents:          parents,
			Stats:            m.Stats,
		})
		var deletedAt *int64
		if dt, ok := deleteMarkerDetails[m.ULID]; ok {
			deletedAtTime := dt.DeletionTime * int64(time.Second/time.Millisecond)
			deletedAt = &deletedAtTime
		}
		richMetas = append(richMetas, richMeta{
			Meta:        m,
			DeletedTime: deletedAt,
			SplitID:     blockSplitID,
		})
	}

	util.RenderHTTPResponse(w, blocksPageContents{
		Now:             time.Now(),
		Tenant:          tenantID,
		RichMetas:       richMetas,
		FormattedBlocks: formattedBlocks,

		SplitCount:  splitCount,
		ShowDeleted: showDeleted,
		ShowSources: showSources,
		ShowParents: showParents,
	}, blocksPageTemplate, req)
}

func formatTimeIfNotZero(t int64, format string) string {
	if t == 0 {
		return ""
	}
	return time.Unix(t, 0).UTC().Format(format)
}
