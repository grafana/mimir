// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	_ "embed" // Used to embed html template
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/tsdb"
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
	ShowSplitCount  bool                 `json:"-"`
	ShowSources     bool                 `json:"-"`
	ShowParents     bool                 `json:"-"`

	ShowDeletedQuery string `json:"-"`
	ShowSourcesQuery string `json:"-"`
	ShowParentsQuery string `json:"-"`
}

type formattedBlockData struct {
	ULID            string
	ULIDTime        string
	SplitCount      *uint32
	MinTime         string
	MaxTime         string
	Duration        string
	DeletedTime     string
	CompactionLevel int
	BlockSize       string
	Labels          string
	Sources         []string
	Parents         []string
}

type richMeta struct {
	*metadata.Meta
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

	showDeleted := req.Form.Get("show_deleted") == "true"
	showSources := req.Form.Get("show_sources") == "true"
	showParents := req.Form.Get("show_parents") == "true"
	var splitCount int
	if sc := req.Form.Get("split_count"); sc != "" {
		var err error
		splitCount, err = strconv.Atoi(sc)
		if err != nil {
			util.WriteTextResponse(w, fmt.Sprintf("Bad split_count param: %s", err))
			return
		}
	}

	metasMap, deletedTimes, err := listblocks.LoadMetaFilesAndDeletionMarkers(req.Context(), s.stores.bucket, tenantID, showDeleted, time.Time{})
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
		return
	}
	metas := listblocks.SortBlocks(metasMap)

	formattedBlocks := make([]formattedBlockData, 0, len(metas))
	richMetas := make([]richMeta, 0, len(metas))

	for _, m := range metas {
		if !showDeleted && !deletedTimes[m.ULID].IsZero() {
			continue
		}
		var parents []string
		for _, pb := range m.Compaction.Parents {
			parents = append(parents, pb.ULID.String())
		}
		var sources []string
		for _, pb := range m.Compaction.Sources {
			sources = append(parents, pb.String())
		}
		var blockSplitID *uint32
		if splitCount > 0 {
			bsc := tsdb.HashBlockID(m.ULID) % uint32(splitCount)
			blockSplitID = &bsc
		}
		lbls := labels.FromMap(m.Thanos.Labels)
		formattedBlocks = append(formattedBlocks, formattedBlockData{
			ULID:            m.ULID.String(),
			ULIDTime:        util.TimeFromMillis(int64(m.ULID.Time())).UTC().Format(time.RFC3339),
			SplitCount:      blockSplitID,
			MinTime:         util.TimeFromMillis(m.MinTime).UTC().Format(time.RFC3339),
			MaxTime:         util.TimeFromMillis(m.MaxTime).UTC().Format(time.RFC3339),
			Duration:        util.TimeFromMillis(m.MaxTime).Sub(util.TimeFromMillis(m.MinTime)).String(),
			DeletedTime:     formatTimeIfNotZero(deletedTimes[m.ULID].UTC(), time.RFC3339),
			CompactionLevel: m.Compaction.Level,
			BlockSize:       listblocks.GetFormattedBlockSize(m),
			Labels:          lbls.String(),
			Sources:         sources,
			Parents:         parents,
		})
		var deletedAt *int64
		if dt, ok := deletedTimes[m.ULID]; ok {
			deletedAtTime := dt.UnixMilli()
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

		ShowSplitCount: splitCount > 0,
		ShowDeleted:    showDeleted,
		ShowSources:    showSources,
		ShowParents:    showParents,

		ShowDeletedQuery: queryWithTrueBoolParam(*req.URL, req.Form, "show_deleted"),
		ShowSourcesQuery: queryWithTrueBoolParam(*req.URL, req.Form, "show_sources"),
		ShowParentsQuery: queryWithTrueBoolParam(*req.URL, req.Form, "show_parents"),
	}, blocksPageTemplate, req)
}

func queryWithTrueBoolParam(u url.URL, form url.Values, boolParam string) string {
	q := u.Query()
	for k, vs := range form {
		for _, val := range vs {
			// Yes, we set only the last value, but otherwise the logic just gets too complicated.
			q.Set(k, val)
		}
	}
	q.Set(boolParam, "true")
	return "?" + q.Encode()
}

func formatTimeIfNotZero(t time.Time, format string) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(format)
}
