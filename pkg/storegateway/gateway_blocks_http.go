package storegateway

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/listblocks"
)

const blocksPageTemplate = `
<!DOCTYPE html>
<html>
	<head>
		<meta charset="UTF-8">
		<title>Store-gateway: bucket user blocks</title>
		</script>
	</head>
	<body>
		<h1>Store-gateway: bucket user blocks</h1>
		<p>Current time: {{ .Now }}</p>
		<p>Showing blocks for user: {{ .User }}</p>
		<p>
			{{ if not .ShowDeleted }}
				<a href="{{ .ShowDeletedURI }}">Show Deleted</a>
			{{ end }}
			{{ if not .ShowSources }}
				<a href="{{ .ShowSourcesURI }}">Show Sources</a>
			{{ end }}
			{{ if not .ShowParents }}
				<a href="{{ .ShowParentsURI }}">Show Parents</a>
			{{ end }}
		</p>
		<p>
			Use ?split_count= query param to show split compactor count preview.
		</p>
		<table border="1">
			<thead>
				<tr>
					<th>Block ID</th>
					{{ if .ShowSplitCount }}<th>Split ID</th>{{ end }}
					<th>ULID Time</th>
					<th>Min Time</th>
					<th>Max Time</th>
					<th>Duration</th>
					{{ if .ShowDeleted }}<th>Deletion Time</th>{{ end }}
					<th>Lvl</th>
					<th>Size</th>
					<th>Labels (excl. {{ .TSDBTenantIDExternalLabel }})</th>
					{{ if .ShowSources }}<th>Sources</th>{{ end }}
					{{ if .ShowParents }}<th>Parents</th>{{ end }}
				</tr>
			</thead>
			<tbody>
				{{ $page := . }}
				{{ range .Blocks }}
				<tr>
					<td>{{ .ULID }}</td>
					{{ if $page.ShowSplitCount }}<td>{{ .SplitCount }}</td>{{ end }}
					<td>{{ .ULIDTime }}</td>
					<td>{{ .MinTime }}</td>
					<td>{{ .MaxTime }}</td>
					<td>{{ .Duration }}</td>
					{{ if $page.ShowDeleted }}<td>{{ .DeletedTime }}</td>{{ end }}
					<td>{{ .CompactionLevel }}</td>
					<td>{{ .BlockSize }}</td>
					<td>{{ .Labels }}</td>
					{{ if $page.ShowSources }}
						<td>
							{{ range $i, $source := .Sources }}
								{{ if $i }}<br>{{ end }}
								{{ . }}
							{{ end }}
						</td>
					{{ end }}
					{{ if $page.ShowParents }}
						<td>
							{{ range $i, $source := .Parents }}
								{{ if $i }}<br>{{ end }}
								{{ . }}
							{{ end }}
						</td>
					{{ end }}
				</tr>
				{{ end }}
			</tbody>
		</table>
	</body>
</html>`

var blocksTemplate = template.Must(template.New("webpage").Parse(blocksPageTemplate))

func (s *StoreGateway) BlocksHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	userID := vars["user"]
	if userID == "" {
		util.WriteTextResponse(w, "User ID can't be empty")
		return
	}

	showDeleted := req.URL.Query().Get("show_deleted") == "true"
	showSources := req.URL.Query().Get("show_sources") == "true"
	showParents := req.URL.Query().Get("show_parents") == "true"
	var splitCount int
	if sc := req.URL.Query().Get("split_count"); sc != "" {
		var err error
		splitCount, err = strconv.Atoi(sc)
		if err != nil {
			util.WriteTextResponse(w, fmt.Sprintf("Bad split_count param: %s", err))
			return
		}
	}

	metasMap, deletedTimes, err := listblocks.LoadMetaFilesAndDeletionMarkers(req.Context(), s.stores.bucket, userID, showDeleted, time.Time{})
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
		return
	}
	metas := listblocks.SortedBlocks(metasMap)

	type blockData struct {
		ULID            string   `json:"ulid,omitempty"`
		ULIDTime        string   `json:"ulid_time,omitempty"`
		SplitCount      uint32   `json:"split_count,omitempty"`
		MinTime         string   `json:"min_time,omitempty"`
		MaxTime         string   `json:"max_time,omitempty"`
		Duration        string   `json:"duration,omitempty"`
		DeletedTime     string   `json:"deleted_time,omitempty"`
		CompactionLevel int      `json:"compaction_level,omitempty"`
		BlockSize       string   `json:"block_size,omitempty"`
		Labels          string   `json:"labels,omitempty"`
		Sources         []string `json:"sources,omitempty"`
		Parents         []string `json:"parents,omitempty"`
	}
	blocks := make([]blockData, 0, len(metas))

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
		var blockSplitCount uint32
		if splitCount > 0 {
			blockSplitCount = tsdb.HashBlockID(m.ULID) % uint32(splitCount)
		}
		blocks = append(blocks, blockData{
			ULID:            m.ULID.String(),
			ULIDTime:        util.TimeFromMillis(int64(m.ULID.Time())).UTC().Format(time.RFC3339),
			SplitCount:      blockSplitCount,
			MinTime:         util.TimeFromMillis(m.MinTime).UTC().Format(time.RFC3339),
			MaxTime:         util.TimeFromMillis(m.MaxTime).UTC().Format(time.RFC3339),
			Duration:        util.TimeFromMillis(m.MaxTime).Sub(util.TimeFromMillis(m.MinTime)).String(),
			DeletedTime:     deletedTimes[m.ULID].UTC().Format(time.RFC3339),
			CompactionLevel: m.Compaction.Level,
			BlockSize:       listblocks.GetFormattedBlockSize(m),
			Labels:          labels.FromMap(m.Thanos.Labels).WithoutLabels(tsdb.TenantIDExternalLabel).String(),
			Sources:         sources,
			Parents:         parents,
		})
	}

	util.RenderHTTPResponse(w, struct {
		Now            time.Time   `json:"now"`
		User           string      `json:"user,omitempty"`
		Blocks         []blockData `json:"blocks,omitempty"`
		ShowDeleted    bool        `json:"show_deleted,omitempty"`
		ShowSplitCount bool        `json:"show_split_count,omitempty"`
		ShowSources    bool        `json:"show_sources,omitempty"`
		ShowParents    bool        `json:"show_parents,omitempty"`

		ShowDeletedURI string `json:"-"`
		ShowSourcesURI string `json:"-"`
		ShowParentsURI string `json:"-"`

		TSDBTenantIDExternalLabel string `json:"tsdb_tenant_id_external_label,omitempty"`
	}{
		Now:    time.Now(),
		User:   userID,
		Blocks: blocks,

		ShowSplitCount: splitCount > 0,

		ShowDeleted: showDeleted,
		ShowSources: showSources,
		ShowParents: showParents,

		ShowDeletedURI: uriWithTrueBoolParam(req.URL, "show_deleted"),
		ShowSourcesURI: uriWithTrueBoolParam(req.URL, "show_sources"),
		ShowParentsURI: uriWithTrueBoolParam(req.URL, "show_parents"),

		TSDBTenantIDExternalLabel: tsdb.TenantIDExternalLabel,
	}, blocksTemplate, req)
}

func uriWithTrueBoolParam(u *url.URL, boolParam string) string {
	uc := *u
	q := uc.Query()
	q.Set(boolParam, "true")
	uc.RawQuery = q.Encode()

	return uc.RequestURI()
}
