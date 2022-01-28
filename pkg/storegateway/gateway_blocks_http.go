// SPDX-License-Identifier: AGPL-3.0-only

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
	"github.com/thanos-io/thanos/pkg/block/metadata"

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
		<table border="1" cellpadding="5" style="border-collapse: collapse">
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
				{{ range .FormattedBlocks }}
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

	metasMap, deletedTimes, err := listblocks.LoadMetaFilesAndDeletionMarkers(req.Context(), s.stores.bucket, userID, showDeleted, time.Time{})
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
		return
	}
	metas := listblocks.SortBlocks(metasMap)

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
			Labels:          lbls.WithoutLabels(tsdb.TenantIDExternalLabel).String(),
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

	util.RenderHTTPResponse(w, struct {
		Now             time.Time            `json:"now"`
		User            string               `json:"user,omitempty"`
		RichMetas       []richMeta           `json:"metas"`
		FormattedBlocks []formattedBlockData `json:"-"`
		ShowDeleted     bool                 `json:"-"`
		ShowSplitCount  bool                 `json:"-"`
		ShowSources     bool                 `json:"-"`
		ShowParents     bool                 `json:"-"`

		ShowDeletedURI string `json:"-"`
		ShowSourcesURI string `json:"-"`
		ShowParentsURI string `json:"-"`

		TSDBTenantIDExternalLabel string `json:"-"`
	}{
		Now:             time.Now(),
		User:            userID,
		RichMetas:       richMetas,
		FormattedBlocks: formattedBlocks,

		ShowSplitCount: splitCount > 0,
		ShowDeleted:    showDeleted,
		ShowSources:    showSources,
		ShowParents:    showParents,

		ShowDeletedURI: uriWithTrueBoolParam(*req.URL, req.Form, "show_deleted"),
		ShowSourcesURI: uriWithTrueBoolParam(*req.URL, req.Form, "show_sources"),
		ShowParentsURI: uriWithTrueBoolParam(*req.URL, req.Form, "show_parents"),

		TSDBTenantIDExternalLabel: tsdb.TenantIDExternalLabel,
	}, blocksTemplate, req)
}

func uriWithTrueBoolParam(u url.URL, form url.Values, boolParam string) string {
	q := u.Query()
	for k, vs := range form {
		for _, val := range vs {
			// Yes, we set only the last value, but otherwise the logic just gets too complicated.
			q.Set(k, val)
		}
	}
	q.Set(boolParam, "true")
	u.RawQuery = q.Encode()

	return u.RequestURI()
}

func formatTimeIfNotZero(t time.Time, format string) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(format)
}
