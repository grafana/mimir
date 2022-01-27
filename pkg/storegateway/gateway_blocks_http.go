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
		<table border="1" cellpadding="5">
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
					<td>{{ .BlockSizeHuman }}</td>
					<td>{{ .LabelsString }}</td>
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
	metas := listblocks.SortedBlocks(metasMap)

	type blockData struct {
		ULID                  string        `json:"ulid,omitempty"`
		ULIDTime              string        `json:"ulid_time,omitempty"`
		ULIDTimeUnixMillis    int64         `json:"ulid_time_unix_millis"`
		SplitCount            *uint32       `json:"split_count,omitempty"`
		MinTime               string        `json:"min_time,omitempty"`
		MinTimeUnixMillis     int64         `json:"min_time_unix_millis"`
		MaxTime               string        `json:"max_time,omitempty"`
		MaxTimeUnixMillis     int64         `json:"max_time_unix_millis"`
		Duration              string        `json:"duration,omitempty"`
		DurationSeconds       float64       `json:"duration_seconds"`
		DeletedTime           string        `json:"deleted_time,omitempty"`
		DeletedTimeUnixMillis int64         `json:"deleted_time_unix_millis"`
		CompactionLevel       int           `json:"compaction_level"`
		BlockSizeHuman        string        `json:"block_size_human,omitempty"`
		BlockSizeBytes        uint64        `json:"block_size_bytes"`
		LabelsString          string        `json:"-"`
		Labels                labels.Labels `json:"labels"`
		Sources               []string      `json:"sources"`
		Parents               []string      `json:"parents"`
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
		var blockSplitCount *uint32
		if splitCount > 0 {
			bsc := tsdb.HashBlockID(m.ULID) % uint32(splitCount)
			blockSplitCount = &bsc
		}
		lbls := labels.FromMap(m.Thanos.Labels)
		blocks = append(blocks, blockData{
			ULID:                  m.ULID.String(),
			ULIDTime:              util.TimeFromMillis(int64(m.ULID.Time())).UTC().Format(time.RFC3339),
			ULIDTimeUnixMillis:    util.TimeFromMillis(int64(m.ULID.Time())).UTC().UnixMilli(),
			SplitCount:            blockSplitCount,
			MinTime:               util.TimeFromMillis(m.MinTime).UTC().Format(time.RFC3339),
			MinTimeUnixMillis:     util.TimeFromMillis(m.MinTime).UTC().UnixMilli(),
			MaxTime:               util.TimeFromMillis(m.MaxTime).UTC().Format(time.RFC3339),
			MaxTimeUnixMillis:     util.TimeFromMillis(m.MaxTime).UTC().UnixMilli(),
			Duration:              util.TimeFromMillis(m.MaxTime).Sub(util.TimeFromMillis(m.MinTime)).String(),
			DurationSeconds:       util.TimeFromMillis(m.MaxTime).Sub(util.TimeFromMillis(m.MinTime)).Seconds(),
			DeletedTime:           deletedTimes[m.ULID].UTC().Format(time.RFC3339),
			DeletedTimeUnixMillis: deletedTimes[m.ULID].UTC().UnixMilli(),
			CompactionLevel:       m.Compaction.Level,
			BlockSizeHuman:        listblocks.GetFormattedBlockSize(m),
			BlockSizeBytes:        listblocks.GetBlockSizeBytes(m),
			LabelsString:          lbls.WithoutLabels(tsdb.TenantIDExternalLabel).String(),
			Labels:                lbls,
			Sources:               sources,
			Parents:               parents,
		})
	}

	util.RenderHTTPResponse(w, struct {
		Now            time.Time   `json:"now"`
		User           string      `json:"user,omitempty"`
		Blocks         []blockData `json:"blocks,omitempty"`
		ShowDeleted    bool        `json:"-"`
		ShowSplitCount bool        `json:"-"`
		ShowSources    bool        `json:"-"`
		ShowParents    bool        `json:"-"`

		ShowDeletedURI string `json:"-"`
		ShowSourcesURI string `json:"-"`
		ShowParentsURI string `json:"-"`

		TSDBTenantIDExternalLabel string `json:"-"`
	}{
		Now:    time.Now(),
		User:   userID,
		Blocks: blocks,

		ShowSplitCount: splitCount > 0,

		ShowDeleted: showDeleted,
		ShowSources: showSources,
		ShowParents: showParents,

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
