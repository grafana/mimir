// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	_ "embed" // Used to embed html template
	"errors"
	"fmt"
	"html/template"
	"maps"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/listblocks"
)

//go:embed blocks.gohtml
var blocksPageHTML string
var blocksPageTemplate = template.Must(template.New("webpage").Parse(blocksPageHTML))

const (
	blocksSourceBucketIndex = "bucket-index"
	blocksSourceBucketScan  = "bucket-scan"

	blocksPageDefaultPageSize = 100
	blocksPageMaxDetailBlocks = 1000
)

type blocksPageContents struct {
	Now             time.Time            `json:"now"`
	Tenant          string               `json:"tenant,omitempty"`
	RichMetas       []richMeta           `json:"metas"`
	FormattedBlocks []formattedBlockData `json:"-"`

	blocksPageOptions

	SourceText        string `json:"-"`
	HasDetails        bool   `json:"-"`
	HasNoCompactMarks bool   `json:"-"`

	Source         string    `json:"source"`
	IndexUpdatedAt time.Time `json:"index_updated_at,omitempty"`

	TotalBlocks         int `json:"total_blocks"`
	DeletedBlocks       int `json:"deleted_blocks"`
	HiddenDeletedBlocks int `json:"-"`
	MatchedBlocks       int `json:"matched_blocks"`
	BlocksRead          int `json:"blocks_read"`
	ShownFrom           int `json:"-"`
	ShownTo             int `json:"-"`
	Page                int `json:"page"`
	TotalPages          int `json:"total_pages"`

	PageLinks     []blocksPageLink `json:"-"`
	PageSizeLinks []blocksPageLink `json:"-"`
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
	OutOfOrder       bool
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

type blocksPageTimeFilter struct {
	Input string
	Time  time.Time
}

func (f blocksPageTimeFilter) isSet() bool {
	return !f.Time.IsZero()
}

type blocksPageLink struct {
	Label   string
	URL     string
	Enabled bool
	Current bool
}

type blocksPageOptions struct {
	ShowDeleted bool `json:"-"`
	ShowSources bool `json:"-"`
	ShowParents bool `json:"-"`
	SplitCount  int  `json:"-"`

	BlockID            string               `json:"-"`
	MinTime            blocksPageTimeFilter `json:"-"`
	MaxTime            blocksPageTimeFilter `json:"-"`
	CreatedAfter       blocksPageTimeFilter `json:"-"`
	CreatedBefore      blocksPageTimeFilter `json:"-"`
	MinCompactionLevel int                  `json:"-"`
	MaxCompactionLevel int                  `json:"-"`

	ScanBucket    bool `json:"-"`
	ShowDetails   bool `json:"-"`
	ShowNoCompact bool `json:"-"`

	Page     int `json:"-"`
	PageSize int `json:"page_size"`
}

func (g *StoreGateway) BlocksHandler(w http.ResponseWriter, req *http.Request) {
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

	jsonResponse := strings.Contains(req.Header.Get("Accept"), "application/json")
	defaultPageSize, defaultScanBucket := blocksPageDefaultPageSize, false
	if jsonResponse {
		defaultPageSize, defaultScanBucket = 0, true
	}

	now := time.Now()
	opts, err := parseBlocksPageOptions(req.Form, now, defaultPageSize, defaultScanBucket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := g.loadBlocksPageData(req.Context(), tenantID, opts)
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
		return
	}
	deleteMarkerDetails, noCompactMarkerDetails := data.deleteMarks, data.noCompactMarks

	matched := opts.filterBlocks(data.metas, deleteMarkerDetails)

	if !data.metasAreComplete && opts.needsBlockDetails() && (opts.PageSize <= 0 || opts.PageSize > blocksPageMaxDetailBlocks) {
		opts.PageSize = blocksPageMaxDetailBlocks
	}

	totalPages := opts.totalPages(len(matched))
	page := min(opts.Page, totalPages)
	pageBlocks := opts.pageBlocks(matched, page)

	if !data.metasAreComplete && opts.needsBlockDetails() {
		details, err := listblocks.LoadMetaFilesForBlocks(req.Context(), g.stores.bucket, tenantID, blocksPageIDs(pageBlocks))
		if err != nil {
			util.WriteTextResponse(w, fmt.Sprintf("Failed to read block metadata: %s", err))
			return
		}

		data.blocksRead = len(details)
		pageBlocks = blocksWithDetails(pageBlocks, details)
	}

	formattedBlocks := []formattedBlockData{}
	richMetas := []richMeta{}
	if jsonResponse {
		richMetas = make([]richMeta, 0, len(pageBlocks))
	} else {
		formattedBlocks = make([]formattedBlockData, 0, len(pageBlocks))
	}

	for _, m := range pageBlocks {
		var blockSplitID *uint32
		if opts.SplitCount > 0 {
			bsc := tsdb.HashBlockID(m.ULID) % uint32(opts.SplitCount)
			blockSplitID = &bsc
		}

		if jsonResponse {
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
			OutOfOrder:       m.IsOutOfOrder(),
			Sources:          sources,
			Parents:          parents,
			Stats:            m.Stats,
		})
	}

	hiddenDeleted := 0
	if !opts.ShowDeleted {
		hiddenDeleted = data.countDeletedBlocks()
	}
	shownFrom, shownTo := 0, len(matched)
	if len(matched) > 0 && opts.PageSize > 0 {
		shownFrom = (page-1)*opts.PageSize + 1
		shownTo = min(page*opts.PageSize, len(matched))
	} else if len(matched) > 0 {
		shownFrom = 1
	}
	query := maps.Clone(req.URL.Query())

	util.RenderHTTPResponse(w, blocksPageContents{
		Now:             now,
		Tenant:          tenantID,
		RichMetas:       richMetas,
		FormattedBlocks: formattedBlocks,

		blocksPageOptions: opts,

		SourceText:        blocksPageSourceText(data, now),
		HasDetails:        data.metasAreComplete || opts.needsBlockDetails(),
		HasNoCompactMarks: data.noCompactMarks != nil,

		Source:         data.source,
		IndexUpdatedAt: data.indexUpdatedAt,

		TotalBlocks:         len(data.metas),
		DeletedBlocks:       data.countDeletedBlocks(),
		HiddenDeletedBlocks: hiddenDeleted,
		MatchedBlocks:       len(matched),
		BlocksRead:          data.blocksRead,
		ShownFrom:           shownFrom,
		ShownTo:             shownTo,
		Page:                page,
		TotalPages:          totalPages,

		PageLinks:     blocksPageLinks(query, page, totalPages),
		PageSizeLinks: blocksPageSizeLinks(query, opts.PageSize),
	}, blocksPageTemplate, req)
}

func parseBlocksPageOptions(form url.Values, now time.Time, defaultPageSize int, defaultScanBucket bool) (blocksPageOptions, error) {
	opts := blocksPageOptions{
		ShowDeleted:   form.Get("show_deleted") == "on",
		ShowSources:   form.Get("show_sources") == "on",
		ShowParents:   form.Get("show_parents") == "on",
		BlockID:       strings.TrimSpace(form.Get("block_id")),
		ScanBucket:    blocksPageCheckbox(form, "scan_bucket", defaultScanBucket),
		ShowDetails:   form.Get("show_details") == "on",
		ShowNoCompact: form.Get("show_no_compact") == "on",
	}

	var err error
	if opts.SplitCount, err = parseBlocksPageInt(form, "split_count", 0, 0); err != nil {
		return opts, err
	}
	if opts.MinCompactionLevel, err = parseBlocksPageInt(form, "min_compaction_level", 0, 0); err != nil {
		return opts, err
	}
	if opts.MaxCompactionLevel, err = parseBlocksPageInt(form, "max_compaction_level", 0, 0); err != nil {
		return opts, err
	}
	if opts.Page, err = parseBlocksPageInt(form, "page", 1, 1); err != nil {
		return opts, err
	}
	if opts.PageSize, err = parseBlocksPageInt(form, "page_size", defaultPageSize, 0); err != nil {
		return opts, err
	}
	if opts.MinTime, err = parseBlocksPageTimeFilter(form, "min_time", now); err != nil {
		return opts, err
	}
	if opts.MaxTime, err = parseBlocksPageTimeFilter(form, "max_time", now); err != nil {
		return opts, err
	}
	if opts.CreatedAfter, err = parseBlocksPageTimeFilter(form, "created_after", now); err != nil {
		return opts, err
	}
	if opts.CreatedBefore, err = parseBlocksPageTimeFilter(form, "created_before", now); err != nil {
		return opts, err
	}

	return opts, nil
}

func blocksPageCheckbox(form url.Values, name string, defaultValue bool) bool {
	value := form.Get(name)
	if value == "" {
		return defaultValue
	}
	return value == "on"
}

func parseBlocksPageInt(form url.Values, name string, defaultValue, minValue int) (int, error) {
	raw := strings.TrimSpace(form.Get(name))
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %q is not a number", name, raw)
	}
	return min(max(value, minValue), math.MaxInt32), nil
}

func parseBlocksPageTimeFilter(form url.Values, name string, now time.Time) (blocksPageTimeFilter, error) {
	raw := strings.TrimSpace(form.Get(name))
	if raw == "" {
		return blocksPageTimeFilter{}, nil
	}

	if raw == "now" {
		return blocksPageTimeFilter{Input: raw, Time: now}, nil
	}

	if offset, isRelative := strings.CutPrefix(raw, "now-"); isRelative {
		duration, err := model.ParseDuration(offset)
		if err != nil {
			return blocksPageTimeFilter{}, fmt.Errorf("invalid %s: %w", name, err)
		}
		return blocksPageTimeFilter{Input: raw, Time: now.Add(-time.Duration(duration))}, nil
	}

	millis, err := util.ParseTime(raw)
	if err != nil {
		return blocksPageTimeFilter{}, fmt.Errorf("invalid %s: %w", name, err)
	}
	return blocksPageTimeFilter{Input: raw, Time: util.TimeFromMillis(millis)}, nil
}

func (o blocksPageOptions) needsBlockDetails() bool {
	return o.ShowDetails || o.ShowSources || o.ShowParents
}

func (o blocksPageOptions) filterBlocks(metas []*block.Meta, deleteMarkerDetails map[ulid.ULID]block.DeletionMark) []*block.Meta {
	minTime, maxTime := o.MinTime.Time.UnixMilli(), o.MaxTime.Time.UnixMilli()
	createdAfter, createdBefore := ulid.Timestamp(o.CreatedAfter.Time), ulid.Timestamp(o.CreatedBefore.Time)

	matched := make([]*block.Meta, 0, len(metas))
	for _, m := range metas {
		if !o.ShowDeleted && deleteMarkerDetails[m.ULID].DeletionTime != 0 {
			continue
		}
		if o.BlockID != "" && !strings.EqualFold(m.ULID.String(), o.BlockID) {
			continue
		}
		if o.MinTime.isSet() && m.MinTime < minTime {
			continue
		}
		if o.MaxTime.isSet() && m.MaxTime > maxTime {
			continue
		}
		if o.CreatedAfter.isSet() && m.ULID.Time() < createdAfter {
			continue
		}
		if o.CreatedBefore.isSet() && m.ULID.Time() > createdBefore {
			continue
		}
		if m.Compaction.Level < o.MinCompactionLevel {
			continue
		}
		if o.MaxCompactionLevel > 0 && m.Compaction.Level > o.MaxCompactionLevel {
			continue
		}
		matched = append(matched, m)
	}

	return matched
}

func (o blocksPageOptions) totalPages(matchedBlocks int) int {
	if o.PageSize <= 0 {
		return 1
	}
	return max(1, (matchedBlocks+o.PageSize-1)/o.PageSize)
}

func (o blocksPageOptions) pageBlocks(matched []*block.Meta, page int) []*block.Meta {
	if o.PageSize <= 0 {
		return matched
	}

	start := min((page-1)*o.PageSize, len(matched))
	return matched[start:min(start+o.PageSize, len(matched))]
}

type blocksPageData struct {
	metas            []*block.Meta
	deleteMarks      map[ulid.ULID]block.DeletionMark
	noCompactMarks   map[ulid.ULID]block.NoCompactMark
	source           string
	metasAreComplete bool
	indexUpdatedAt   time.Time
	blocksRead       int
}

func (d blocksPageData) countDeletedBlocks() int {
	deleted := 0
	for _, m := range d.metas {
		if d.deleteMarks[m.ULID].DeletionTime != 0 {
			deleted++
		}
	}
	return deleted
}

func (g *StoreGateway) loadBlocksPageData(ctx context.Context, tenantID string, opts blocksPageOptions) (blocksPageData, error) {
	if opts.ScanBucket {
		return g.scanBlocksFromBucket(ctx, tenantID, opts)
	}

	data, err := g.loadBlocksFromBucketIndex(ctx, tenantID, opts)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, bucketindex.ErrIndexNotFound) && !errors.Is(err, bucketindex.ErrIndexCorrupted) {
		return blocksPageData{}, err
	}

	level.Warn(g.logger).Log("msg", "the blocks page could not use the bucket index, reading every block instead", "user", tenantID, "err", err)

	return g.scanBlocksFromBucket(ctx, tenantID, opts)
}

func (g *StoreGateway) loadBlocksFromBucketIndex(ctx context.Context, tenantID string, opts blocksPageOptions) (blocksPageData, error) {
	idx, err := bucketindex.ReadIndex(ctx, g.stores.bucket, tenantID, g.stores.limits, g.logger)
	if err != nil {
		return blocksPageData{}, err
	}

	metas := make(map[ulid.ULID]*block.Meta, len(idx.Blocks))
	for _, indexed := range idx.Blocks {
		metas[indexed.ID] = indexed.ThanosMeta()
	}

	deleteMarks := make(map[ulid.ULID]block.DeletionMark, len(idx.BlockDeletionMarks))
	for _, mark := range idx.BlockDeletionMarks {
		deleteMarks[mark.ID] = *mark.ThanosDeletionMark()
	}

	data := blocksPageData{
		metas:          listblocks.SortBlocks(metas),
		deleteMarks:    deleteMarks,
		source:         blocksSourceBucketIndex,
		indexUpdatedAt: idx.GetUpdatedAt(),
	}

	if opts.ShowNoCompact {
		if data.noCompactMarks, err = listblocks.LoadNoCompactMarks(ctx, g.stores.bucket, tenantID); err != nil {
			return blocksPageData{}, err
		}
	}

	return data, nil
}

func (g *StoreGateway) scanBlocksFromBucket(ctx context.Context, tenantID string, opts blocksPageOptions) (blocksPageData, error) {
	metas, deleteMarks, noCompactMarks, err := listblocks.LoadMetaFilesAndMarkers(ctx, g.stores.bucket, tenantID, opts.ShowDeleted, opts.CreatedAfter.Time)
	if err != nil {
		return blocksPageData{}, err
	}

	return blocksPageData{
		metas:            listblocks.SortBlocks(metas),
		deleteMarks:      deleteMarks,
		noCompactMarks:   noCompactMarks,
		source:           blocksSourceBucketScan,
		metasAreComplete: true,
		blocksRead:       len(metas),
	}, nil
}

func blocksPageIDs(metas []*block.Meta) []ulid.ULID {
	ids := make([]ulid.ULID, 0, len(metas))
	for _, m := range metas {
		ids = append(ids, m.ULID)
	}
	return ids
}

func blocksWithDetails(metas []*block.Meta, details map[ulid.ULID]*block.Meta) []*block.Meta {
	detailed := make([]*block.Meta, 0, len(metas))
	for _, m := range metas {
		if full, ok := details[m.ULID]; ok {
			m = full
		}
		detailed = append(detailed, m)
	}
	return detailed
}

func blocksPageSourceText(data blocksPageData, now time.Time) string {
	if data.source != blocksSourceBucketIndex {
		return "From a live read of every block in the bucket."
	}

	return fmt.Sprintf("From the bucket index, written %s ago (%s).",
		now.Sub(data.indexUpdatedAt).Round(time.Second), data.indexUpdatedAt.UTC().Format(time.RFC3339))
}

func blocksPageLinks(query url.Values, page, totalPages int) []blocksPageLink {
	link := func(label string, target int, enabled bool) blocksPageLink {
		targetQuery := maps.Clone(query)
		targetQuery.Set("page", strconv.Itoa(target))
		return blocksPageLink{Label: label, URL: "?" + targetQuery.Encode(), Enabled: enabled}
	}

	return []blocksPageLink{
		link("first", 1, page > 1),
		link("previous", page-1, page > 1),
		link("next", page+1, page < totalPages),
		link("last", totalPages, page < totalPages),
	}
}

func blocksPageSizeLinks(query url.Values, pageSize int) []blocksPageLink {
	sizes := []int{25, 50, 100, 250, 1000}
	if pageSize > 0 && !slices.Contains(sizes, pageSize) {
		sizes = append(sizes, pageSize)
		slices.Sort(sizes)
	}

	link := func(label string, size int) blocksPageLink {
		targetQuery := maps.Clone(query)
		targetQuery.Set("page_size", strconv.Itoa(size))
		targetQuery.Del("page")
		return blocksPageLink{Label: label, URL: "?" + targetQuery.Encode(), Current: size == pageSize}
	}

	links := make([]blocksPageLink, 0, len(sizes)+1)
	for _, size := range sizes {
		links = append(links, link(strconv.Itoa(size), size))
	}
	return append(links, link("all", 0))
}

func formatTimeIfNotZero(t int64, format string) string {
	if t == 0 {
		return ""
	}
	return time.Unix(t, 0).UTC().Format(format)
}
