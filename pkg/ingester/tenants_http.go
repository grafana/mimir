// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	_ "embed"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util"
)

type tenantsPageContent struct {
	Now     time.Time
	Tenants []tenantStats
}

type tenantStats struct {
	Tenant  string
	Blocks  int
	MinTime string
	MaxTime string

	Warning string
}

//go:embed tenants.gohtml
var tenantsPageHTML string
var tenantsTemplate = template.Must(template.New("webpage").Parse(tenantsPageHTML))

type tenantTSDBPageContent struct {
	Now    time.Time
	Tenant string

	Head   tenantTSDBHeadPageContent
	Blocks []tenantTSDBBlockPageContent
}

type tenantTSDBHeadPageContent struct {
	MinTime                string
	MaxTime                string
	NumSeries              uint64
	AppendableMinValidTime string
	MinOOOTime             string
	MaxOOOTime             string
}

type tenantTSDBBlockPageContent struct {
	ID         string
	MinTime    string
	MaxTime    string
	OutOfOrder bool
	Compaction tsdb.BlockMetaCompaction
	Stats      tsdb.BlockStats
	UploadedOn string
}

//go:embed tenant_tsdb.gohtml
var tenantTSDBPageHTML string
var tenantTSDBTemplate = template.Must(template.New("webpage").Parse(tenantTSDBPageHTML))

func (i *Ingester) TenantsHandler(w http.ResponseWriter, req *http.Request) {
	tenants := i.getTSDBUsers()
	slices.Sort(tenants)

	nowMillis := time.Now().UnixMilli()

	var tss []tenantStats
	for _, t := range tenants {
		db := i.getTSDB(t)
		if db == nil {
			continue
		}

		s := tenantStats{}
		s.Tenant = t
		s.Blocks = len(db.Blocks())
		minMillis := db.Head().MinTime()
		s.MinTime = formatMillisTime(db.Head().MinTime())
		maxMillis := db.Head().MaxTime()
		s.MaxTime = formatMillisTime(maxMillis)

		if maxMillis-nowMillis > i.limits.CreationGracePeriod(t).Milliseconds() {
			s.Warning = "TSDB Head max timestamp too far in the future"
		}
		if i.limits.PastGracePeriod(t) > 0 && nowMillis-minMillis > (i.limits.PastGracePeriod(t)+i.limits.OutOfOrderTimeWindow(t)).Milliseconds() {
			s.Warning = "TSDB Head min timestamp too far in the past"
		}

		tss = append(tss, s)
	}

	util.RenderHTTPResponse(w, tenantsPageContent{
		Now:     time.Now(),
		Tenants: tss,
	}, tenantsTemplate, req)
}

func (i *Ingester) TenantTSDBHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	tenant := vars["tenant"]
	if tenant == "" {
		util.WriteTextResponse(w, "Tenant ID can't be empty")
		return
	}

	db := i.getTSDB(tenant)
	if db == nil {
		w.WriteHeader(http.StatusNotFound)
		util.WriteTextResponse(w, "TSDB not found for tenant "+tenant)
		return
	}

	head := db.db.Head()

	c := tenantTSDBPageContent{
		Now:    time.Now(),
		Tenant: tenant,

		Head: tenantTSDBHeadPageContent{
			NumSeries:  head.NumSeries(),
			MinTime:    formatMillisTime(head.MinTime()),
			MaxTime:    formatMillisTime(head.MaxTime()),
			MinOOOTime: formatMillisTime(head.MinOOOTime()),
			MaxOOOTime: formatMillisTime(head.MaxOOOTime()),
		},
	}

	if m, ok := head.AppendableMinValidTime(); ok {
		c.Head.AppendableMinValidTime = formatMillisTime(m)
	}

	shipped := db.getCachedShippedBlocks()

	blocks := db.db.Blocks()
	for _, b := range blocks {
		m := b.Meta()

		bc := tenantTSDBBlockPageContent{
			ID:         m.ULID.String(),
			MinTime:    formatMillisTime(m.MinTime),
			MaxTime:    formatMillisTime(m.MaxTime),
			Stats:      m.Stats,
			OutOfOrder: m.OutOfOrder,
			Compaction: m.Compaction,
		}

		if t, ok := shipped[m.ULID]; ok {
			bc.UploadedOn = formatTime(t)
		}

		c.Blocks = append(c.Blocks, bc)
	}

	util.RenderHTTPResponse(w, c, tenantTSDBTemplate, req)
}

func formatMillisTime(t int64) string {
	switch t {
	case 0:
		return "0"
	case math.MinInt64:
		return "math.MinInt64"
	case math.MaxInt64:
		return "math.MaxInt64"
	}
	return fmt.Sprintf("%s (%d)", formatTime(time.UnixMilli(t)), t)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
