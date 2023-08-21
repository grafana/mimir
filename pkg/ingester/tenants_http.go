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
	Tenants []string
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

	util.RenderHTTPResponse(w, tenantsPageContent{
		Now:     time.Now(),
		Tenants: tenants,
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
