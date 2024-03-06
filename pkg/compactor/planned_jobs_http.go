// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	_ "embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/timestamp"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
)

//go:embed compactor_tenants.gohtml
var tenantsPageHTML string
var tenantsTemplate = template.Must(template.New("webpage").Parse(tenantsPageHTML))

type tenantsPageContents struct {
	Now     time.Time `json:"now"`
	Tenants []string  `json:"tenants,omitempty"`
}

func (c *MultitenantCompactor) TenantsHandler(w http.ResponseWriter, req *http.Request) {
	tenants, err := tsdb.ListUsers(req.Context(), c.bucketClient)
	if err != nil {
		util.WriteTextResponse(w, fmt.Sprintf("Can't read tenants: %s", err))
		return
	}

	util.RenderHTTPResponse(w, tenantsPageContents{
		Now:     time.Now(),
		Tenants: tenants,
	}, tenantsTemplate, req)
}

//go:embed planned_jobs.gohtml
var plannerJobsHTML string
var plannerJobsTemplateFuncs = template.FuncMap{"add": func(x, y int) int { return x + y }}
var plannerJobsTemplate = template.Must(template.New("webpage").Funcs(plannerJobsTemplateFuncs).Parse(plannerJobsHTML))

type plannerJobsContent struct {
	Now                string `json:"now"`
	BucketIndexUpdated string `json:"bucket_index_updated"`

	Tenant      string                 `json:"tenant"`
	PlannedJobs []plannedCompactionJob `json:"jobs"`

	ShowBlocks     bool `json:"-"`
	ShowCompactors bool `json:"-"`

	SplitJobsCount int `json:"split_jobs_count"`
	MergeJobsCount int `json:"merge_jobs_count"`

	TenantSplitGroups int `json:"tenant_split_groups"`
	TenantMergeShards int `json:"tenant_merge_shards"`
	SplitGroups       int `json:"-"`
	MergeShards       int `json:"-"`
}

type plannedCompactionJob struct {
	Key       string      `json:"key"`
	MinTime   string      `json:"min_time"`
	MaxTime   string      `json:"max_time"`
	Blocks    []ulid.ULID `json:"blocks"`
	Compactor string      `json:"compactor,omitempty"`
}

func (c *MultitenantCompactor) PlannedJobsHandler(w http.ResponseWriter, req *http.Request) {
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

	showBlocks := req.Form.Get("show_blocks") == "on"
	showCompactors := req.Form.Get("show_compactors") == "on"
	tenantSplitGroups := c.cfgProvider.CompactorSplitGroups(tenantID)

	tenantMergeShards := c.cfgProvider.CompactorSplitAndMergeShards(tenantID)

	mergeShards := tenantMergeShards
	if sc := req.Form.Get("merge_shards"); sc != "" {
		mergeShards, _ = strconv.Atoi(sc)
		if mergeShards < 0 {
			mergeShards = 0
		}
	}

	splitGroups := tenantSplitGroups
	if sc := req.Form.Get("split_groups"); sc != "" {
		splitGroups, _ = strconv.Atoi(sc)
		if splitGroups < 0 {
			splitGroups = 0
		}
	}

	idx, err := bucketindex.ReadIndex(req.Context(), c.bucketClient, tenantID, nil, c.logger)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to read bucket index for tenant while listing compaction jobs", "user", tenantID, "err", err)
		util.WriteTextResponse(w, "Failed to read bucket index for tenant")
		return
	}

	jobs, err := estimateCompactionJobsFromBucketIndex(req.Context(), tenantID, bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider), idx, c.compactorCfg.BlockRanges, mergeShards, splitGroups)
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to compute compaction jobs from bucket index for tenant while listing compaction jobs", "user", tenantID, "err", err)
		util.WriteTextResponse(w, "Failed to compute compaction jobs from bucket index")
		return
	}

	jobs = c.jobsOrder(jobs)

	plannedJobs := make([]plannedCompactionJob, 0, len(jobs))

	splitJobs, mergeJobs := 0, 0

	for _, j := range jobs {
		pj := plannedCompactionJob{
			Key:     j.Key(),
			MinTime: formatTime(timestamp.Time(j.MinTime())),
			MaxTime: formatTime(timestamp.Time(j.MaxTime())),
			Blocks:  j.IDs(),
		}

		if j.UseSplitting() {
			splitJobs++
		} else {
			mergeJobs++
		}

		if showCompactors {
			inst, err := c.shardingStrategy.instanceOwningJob(j)
			if err != nil {
				pj.Compactor = err.Error()
			} else {
				pj.Compactor = fmt.Sprintf("%s %s", inst.Id, inst.Addr)
			}
		}

		plannedJobs = append(plannedJobs, pj)
	}

	util.RenderHTTPResponse(w, plannerJobsContent{
		Now:                formatTime(time.Now()),
		BucketIndexUpdated: formatTime(idx.GetUpdatedAt()),
		Tenant:             tenantID,
		PlannedJobs:        plannedJobs,

		ShowBlocks:     showBlocks,
		ShowCompactors: showCompactors,

		TenantSplitGroups: tenantSplitGroups,
		TenantMergeShards: tenantMergeShards,
		SplitGroups:       splitGroups,
		MergeShards:       mergeShards,

		SplitJobsCount: splitJobs,
		MergeJobsCount: mergeJobs,
	}, plannerJobsTemplate, req)
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}
