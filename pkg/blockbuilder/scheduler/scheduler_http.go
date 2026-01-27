// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	_ "embed" // Used to embed html template
	"html/template"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"

	"github.com/grafana/mimir/pkg/util"
)

var (
	//go:embed scheduler_status.gohtml
	schedulerStatusHTML     string
	schedulerStatusTemplate = template.Must(template.New("main").Parse(schedulerStatusHTML))

	//go:embed scheduler_jobs.gohtml
	schedulerJobsHTML     string
	schedulerJobsTemplate = template.Must(template.New("webpage").Parse(schedulerJobsHTML))

	//go:embed scheduler_partitions.gohtml
	schedulerPartitionsHTML     string
	schedulerPartitionsTemplate = template.Must(template.New("webpage").Parse(schedulerPartitionsHTML))
)

type schedulerStatusContents struct {
	Message string
}

type schedulerJobsContents struct {
	Now           time.Time `json:"now"`
	Jobs          []jobInfo `json:"jobs"`
	TotalJobs     int       `json:"total_jobs"`
	AssignedJobs  int       `json:"assigned_jobs"`
	PendingJobs   int       `json:"pending_jobs"`
	CompletedJobs int       `json:"completed_jobs"`
}

type schedulerPartitionsContents struct {
	Now        time.Time       `json:"now"`
	Partitions []partitionInfo `json:"partitions"`
}

type partitionInfo struct {
	Partition       int32     `json:"partition"`
	Topic           string    `json:"topic"`
	CommittedOffset int64     `json:"committed_offset"`
	PlannedOffset   int64     `json:"planned_offset"`
	EndOffset       int64     `json:"end_offset"`
	PendingJobs     int       `json:"pending_jobs"`
	LastUpdated     time.Time `json:"last_updated"`
}

func writeMessage(w http.ResponseWriter, message string, logger log.Logger) {
	w.WriteHeader(http.StatusOK)
	err := schedulerStatusTemplate.Execute(w, schedulerStatusContents{Message: message})

	if err != nil {
		level.Error(logger).Log("msg", "unable to serve scheduler status page", "err", err)
	}
}

// StatusHandler shows the overall scheduler status
func (s *BlockBuilderScheduler) StatusHandler(w http.ResponseWriter, req *http.Request) {
	if s.State() != services.Running {
		writeMessage(w, "Scheduler is not running yet.", s.logger)
		return
	}

	writeMessage(w, "Scheduler is running.", s.logger)
}

// JobsHandler shows all currently known jobs
func (s *BlockBuilderScheduler) JobsHandler(w http.ResponseWriter, req *http.Request) {
	if s.State() != services.Running {
		util.WriteTextResponse(w, "Scheduler is not running yet.")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.observationComplete {
		util.WriteTextResponse(w, "Scheduler is still in observation mode.")
		return
	}

	jobs := s.getAllJobs()
	assignedCount := 0
	pendingCount := 0

	for _, job := range jobs {
		if job.Assignee != "" {
			assignedCount++
		} else {
			pendingCount++
		}
	}

	util.RenderHTTPResponse(w, schedulerJobsContents{
		Now:           time.Now(),
		Jobs:          jobs,
		TotalJobs:     len(jobs),
		AssignedJobs:  assignedCount,
		PendingJobs:   pendingCount,
		CompletedJobs: 0, // Completed jobs are removed from the queue
	}, schedulerJobsTemplate, req)
}

// PartitionsHandler shows all known partitions and their offsets
func (s *BlockBuilderScheduler) PartitionsHandler(w http.ResponseWriter, req *http.Request) {
	if s.State() != services.Running {
		util.WriteTextResponse(w, "Scheduler is not running yet.")
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	partitions := s.getAllPartitions()

	util.RenderHTTPResponse(w, schedulerPartitionsContents{
		Now:        time.Now(),
		Partitions: partitions,
	}, schedulerPartitionsTemplate, req)
}

// getAllJobs returns information about all jobs in the scheduler
func (s *BlockBuilderScheduler) getAllJobs() []jobInfo {
	if s.jobs == nil {
		return []jobInfo{}
	}

	return s.jobs.getAllJobs()
}

// getAllPartitions returns information about all partitions
func (s *BlockBuilderScheduler) getAllPartitions() []partitionInfo {
	partitions := make([]partitionInfo, 0, len(s.partState))

	for partition, ps := range s.partState {
		committedOffset := int64(-1)
		plannedOffset := int64(-1)

		if !ps.committed.empty() {
			committedOffset = ps.committed.offset()
		}
		if !ps.planned.empty() {
			plannedOffset = ps.planned.offset()
		}

		partitions = append(partitions, partitionInfo{
			Partition:       partition,
			Topic:           ps.topic,
			CommittedOffset: committedOffset,
			PlannedOffset:   plannedOffset,
			EndOffset:       -1, // We don't track this in partitionState
			PendingJobs:     ps.pendingJobs.Len(),
			LastUpdated:     time.Now(), // We don't track this in partitionState
		})
	}

	return partitions
}
