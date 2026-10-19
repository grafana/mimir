// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"time"
)

// A results directory holds a directory per run, with its report.json and report.html, and an index.html of them all.

//go:embed index.html
var indexHTML []byte

// saveResult copies the run's report into the results directory, with the profiles and slow reads of the run's
// directory, and writes the index again.
func saveResult(results string, report Report, runDir string) (string, error) {
	started := time.Unix(report.Started, 0).UTC()
	if report.Started == 0 {
		started = time.Now().UTC()
	}
	revision := report.Config["bench_revision"]
	if len(revision) > 10 {
		revision = revision[:10]
	}
	dir := filepath.Join(results, started.Format("20060102-150405")+"-"+safeName(revision))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := writeJSON(filepath.Join(dir, "report.json"), report); err != nil {
		return "", err
	}
	if err := writeHTML(filepath.Join(dir, "report.html"), report); err != nil {
		return "", err
	}
	if runDir != "" {
		files, _ := filepath.Glob(filepath.Join(runDir, "*.pb.gz"))
		files = append(files, filepath.Join(runDir, "slow.log"))
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				continue
			}
			if err := os.WriteFile(filepath.Join(dir, filepath.Base(file)), data, 0o644); err != nil {
				return "", err
			}
		}
	}
	return dir, writeIndex(results)
}

// addResult adds a report.json from elsewhere, such as a pod's, to the results directory.
func addResult(results, path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	return saveResult(results, report, filepath.Dir(path))
}

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func safeName(s string) string {
	if s = unsafeName.ReplaceAllString(s, "_"); s == "" {
		return "unknown"
	}
	return s
}

// RunSummary is what the index shows of a run.
type RunSummary struct {
	Dir         string               `json:"dir"`
	Started     int64                `json:"started"`
	Ingesters   [2]string            `json:"ingesters"`
	Series      int                  `json:"series"`
	Tenants     int                  `json:"tenants"`
	SamplesPerS float64              `json:"samples_per_second"`
	Replay      [2]float64           `json:"replay_seconds"`
	WindowS     float64              `json:"window_seconds"`
	Resources   [2]Resources         `json:"resources"`
	APIs        map[string]APIReport `json:"apis"`
	Config      map[string]string    `json:"config"`
	// The settings that make runs comparable with each other.
	Setup string `json:"setup"`
}

// writeIndex writes the index.html of every run in the results directory.
func writeIndex(results string) error {
	entries, err := os.ReadDir(results)
	if err != nil {
		return err
	}
	var runs []RunSummary
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(results, entry.Name(), "report.json"))
		if err != nil {
			continue
		}
		// The summary leaves the timeline out, which is most of a report.
		var run RunSummary
		if err := json.Unmarshal(data, &run); err != nil {
			continue
		}
		run.Dir = entry.Name()
		run.Setup = setupOf(run)
		runs = append(runs, run)
	}
	sort.Slice(runs, func(a, b int) bool { return runs[a].Started < runs[b].Started })
	data, err := json.Marshal(runs)
	if err != nil {
		return err
	}
	data = bytes.ReplaceAll(data, []byte("</"), []byte(`<\/`))
	page := bytes.Replace(indexHTML, []byte("__RUNS_DATA__"), data, 1)
	return os.WriteFile(filepath.Join(results, "index.html"), page, 0o644)
}

// setupOf names what was run: runs of the same setup compare, the others less so.
func setupOf(run RunSummary) string {
	c := run.Config
	return fmt.Sprintf("%d series, %d tenants, %s backfill, profile %s, query scale %s, %s vs %s",
		run.Series, run.Tenants, c["backfill"], c["profile"], c["query_scale"], run.Ingesters[0], run.Ingesters[1])
}

// benchRevision is the commit the bench was built from, which a binary built from the same tree has too.
func benchRevision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	revision, modified := "unknown", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if modified {
		revision += "-dirty"
	}
	return revision
}
