// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/google/go-github/v61/github"
)

type templateContext struct {
	Name                    string
	PbPackagePath           string
	PbPackage               string
	Package                 string
	LabelType               string
	SampleTimestampField    string
	ExemplarTimestampField  string
	HistogramTimestampField string
	UnknownType             string
	GaugeType               string
	CounterType             string
	HistogramType           string
	SummaryType             string
}

const (
	owner = "grafana"
	repo  = "mimir-prometheus"
	dpath = "storage/remote/otlptranslator/prometheusremotewrite/templates"
)

func main() {
	f, err := os.Open(filepath.Join("..", "..", "..", "go.mod"))
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reReplace := regexp.MustCompile(`^replace github.com/prometheus/prometheus => github.com/grafana/mimir-prometheus .+-([^-]+)$`)
	var ref string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		l := scanner.Text()
		ms := reReplace.FindStringSubmatch(l)
		if ms == nil {
			continue
		}

		ref = ms[1]
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}

	if ref == "" {
		panic(fmt.Errorf("Couldn't find mimir-prometheus replace directive in go.mod"))
	}

	ctx := context.Background()

	token := os.Getenv("GITHUB_TOKEN")
	client := github.NewClient(nil).WithAuthToken(token)
	_, contents, _, err := client.Repositories.GetContents(
		ctx, owner, repo, dpath, &github.RepositoryContentGetOptions{
			Ref: ref,
		},
	)
	if err != nil {
		panic(fmt.Errorf("getting repo dir contents: %w", err))
	}

	if err := os.MkdirAll("templates", 0755); err != nil {
		panic(err)
	}

	var templateFiles []string
	for _, c := range contents {
		if *c.Type != "file" || !strings.HasSuffix(*c.Name, ".go.tmpl") {
			continue
		}

		if err := downloadFile(ctx, client, ref, *c.Name); err != nil {
			panic(err)
		}
		templateFiles = append(templateFiles, filepath.Join("templates", *c.Name))
	}

	c := templateContext{
		Name:                    "Mimir",
		Package:                 "otlp",
		PbPackagePath:           "github.com/grafana/mimir/pkg/mimirpb",
		PbPackage:               "mimirpb",
		LabelType:               "LabelAdapter",
		SampleTimestampField:    "TimestampMs",
		ExemplarTimestampField:  "TimestampMs",
		HistogramTimestampField: "Timestamp",
		UnknownType:             "UNKNOWN",
		GaugeType:               "GAUGE",
		CounterType:             "COUNTER",
		HistogramType:           "HISTOGRAM",
		SummaryType:             "SUMMARY",
	}

	for _, fpath := range templateFiles {
		t, err := template.ParseFiles(fpath)
		if err != nil {
			panic(err)
		}

		name, _, found := strings.Cut(filepath.Base(fpath), ".")
		if !found {
			panic(fmt.Errorf("invalid filename %q", fpath))
		}
		if err := executeTemplate(t, name, c); err != nil {
			panic(err)
		}
	}
}

func downloadFile(ctx context.Context, client *github.Client, ref, fname string) (err error) {
	ghFpath := path.Join(dpath, fname)
	r, _, err := client.Repositories.DownloadContents(
		ctx, owner, repo, ghFpath, &github.RepositoryContentGetOptions{
			Ref: ref,
		},
	)
	if err != nil {
		return fmt.Errorf("download %s: %w", ghFpath, err)
	}
	defer r.Close()

	fpath := filepath.Join("templates", fname)
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open file %s: %w", fpath, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", fpath, closeErr)
		}
	}()

	wr := bufio.NewWriter(f)
	if _, err := wr.ReadFrom(r); err != nil {
		return fmt.Errorf("read from GitHub repo file %s: %w", fpath, err)
	}
	if err := wr.Flush(); err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	return nil
}

func executeTemplate(t *template.Template, name string, c templateContext) (err error) {
	fname := fmt.Sprintf("%s_generated.go", name)
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", fname, closeErr)
		}
	}()

	if _, err := f.WriteString("// Code generated from templates/*.go.tmpl - DO NOT EDIT.\n\n"); err != nil {
		return err
	}

	if err := t.Execute(f, c); err != nil {
		return fmt.Errorf("execute template: %w", err)
	}

	if out, err := exec.Command("goimports", "-w", "-local", "github.com/grafana/mimir", fname).CombinedOutput(); err != nil {
		return fmt.Errorf("execute goimports on %s: %s", fname, out)
	}

	return nil
}
