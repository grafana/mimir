// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/grafana/regexp"

	"github.com/google/go-github/v57/github"
)

const (
	dpath = "storage/remote/otlptranslator/prometheusremotewrite"
	owner = "grafana"
	repo  = "mimir-prometheus"
)

func main() {
	app := kingpin.New("generate", "Generate OTLP translation code.")
	checkFlag := app.Flag("check", "Verify that generated OTLP translation code corresponds to mimir-prometheus version.").Bool()

	generate := func(*kingpin.ParseContext) error {
		sourceRefPath := filepath.Join("cmd", "generate", ".source-ref")

		if *checkFlag {
			return check(sourceRefPath)
		}

		sources, sourceRef, err := downloadSources(sourceRefPath)
		if err != nil {
			return err
		}
		if sourceRef == "" {
			// The source hasn't changed since last time
			return nil
		}

		if err := patchSources(sources); err != nil {
			return err
		}

		if err := os.WriteFile(sourceRefPath, []byte(sourceRef+"\n"), 0644); err != nil {
			return fmt.Errorf("couldn't write to %s: %w", sourceRefPath, err)
		}

		return nil
	}

	app = app.Action(generate)
	kingpin.MustParse(app.Parse(os.Args[1:]))
}

func patchSources(sources []string) error {
	if out, err := exec.Command("gopatch", "-p", "cmd/generate/mimirpb.patch", "./").CombinedOutput(); err != nil {
		return fmt.Errorf("failed to execute gopatch: %s", out)
	}
	sed := "sed"
	if runtime.GOOS == "darwin" {
		sed = "gsed"
	}
	for _, fname := range sources {
		if out, err := exec.Command(sed, "-i", "s/PrometheusConverter/MimirConverter/g", fname).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to execute sed: %s", out)
		}
		if out, err := exec.Command(sed, "-i", "s/Prometheus remote write format/Mimir remote write format/g", fname).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to execute sed: %s", out)
		}
		if out, err := exec.Command("goimports", "-w", "-local", "github.com/grafana/mimir", fname).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to execute goimports: %s", out)
		}
	}

	return nil
}

// getSourceRef gets the mimir-prometheus source GitHub reference hash,
// and whether it's different from the source ref the code was generated from.
func getSourceRef(sourceRefPath string) (string, bool, error) {
	var curSourceRef string
	data, err := os.ReadFile(sourceRefPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", false, fmt.Errorf("couldn't read file %s: %w", sourceRefPath, err)
		}
	} else {
		curSourceRef = strings.TrimSpace(string(data))
	}

	f, err := os.Open(filepath.Join("..", "..", "..", "go.mod"))
	if err != nil {
		return "", false, err
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
		return "", false, err
	}

	if ref == "" {
		return "", false, fmt.Errorf("couldn't find mimir-prometheus replace directive in go.mod")
	}

	return ref, ref != curSourceRef, nil
}

func downloadSources(sourceRefPath string) ([]string, string, error) {
	ref, changed, err := getSourceRef(sourceRefPath)
	if err != nil {
		return nil, "", err
	}
	if !changed {
		return nil, "", nil
	}

	ctx := context.Background()

	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return nil, "", fmt.Errorf("$GITHUB_TOKEN must be set")
	}
	client := github.NewClient(nil).WithAuthToken(token)
	_, contents, _, err := client.Repositories.GetContents(
		ctx, owner, repo, dpath, &github.RepositoryContentGetOptions{
			Ref: ref,
		},
	)
	if err != nil {
		return nil, "", fmt.Errorf("getting repo dir contents: %w", err)
	}

	var sources []string
	for _, c := range contents {
		if *c.Type != "file" || !strings.HasSuffix(*c.Name, ".go") || *c.Name == "timeseries.go" || strings.HasSuffix(*c.Name, "_test.go") {
			continue
		}

		fname, err := downloadFile(ctx, client, ref, *c.Name)
		if err != nil {
			return nil, "", err
		}
		sources = append(sources, fname)
	}

	return sources, ref, nil
}

func downloadFile(ctx context.Context, client *github.Client, ref, fname string) (string, error) {
	ghFpath := path.Join(dpath, fname)
	r, _, err := client.Repositories.DownloadContents(
		ctx, owner, repo, ghFpath, &github.RepositoryContentGetOptions{
			Ref: ref,
		},
	)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", ghFpath, err)
	}
	defer r.Close()

	reFilename := regexp.MustCompile(`^(.+)\.go$`)
	ms := reFilename.FindStringSubmatch(fname)
	outputFname := fmt.Sprintf("%s_generated.go", ms[1])

	f, err := os.OpenFile(outputFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", fmt.Errorf("open file %s: %w", outputFname, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close %s: %w", outputFname, closeErr)
		}
	}()

	wr := bufio.NewWriter(f)
	if _, err := wr.WriteString("// Code generated from Prometheus sources - DO NOT EDIT.\n\n"); err != nil {
		return "", fmt.Errorf("write to %s: %w", outputFname, err)
	}
	if _, err := wr.ReadFrom(r); err != nil {
		return "", fmt.Errorf("read from GitHub repo file %s: %w", ghFpath, err)
	}
	if err := wr.Flush(); err != nil {
		return "", fmt.Errorf("write to file %s: %w", outputFname, err)
	}

	return outputFname, nil
}

// check verifies that the generated OTLP translation sources correspond to the current mimir-promethus version depended on.
func check(sourceRefPath string) error {
	_, changed, err := getSourceRef(sourceRefPath)
	if err != nil {
		return err
	}
	if changed {
		return fmt.Errorf("the OTLP translation code was generated from an older version of mimir-prometheus - " +
			"please regenerate and commit: make generate-otlp")
	}

	return nil
}
