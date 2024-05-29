// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/alecthomas/kingpin/v2"

	// Dummy dependency to ensure these files get vendored and we can use them as transformation sources.
	_ "github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
)

func main() {
	app := kingpin.New("generate", "Generate OTLP translation code.")

	generate := func(*kingpin.ParseContext) error {
		sources, err := copySources()
		if err != nil {
			return err
		}
		if err := patchSources(sources); err != nil {
			return err
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

func copySources() ([]string, error) {
	dpath := filepath.Join("..", "..", "..", "vendor", "github.com", "prometheus", "prometheus", "storage", "remote", "otlptranslator", "prometheusremotewrite")
	entries, err := os.ReadDir(dpath)
	if err != nil {
		return nil, fmt.Errorf("list files in %q: %w", dpath, err)
	}

	var sources []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || e.Name() == "timeseries.go" || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}

		dst := fmt.Sprintf("%s_generated.go", e.Name()[:len(e.Name())-3])
		if err := copyFile(filepath.Join(dpath, e.Name()), dst); err != nil {
			return nil, err
		}

		sources = append(sources, dst)
	}
	return sources, nil
}

func copyFile(srcPath, dstPath string) (err error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", srcPath, err)
	}
	defer func() {
		if cErr := src.Close(); cErr != nil {
			err = errors.Join(fmt.Errorf("close %q: %w", srcPath, cErr), err)
		}
	}()

	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create %q: %w", dstPath, err)
	}
	defer func() {
		if cErr := dst.Close(); cErr != nil {
			err = errors.Join(fmt.Errorf("close %q: %w", dstPath, cErr), err)
		}
	}()

	wr := bufio.NewWriter(dst)
	if _, err := wr.WriteString("// Code generated from Prometheus sources - DO NOT EDIT.\n\n"); err != nil {
		return fmt.Errorf("write to %q: %w", dstPath, err)
	}
	if _, err := wr.ReadFrom(src); err != nil {
		return fmt.Errorf("read from file %q: %w", srcPath, err)
	}
	if err := wr.Flush(); err != nil {
		return fmt.Errorf("write to file %q: %w", dstPath, err)
	}

	return nil
}
