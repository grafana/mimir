// SPDX-License-Identifier: AGPL-3.0-only

// format-markdown formats Markdown files like "prettier --write '**/*.md'", without Node.js:
// it runs prettier's own JavaScript bundles in goja, a JavaScript engine written in Go.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/dop251/goja"
	gitignore "github.com/monochromegane/go-gitignore"
	"golang.org/x/sync/errgroup"
)

const (
	prettierVersion = "3.8.4"
	// The .dist.integrity field of https://registry.npmjs.org/prettier/3.8.4.
	prettierIntegrity = "sha512-N2MylSdi48+5N/6S5j+maeHbUSIzzZ5uOcX5Hm4QpV8Dkb1HFjfAKTKX6yNPJQD9AhcT3ifHNB66tWTTJDi11Q=="
)

// shortcut: only the plugins for Markdown and for the code block languages that our docs use (yaml, json, javascript).
// Code blocks in other languages are left as they are. Add plugins here if docs start using e.g. typescript or css blocks.
var bundles = []string{"standalone.js", "plugins/markdown.js", "plugins/yaml.js", "plugins/babel.js", "plugins/estree.js"}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	programs, err := loadPrettier()
	if err != nil {
		return err
	}
	files, err := markdownFiles()
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(context.Background())
	paths := make(chan string)
	g.Go(func() error {
		defer close(paths)
		for _, p := range files {
			select {
			case paths <- p:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	// A goja runtime is not safe for concurrent use, so each worker has its own.
	for range runtime.NumCPU() {
		g.Go(func() error {
			format, err := newFormatter(programs)
			if err != nil {
				return err
			}
			for p := range paths {
				if err := formatFile(format, p); err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// loadPrettier downloads the prettier npm package, verifies it, and compiles the bundles.
func loadPrettier() ([]*goja.Program, error) {
	client := http.Client{Timeout: time.Minute}
	resp, err := client.Get("https://registry.npmjs.org/prettier/-/prettier-" + prettierVersion + ".tgz")
	if err != nil {
		return nil, fmt.Errorf("downloading prettier: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloading prettier: %s", resp.Status)
	}
	tgz, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("downloading prettier: %w", err)
	}
	sum := sha512.Sum512(tgz)
	if got := "sha512-" + base64.StdEncoding.EncodeToString(sum[:]); got != prettierIntegrity {
		return nil, fmt.Errorf("prettier package integrity is %s, expected %s", got, prettierIntegrity)
	}

	gz, err := gzip.NewReader(bytes.NewReader(tgz))
	if err != nil {
		return nil, err
	}
	sources := map[string]string{}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if name := strings.TrimPrefix(h.Name, "package/"); slices.Contains(bundles, name) {
			b, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			sources[name] = string(b)
		}
	}

	programs := make([]*goja.Program, 0, len(bundles))
	for _, name := range bundles {
		src, ok := sources[name]
		if !ok {
			return nil, fmt.Errorf("%s not found in the prettier package", name)
		}
		p, err := goja.Compile(name, src, false)
		if err != nil {
			return nil, fmt.Errorf("compiling %s: %w", name, err)
		}
		programs = append(programs, p)
	}
	return programs, nil
}

func newFormatter(programs []*goja.Program) (func(text, path string) (string, error), error) {
	vm := goja.New()
	// The bundles are UMD modules: without CommonJS or AMD they set the prettier and prettierPlugins globals.
	for _, p := range programs {
		if _, err := vm.RunProgram(p); err != nil {
			return nil, err
		}
	}
	v, err := vm.RunString(`(text, filepath) => prettier.format(text, { filepath, plugins: Object.values(prettierPlugins) })`)
	if err != nil {
		return nil, err
	}
	format, _ := goja.AssertFunction(v)

	return func(text, path string) (string, error) {
		v, err := format(goja.Undefined(), vm.ToValue(text), vm.ToValue(path))
		if err != nil {
			return "", err
		}
		// goja runs the promise jobs before the call returns, because prettier does not wait for timers or I/O.
		p, ok := v.Export().(*goja.Promise)
		if !ok {
			return "", fmt.Errorf("prettier.format returned %T, expected a promise", v.Export())
		}
		switch p.State() {
		case goja.PromiseStateFulfilled:
			return p.Result().String(), nil
		case goja.PromiseStateRejected:
			return "", errors.New(p.Result().String())
		default:
			return "", errors.New("prettier.format did not finish")
		}
	}, nil
}

func formatFile(format func(text, path string) (string, error), path string) error {
	in, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	out, err := format(string(in), path)
	if err != nil || out == string(in) {
		return err
	}
	return os.WriteFile(path, []byte(out), 0o644)
}

// markdownFiles returns the *.md files under the current directory, without the ones that
// .gitignore, .prettierignore or node_modules exclude, like the prettier CLI does.
func markdownFiles() ([]string, error) {
	var ignores []gitignore.IgnoreMatcher
	for _, name := range []string{".gitignore", ".prettierignore"} {
		m, err := gitignore.NewGitIgnore(name, ".")
		if err != nil {
			return nil, err
		}
		ignores = append(ignores, m)
	}

	var files []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == "." {
			return err
		}
		if slices.ContainsFunc(ignores, func(m gitignore.IgnoreMatcher) bool { return m.Match(path, d.IsDir()) }) || d.Name() == "node_modules" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}
