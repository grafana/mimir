// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/testutil/e2eutil/copy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package e2eutil

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func Copy(t testing.TB, src, dst string) {
	testutil.Ok(t, copyRecursive(src, dst))
}

func copyRecursive(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, relPath), os.ModePerm)
		}

		if !info.Mode().IsRegular() {
			return errors.Errorf("%s is not a regular file", path)
		}

		source, err := os.Open(filepath.Clean(path))
		if err != nil {
			return err
		}
		defer runutil.CloseWithErrCapture(&err, source, "close file")

		destination, err := os.Create(filepath.Join(dst, relPath))
		if err != nil {
			return err
		}
		defer runutil.CloseWithErrCapture(&err, destination, "close file")

		_, err = io.Copy(destination, source)
		return err
	})
}
