// SPDX-License-Identifier: AGPL-3.0-only

package fixtures

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
)

// copyTree copies src to dst, preserving file modes. dst is created; it
// must not already exist. Symlinks aren't expected in a Mimir block
// directory and are followed rather than recreated.
//
// On APFS (the default macOS filesystem), BSD cp's -c flag makes this a
// copy-on-write clone: near-instant, and consuming no extra disk space
// until either side is later modified. At the data volumes fixtures deals
// in, that is the difference between fitting on disk and not, so it is
// tried first; a filesystem that doesn't support it (a non-macOS host, or
// a destination filesystem other than APFS) falls back to a plain copy.
func copyTree(src, dst string) error {
	if err := cloneTree(src, dst); err == nil {
		return nil
	}
	_ = os.RemoveAll(dst) // discard any partial clone before falling back.
	return copyTreePlain(src, dst)
}

func cloneTree(src, dst string) error {
	return exec.Command("cp", "-c", "-R", src, dst).Run()
}

func copyTreePlain(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)

		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
