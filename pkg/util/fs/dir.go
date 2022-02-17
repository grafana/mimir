// SPDX-License-Identifier: AGPL-3.0-only

package fs

import (
	"os"
	"path/filepath"
)

// IsDirReadWritable checks if the dir is writable and readable by the process.
func IsDirReadWritable(dir string) error {
	f := filepath.Join(dir, ".check")
	if err := os.WriteFile(f, []byte(""), 0o600); err != nil {
		return err
	}
	if _, err := os.ReadFile(f); err != nil {
		return err
	}
	_ = os.Remove(f)
	return nil
}
