// SPDX-License-Identifier: AGPL-3.0-only

package fs

import (
	"os"
	"path/filepath"
)

// DirExists tells whether dir exists.
func DirExists(dir string) (bool, error) {
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsDirReadWritable checks if the dir is writable and readable by the process.
func IsDirReadWritable(dir string) error {
	f := filepath.Join(dir, ".check")
	if err := os.WriteFile(f, []byte(""), 0o600); err != nil {
		return err
	}
	defer os.Remove(f)

	if _, err := os.ReadFile(f); err != nil {
		return err
	}
	return nil
}
