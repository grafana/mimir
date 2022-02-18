// SPDX-License-Identifier: AGPL-3.0-only

package fs

import (
	"fmt"
	"os"
	"path/filepath"
)

// DirExists tells whether dir exists.
func DirExists(dir string) (bool, error) {
	inf, err := os.Stat(dir)
	if err == nil {
		if inf.IsDir() {
			return true, nil
		}
		return false, fmt.Errorf("%s is not a directory", dir)
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
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
