// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package indexheader

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type SlowFileMount struct {
	cmd      *exec.Cmd
	mountdir string
	file     string
	pidfile  string
}

// Return the path to the mounted file.
func (s *SlowFileMount) File() string {
	return s.file
}

func (s *SlowFileMount) Close() {
	fmt.Printf("fusermount -r\n")
	err := exec.Command("fusermount", "-u", s.mountdir).Run()
	if err != nil {
		fmt.Printf("fusermount -r failed: %v\n", err)
	}

	err = s.cmd.Wait()
	if err != nil {
		fmt.Printf("nbdfuse: %v\n", err)
	}

	os.Remove(s.mountdir)
	os.Remove(s.pidfile)
}

func NewSlowFileMount(tempdir, filename string) (*SlowFileMount, error) {
	mountdir := filepath.Join(tempdir, "nbd.dir")
	file := filepath.Join(mountdir, "nbd")
	pidfile := filepath.Join(tempdir, "nbd.pidfile")

	err := os.Mkdir(mountdir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(
		"nbdfuse",
		"-P", pidfile,
		"-o", "kernel_cache",
		mountdir,
		"--command",
		"nbdkit",
		"-s",
		"--filter=delay",
		//"--filter=rate",
		"file",
		filename,
		"rdelay=5ms",
		//"rate=10K",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Start()
	if err != nil {
		os.RemoveAll(mountdir)
		return nil, err
	}

	// Wait for maximum 5s for pidfile to exist
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(pidfile); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return &SlowFileMount{
		cmd:      cmd,
		mountdir: mountdir,
		file:     file,
	}, nil
}
