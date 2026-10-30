// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

// This file implements a scratch/investigation-only, one-shot diagnostic capture for the
// "Unknown series references" WAL-replay warning. It is gated entirely behind
// HeadOptions.UnknownRefsDiagnosticsDir (empty by default, i.e. disabled) and is designed to
// never affect normal operation: every failure is logged and swallowed, never returned to Init.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/wlog"
)

const unknownRefsCaptureMarkerFile = ".captured"

// maybeCaptureUnknownRefsDiagnostics is called once per Init, after the in-order WAL (checkpoint
// + live-tail segments) has been fully replayed. If UnknownRefsDiagnosticsDir is configured, this
// Init reported at least one unknown series reference, and no prior capture has been made in that
// directory, it copies the checkpoint (if any) and every segment from firstSegment to lastSegment
// into a timestamped subdirectory, alongside a dump of replay-internal state that isn't otherwise
// recoverable from the raw WAL files, then writes a marker file so this never captures again.
func (h *Head) maybeCaptureUnknownRefsDiagnostics(checkpointDir string, hasCheckpoint bool, firstSegment, lastSegment int, multiRef map[chunks.HeadSeriesRef]chunks.HeadSeriesRef) {
	destRoot := h.opts.UnknownRefsDiagnosticsDir
	if destRoot == "" {
		return
	}
	if !h.unknownRefsDetectedThisInit.Load() {
		return
	}

	markerPath := filepath.Join(destRoot, unknownRefsCaptureMarkerFile)
	if _, err := os.Stat(markerPath); err == nil {
		h.logger.Info("Unknown series references diagnostics capture already done, skipping", "dir", destRoot)
		return
	} else if !os.IsNotExist(err) {
		h.logger.Error("Unknown series references diagnostics: failed to check marker file, skipping capture", "err", err)
		return
	}

	captureDir := filepath.Join(destRoot, time.Now().UTC().Format("20060102T150405Z"))
	h.logger.Warn("Unknown series references detected: capturing WAL diagnostics", "dir", captureDir)

	if err := os.MkdirAll(captureDir, 0o777); err != nil {
		h.logger.Error("Unknown series references diagnostics: failed to create capture dir, aborting capture", "err", err)
		return
	}

	var notes []string
	note := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		notes = append(notes, msg)
		h.logger.Warn("Unknown series references diagnostics: " + msg)
	}

	if hasCheckpoint && checkpointDir != "" {
		dst := filepath.Join(captureDir, filepath.Base(checkpointDir))
		if err := copyDir(checkpointDir, dst); err != nil {
			note("failed to copy checkpoint %s: %v", checkpointDir, err)
		} else {
			note("copied checkpoint %s -> %s", checkpointDir, dst)
		}
	} else {
		note("no checkpoint present at replay time")
	}

	if h.wal != nil {
		walDestDir := filepath.Join(captureDir, "wal")
		if err := os.MkdirAll(walDestDir, 0o777); err != nil {
			note("failed to create wal capture dir: %v", err)
		} else {
			for i := firstSegment; i <= lastSegment; i++ {
				src := wlog.SegmentName(h.wal.Dir(), i)
				dst := filepath.Join(walDestDir, filepath.Base(src))
				if err := copyFile(src, dst); err != nil {
					note("failed to copy segment %d (%s): %v", i, src, err)
					continue
				}
				note("copied segment %d -> %s", i, dst)
			}
		}
	}

	if err := h.writeUnknownRefsDiagnosticsDump(captureDir, checkpointDir, hasCheckpoint, firstSegment, lastSegment, multiRef, notes); err != nil {
		h.logger.Error("Unknown series references diagnostics: failed to write diagnostics dump", "err", err)
	}

	if err := os.WriteFile(markerPath, []byte(captureDir+"\n"), 0o666); err != nil {
		// Not fatal: worst case, a future restart captures again into a new timestamped
		// subdirectory. Logged so it's visible that the "only once" guarantee may not hold.
		h.logger.Error("Unknown series references diagnostics: failed to write marker file, a future restart may capture again", "err", err)
	}
}

// writeUnknownRefsDiagnosticsDump writes a human-readable summary of replay-internal state that
// is not recoverable from the raw WAL/checkpoint files alone: the full multiRef aliasing table
// built during this replay, and the full walExpiries table as it stands once replay completed.
func (h *Head) writeUnknownRefsDiagnosticsDump(captureDir, checkpointDir string, hasCheckpoint bool, firstSegment, lastSegment int, multiRef map[chunks.HeadSeriesRef]chunks.HeadSeriesRef, notes []string) error {
	f, err := os.Create(filepath.Join(captureDir, "diagnostics.txt"))
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "Unknown series references diagnostics capture\n")
	fmt.Fprintf(f, "captured_at: %s\n", time.Now().UTC().Format(time.RFC3339Nano))
	fmt.Fprintf(f, "has_checkpoint: %v\n", hasCheckpoint)
	fmt.Fprintf(f, "checkpoint_dir: %s\n", checkpointDir)
	fmt.Fprintf(f, "segments_read: [%d, %d]\n", firstSegment, lastSegment)
	fmt.Fprintf(f, "\nnotes:\n")
	for _, n := range notes {
		fmt.Fprintf(f, "  - %s\n", n)
	}

	fmt.Fprintf(f, "\nmultiRef (new_ref -> old_ref aliases established during this replay, %d entries):\n", len(multiRef))
	for newRef, oldRef := range multiRef {
		fmt.Fprintf(f, "  %d -> %d\n", newRef, oldRef)
	}

	h.walExpiriesMtx.Lock()
	expiries := make(map[chunks.HeadSeriesRef]int64, len(h.walExpiries))
	for ref, t := range h.walExpiries {
		expiries[ref] = t
	}
	h.walExpiriesMtx.Unlock()
	fmt.Fprintf(f, "\nwalExpiries (ref -> keepUntil, as of end of this replay, %d entries):\n", len(expiries))
	for ref, t := range expiries {
		fmt.Fprintf(f, "  %d -> %d\n", ref, t)
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o777); err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}
		if err := copyFile(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}
