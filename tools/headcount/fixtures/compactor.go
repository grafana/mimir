// SPDX-License-Identifier: AGPL-3.0-only

package fixtures

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// compactorOptions are the flags that differ between the handover and
// compacted snapshots; everything else about the compactor invocation is
// fixed by runCompactor.
type compactorOptions struct {
	Partitions    int
	DeletionDelay string
	// WantSources says whether level-1 blocks are expected to still be on
	// disk once the run settles: true for handover, false for compacted.
	// A mismatch means something about the run's assumptions is wrong, not
	// just that it hasn't finished yet.
	WantSources bool
}

// compactionInterval and cleanupInterval are how often the compactor
// re-checks for work. Both must stay well below stableWindow, or a run that
// is genuinely still progressing could look finished.
const (
	compactionInterval = 15 * time.Second
	cleanupInterval    = 15 * time.Second
	// stableWindow is how long the on-disk block set must be unchanged
	// before a run is considered finished. The compactor logs its own
	// per-tick boilerplate ("start of GC", "compaction iterations done")
	// every compactionInterval even when nothing changed, so log silence
	// can't be used as a completion signal; the block set itself can.
	stableWindow = 3 * (compactionInterval + cleanupInterval)
)

// compactSnapshot copies sourceDir into a scratch directory (sourceDir
// itself is left untouched, since it is typically another snapshot this
// build still needs to return), runs cfg's Mimir binary as a compactor
// against the copy with opts until the block set stabilizes, then moves
// the result into cfg.Dir/name and returns its path.
func compactSnapshot(cfg Config, sourceDir, name string, opts compactorOptions) (string, error) {
	work := filepath.Join(cfg.Dir, "work-"+name)
	if err := os.RemoveAll(work); err != nil {
		return "", err
	}
	if err := copyTree(sourceDir, work); err != nil {
		return "", fmt.Errorf("copying %s into scratch dir: %w", sourceDir, err)
	}

	dataDir := filepath.Join(cfg.Dir, "compactor-data-"+name)
	logPath := filepath.Join(cfg.Dir, name+".log")
	if err := os.RemoveAll(dataDir); err != nil {
		return "", err
	}

	cmd, logFile, err := startCompactor(cfg.MimirBinary, work, dataDir, logPath, opts)
	if err != nil {
		return "", err
	}
	defer logFile.Close()
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	const timeout = 20 * time.Minute
	if err := waitForCompaction(work, opts.WantSources, stableWindow, 5*time.Second, timeout); err != nil {
		return "", fmt.Errorf("waiting for compactor to finish (see %s): %w", logPath, err)
	}

	// work is scratch-only past this point, so move rather than copy it
	// into place: at this data volume, a duplicate copy is the difference
	// between fitting on disk and not.
	dst := filepath.Join(cfg.Dir, name)
	if err := os.RemoveAll(dst); err != nil {
		return "", err
	}
	if err := os.Rename(work, dst); err != nil {
		return "", fmt.Errorf("snapshotting %s: %w", name, err)
	}
	_ = os.RemoveAll(dataDir) // compactor scratch state; safe to lose.
	return dst, nil
}

// startCompactor launches binPath as a Mimir compactor targeting bucketDir,
// with its own ring store (so it doesn't need a running Consul/etcd) and
// ports picked freely, so multiple compactSnapshot calls never collide.
func startCompactor(binPath, bucketDir, dataDir, logPath string, opts compactorOptions) (*exec.Cmd, *os.File, error) {
	httpPort, err := freePort()
	if err != nil {
		return nil, nil, err
	}
	grpcPort, err := freePort()
	if err != nil {
		return nil, nil, err
	}

	args := []string{
		"-target=compactor",
		"-auth.multitenancy-enabled=false",
		"-blocks-storage.backend=filesystem",
		"-blocks-storage.filesystem.dir=" + bucketDir,
		"-compactor.data-dir=" + dataDir,
		"-compactor.ring.store=inmemory",
		"-compactor.block-ranges=2h,12h,24h",
		fmt.Sprintf("-compactor.split-and-merge-shards=%d", opts.Partitions),
		"-compactor.split-groups=2",
		"-compactor.first-level-compaction-wait-period=0",
		"-compactor.compaction-interval=15s",
		"-compactor.cleanup-interval=15s",
		"-compactor.deletion-delay=" + opts.DeletionDelay,
		"-compactor.max-lookback=0",
		fmt.Sprintf("-server.http-listen-port=%d", httpPort),
		fmt.Sprintf("-server.grpc-listen-port=%d", grpcPort),
	}

	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, nil, err
	}

	cmd := exec.Command(binPath, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, nil, fmt.Errorf("starting compactor: %w", err)
	}
	return cmd, logFile, nil
}

// waitForCompaction polls bucketDir's block set every pollEvery until it has
// both stopped changing for stable and reached the shape wantSources
// describes, or returns an error once timeout elapses first.
//
// Stability alone is not enough: right after the compactor starts, the
// block set is just its unchanged input, which trivially looks "stable"
// for as long as planning, downloading or a failing compaction job takes
// before it produces any output. Requiring the target shape too means a
// compactor that is still working (or stuck retrying a job that keeps
// failing) is correctly reported as not finished, rather than mistaken for
// an instantly completed run.
func waitForCompaction(bucketDir string, wantSources bool, stable, pollEvery, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	sig, err := blockSetSignature(bucketDir)
	if err != nil {
		return err
	}
	stableSince := time.Now()

	for time.Now().Before(deadline) {
		time.Sleep(pollEvery)

		next, err := blockSetSignature(bucketDir)
		if err != nil {
			return err
		}
		if next != sig {
			sig, stableSince = next, time.Now()
			continue
		}
		if time.Since(stableSince) >= stable && checkBlockLevels(bucketDir, wantSources) == nil {
			return nil
		}
	}
	if err := checkBlockLevels(bucketDir, wantSources); err != nil {
		return fmt.Errorf("timed out, and the block set still doesn't have the expected shape: %w", err)
	}
	return fmt.Errorf("block set still changing after %s", timeout)
}

// blockSetSignature summarizes bucketDir/anonymous's blocks as a stable
// string: each block's ID and compaction level, sorted. Two calls compare
// equal exactly when the same blocks, at the same compaction level, are
// present, regardless of directory-walk order.
func blockSetSignature(bucketDir string) (string, error) {
	levels, err := blockLevels(bucketDir)
	if err != nil {
		return "", err
	}
	lines := make([]string, 0, len(levels))
	for id, level := range levels {
		lines = append(lines, fmt.Sprintf("%s:%d", id, level))
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n"), nil
}

// checkBlockLevels fails if bucketDir's level-1 blocks don't match
// wantSources: present for a handover snapshot, gone for a compacted one.
func checkBlockLevels(bucketDir string, wantSources bool) error {
	levels, err := blockLevels(bucketDir)
	if err != nil {
		return err
	}
	var haveLevel1, haveHigher bool
	for _, level := range levels {
		if level == 1 {
			haveLevel1 = true
		} else {
			haveHigher = true
		}
	}
	if !haveHigher {
		return fmt.Errorf("no compacted (level > 1) blocks found; the compactor may not have run")
	}
	if haveLevel1 != wantSources {
		return fmt.Errorf("level-1 blocks present=%v, wanted %v", haveLevel1, wantSources)
	}
	return nil
}

// blockLevels maps each block ID under bucketDir/anonymous to its
// compaction level.
func blockLevels(bucketDir string) (map[string]int, error) {
	entries, err := os.ReadDir(filepath.Join(bucketDir, "anonymous"))
	if err != nil {
		return nil, err
	}

	levels := make(map[string]int, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		meta, err := block.ReadMetaFromDir(filepath.Join(bucketDir, "anonymous", e.Name()))
		if err != nil {
			continue // a block mid-write or mid-deletion; its meta.json is transiently absent or partial.
		}
		levels[meta.ULID.String()] = meta.Compaction.Level
	}
	return levels, nil
}

// freePort asks the OS for a currently unused TCP port by binding to :0 and
// immediately releasing it. There is a small unavoidable race if something
// else grabs the port before the compactor binds it, acceptable for a local
// test tool run one compactor at a time.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
