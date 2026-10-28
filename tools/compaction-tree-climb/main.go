// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/validation"
)

type config struct {
	bucket        bucket.Config
	tenant        string
	blockRanges   mimir_tsdb.DurationList
	shardCount    int
	oooShardCount int
	splitGroups   int
	groupsFile    string
	stateFile     string
	markFile      string
	unmarkFile    string
}

func main() {
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	// Clean up all flags registered via init() methods of 3rd-party libraries
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var cfg config
	cfg.bucket.RegisterFlags(flag.CommandLine)
	cfg.blockRanges = mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flag.Var(&cfg.blockRanges, "block-ranges", "List of compaction time ranges.")
	flag.StringVar(&cfg.tenant, "tenant", "", "Tenant ID.")
	flag.IntVar(&cfg.shardCount, "shard-count", 0, "Split-and-merge shard count of the tenant.")
	flag.IntVar(&cfg.oooShardCount, "ooo-shard-count", 0, "Shard count for out-of-order blocks (0 to use shard-count).")
	flag.IntVar(&cfg.splitGroups, "split-groups", 1, "Split groups of the tenant.")
	flag.StringVar(&cfg.groupsFile, "groups-file", "", "JSON file mapping each group to its block IDs.")
	flag.StringVar(&cfg.stateFile, "state-file", "chosen-groups", "File listing the groups already chosen, one per line. Groups chosen by a run are appended to it.")
	flag.StringVar(&cfg.markFile, "mark-file", "mark", "File the blocks to mark for no compaction are written to, for mimirtool blocks mark. Must not exist.")
	flag.StringVar(&cfg.unmarkFile, "unmark-file", "unmark", "File the blocks to remove no-compact marks from are written to, for mimirtool blocks mark --remove. Must not exist.")

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		level.Error(logger).Log("msg", "failed to parse flags", "err", err)
		os.Exit(1)
	}

	if cfg.tenant == "" || cfg.shardCount <= 0 || cfg.groupsFile == "" {
		level.Error(logger).Log("msg", "-tenant, -shard-count and -groups-file are required")
		os.Exit(1)
	}
	if err := tenant.ValidTenantID(cfg.tenant); err != nil {
		level.Error(logger).Log("msg", "-tenant is invalid", "err", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	if err := run(ctx, cfg, logger); err != nil {
		level.Error(logger).Log("msg", "compaction-tree-climb failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, logger log.Logger) error {
	for _, file := range []string{cfg.markFile, cfg.unmarkFile} {
		if err := checkNotExists(file); err != nil {
			return err
		}
	}
	groups, err := readGroups(cfg.groupsFile)
	if err != nil {
		return fmt.Errorf("read groups file: %w", err)
	}
	previouslyChosen, err := readState(cfg.stateFile)
	if err != nil {
		return fmt.Errorf("read state file: %w", err)
	}
	// Every prompt reads from one reader, so input buffered by one is still read by the next
	stdin := bufio.NewReader(os.Stdin)
	if len(previouslyChosen) == 0 {
		if err := confirmFirstRun(stdin, cfg.stateFile); err != nil {
			return err
		}
	}
	for g := range previouslyChosen {
		if _, ok := groups[g]; !ok {
			return fmt.Errorf("state file contains group %q which is not in the groups file", g)
		}
	}

	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	userBkt := bucket.NewUserBucketClient(cfg.tenant, bkt, nil)

	idx, err := bucketindex.ReadIndex(ctx, bkt, cfg.tenant, nil, logger)
	if err != nil {
		return fmt.Errorf("load bucket index: %w", err)
	}
	level.Info(logger).Log("msg", "loaded bucket index", "updated_at", time.Unix(idx.UpdatedAt, 0).UTC().Format(time.RFC3339))

	metas := compactor.ConvertBucketIndexToMetasForCompactionJobPlanning(idx)
	// The compactor ignores these labels when planning, so blocks that differ only by them are in the same tree
	if err := compactor.NewLabelRemoverFilter(compactor.CompactionIgnoredLabels).Filter(ctx, metas, nil); err != nil {
		return fmt.Errorf("remove ignored labels: %w", err)
	}

	noCompactFilter := compactor.NewNoCompactionMarkFilter(userBkt)

	synced := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "synced"}, []string{"state"}) // not actually read
	// The filter removes marked blocks from the map it's given, so pass a copy
	if err := noCompactFilter.Filter(ctx, maps.Clone(metas), synced); err != nil {
		return fmt.Errorf("read no-compact marks: %w", err)
	}
	noCompact := noCompactFilter.NoCompactMarkedBlocks()
	level.Info(logger).Log("msg", "loaded blocks", "blocks", len(metas), "no_compact", len(noCompact))

	ranges := cfg.blockRanges.ToMilliseconds()
	cfgProvider := newOverrides(cfg.shardCount, cfg.oooShardCount, cfg.splitGroups)
	grouper := compactor.NewSplitAndMergeGrouper(cfg.tenant, ranges, cfgProvider, log.NewNopLogger())

	choice, err := chooseGroups(logger, metas, noCompact, groups, previouslyChosen, ranges[len(ranges)-1])
	if err != nil {
		return err
	}

	if err := verify(grouper, metas, noCompact, choice); err != nil {
		return fmt.Errorf("chosen groups failed verification, nothing written: %w", err)
	}

	choice.print(os.Stdout)
	if len(choice.chosen) == 0 {
		return nil
	}

	n, err := promptGroupCount(stdin, len(choice.chosen))
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	choice.chosen = choice.chosen[:n]

	var toMark, toUnmark []ulid.ULID
	for _, g := range choice.chosen {
		toMark = append(toMark, g.toMark...)
		toUnmark = append(toUnmark, g.blocks...)
	}
	fmt.Printf("Chosen groups: %d (%d blocks to unmark, %d blocks to mark)\n", len(choice.chosen), len(toUnmark), len(toMark))

	if len(toMark) > 0 {
		if err := createFile(cfg.markFile, blocksFile(cfg.tenant, toMark)); err != nil {
			return fmt.Errorf("write mark file: %w", err)
		}
	}
	if err := createFile(cfg.unmarkFile, blocksFile(cfg.tenant, toUnmark)); err != nil {
		return fmt.Errorf("write unmark file: %w", err)
	}
	if err := appendState(cfg.stateFile, choice.chosen); err != nil {
		return fmt.Errorf("append to state file: %w", err)
	}

	// Marking first keeps the unmarked groups from ever being compacted together with the blocks being marked
	fmt.Println("\nRun these in order, with your bucket flags:")
	if len(toMark) > 0 {
		fmt.Printf("  mimirtool blocks mark --mark-type no-compact --details compaction-tree-climb --blocks-file %s\n", cfg.markFile)
	}
	fmt.Printf("  mimirtool blocks mark --mark-type no-compact --remove --blocks-file %s\n", cfg.unmarkFile)
	fmt.Printf("If they are not run, remove the last %d lines of %s:\n", len(choice.chosen), cfg.stateFile)
	for _, g := range choice.chosen {
		fmt.Printf("  %s\n", g.key)
	}

	return nil
}

// confirmFirstRun asks for confirmation that a missing or empty state file is expected. Only the first run
// should have no chosen groups, and an empty state file on a later run suggests the wrong -state-file.
func confirmFirstRun(in *bufio.Reader, stateFile string) error {
	answer, err := prompt(in, fmt.Sprintf("State file %s is missing or empty, so no groups are treated as already chosen. Is this the first run? [y/N] ", stateFile))
	if err != nil {
		return err
	}
	if !strings.EqualFold(answer, "y") && !strings.EqualFold(answer, "yes") {
		return fmt.Errorf("state file %s is missing or empty", stateFile)
	}
	return nil
}

// promptGroupCount asks how many of the available groups to choose, defaulting to all of them. Each chosen
// group is one compaction job, so the number of groups bounds the load put on compactors.
func promptGroupCount(in *bufio.Reader, available int) (int, error) {
	answer, err := prompt(in, fmt.Sprintf("\n%d groups can be chosen, one compaction job each. How many do you want to choose? [%d] ", available, available))
	if err != nil {
		return 0, err
	}
	if answer == "" {
		return available, nil
	}
	n, err := strconv.Atoi(answer)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid number of groups %q", answer)
	}
	return min(n, available), nil
}

// checkNotExists fails if a file exists, so a file from an earlier run is never overwritten or mistaken for this run's
func checkNotExists(file string) error {
	if _, err := os.Stat(file); err == nil {
		return fmt.Errorf("%s already exists", file)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check %s: %w", file, err)
	}
	return nil
}

// createFile writes a file that must not already exist
func createFile(file string, b []byte) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// blocksFile lists blocks one per line as <tenant>/<block ID>, as mimirtool blocks mark reads them without --tenant
func blocksFile(tenantID string, ids []ulid.ULID) []byte {
	var b strings.Builder
	for _, id := range ids {
		b.WriteString(tenantID)
		b.WriteByte('/')
		b.WriteString(id.String())
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// appendState records groups as chosen. While a recorded group's blocks are unmarked its tree stays awaiting compaction,
// and once they are compacted or marked again the tree moves on to its next group.
func appendState(file string, groups []*chosenGroup) error {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if _, err := fmt.Fprintln(f, g.key); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

// prompt asks a question and returns the trimmed answer read from in. It fails if in is closed before
// anything is read, so a closed input is never taken as the default answer.
func prompt(in *bufio.Reader, question string) (string, error) {
	fmt.Print(question)
	answer, err := in.ReadString('\n')
	if err != nil && (!errors.Is(err, io.EOF) || answer == "") {
		return "", fmt.Errorf("read answer: %w", err)
	}
	return strings.TrimSpace(answer), nil
}

// readGroups reads a JSON object mapping group keys to block IDs
func readGroups(file string) (map[string][]ulid.ULID, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var raw map[string][]string
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}

	groups := make(map[string][]ulid.ULID, len(raw))
	seen := map[ulid.ULID]string{}
	for key, ids := range raw {
		if len(ids) < 2 {
			return nil, fmt.Errorf("group %s has fewer than 2 blocks", key)
		}
		for _, s := range ids {
			id, err := ulid.Parse(s)
			if err != nil {
				return nil, fmt.Errorf("group %s: parse block ID %q: %w", key, s, err)
			}
			if other, ok := seen[id]; ok {
				return nil, fmt.Errorf("block %s is in both group %s and group %s", id, other, key)
			}
			seen[id] = key
			groups[key] = append(groups[key], id)
		}
	}
	return groups, nil
}

// readState reads the groups chosen by earlier runs, one per line, or none if the file is missing
func readState(file string) (map[string]struct{}, error) {
	previouslyChosen := map[string]struct{}{}

	f, err := os.Open(file)
	if errors.Is(err, os.ErrNotExist) {
		return previouslyChosen, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		if line := strings.TrimSpace(s.Text()); line != "" {
			previouslyChosen[line] = struct{}{}
		}
	}
	return previouslyChosen, s.Err()
}

// treeKey identifies the blocks that compaction would eventually merge together:
// blocks with the same external labels and shard ID within the same largest range.
type treeKey struct {
	labels     string
	shardID    string
	rangeStart int64
}

func (k treeKey) String() string {
	return fmt.Sprintf("%s %s %s", k.labels, k.shardID, time.UnixMilli(k.rangeStart).UTC().Format(time.RFC3339))
}

// treeOf returns the tree of a block, or false if the block has no shard ID
func treeOf(m *block.Meta, largestRange int64) (treeKey, bool) {
	shardID := m.Thanos.Labels[block.CompactorShardIDExternalLabel]
	if shardID == "" {
		return treeKey{}, false
	}

	b := labels.NewScratchBuilder(len(m.Thanos.Labels))
	for k, v := range m.Thanos.Labels {
		if k != block.CompactorShardIDExternalLabel {
			b.Add(k, v)
		}
	}
	b.Sort()

	// Block timestamps are never negative, so a plain division aligns the range
	return treeKey{
		labels:     fmt.Sprintf("%d@%s", m.Thanos.Downsample.Resolution, b.Labels().String()),
		shardID:    shardID,
		rangeStart: largestRange * (m.MinTime / largestRange),
	}, true
}

type chosenGroup struct {
	key    string
	blocks []ulid.ULID
	toMark []ulid.ULID
}

type groupChoice struct {
	chosen []*chosenGroup

	numGroupsRemaining int
	numAwaitingTrees   int
}

func (c *groupChoice) print(w io.Writer) {
	tabber := tabwriter.NewWriter(w, 1, 4, 3, ' ', 0)
	fmt.Fprintf(tabber, "Group\tUnmark\tMark\n")
	var unmarkCount, markCount int
	for _, g := range c.chosen {
		fmt.Fprintf(tabber, "%s\t%d\t%s\n", g.key, len(g.blocks), idList(g.toMark))
		unmarkCount += len(g.blocks)
		markCount += len(g.toMark)
	}
	tabber.Flush()

	fmt.Fprintf(w, "\nRemaining groups: %d\n", c.numGroupsRemaining)
	fmt.Fprintf(w, "Trees awaiting compaction: %d\n", c.numAwaitingTrees)
	fmt.Fprintf(w, "Available groups: %d (%d blocks to unmark, %d blocks to mark)\n", len(c.chosen), unmarkCount, markCount)
}

// chooseGroups picks the first remaining group of each tree that has no unmarked group still awaiting compaction,
// and finds the blocks of that tree which must be marked so the group compacts into a block nothing merges with.
func chooseGroups(logger log.Logger, metas map[ulid.ULID]*block.Meta, noCompact map[ulid.ULID]struct{}, groups map[string][]ulid.ULID, previouslyChosen map[string]struct{}, largestRange int64) (*groupChoice, error) {
	res := &groupChoice{}

	trees := map[treeKey][]ulid.ULID{}
	for id, m := range metas {
		if k, ok := treeOf(m, largestRange); ok {
			trees[k] = append(trees[k], id)
		}
	}

	// groupTree returns the tree of a group and the group's blocks that still exist. Blocks of
	// previously chosen groups may have been compacted away.
	groupTree := func(key string) (treeKey, []ulid.ULID, error) {
		var (
			tree    treeKey
			present []ulid.ULID
		)
		for _, id := range groups[key] {
			m, ok := metas[id]
			if !ok {
				continue
			}
			k, ok := treeOf(m, largestRange)
			if !ok {
				return treeKey{}, nil, fmt.Errorf("group %s: block %s has no shard ID", key, id)
			}
			if len(present) > 0 && k != tree {
				return treeKey{}, nil, fmt.Errorf("group %s: blocks belong to different trees (%s and %s)", key, tree, k)
			}
			tree = k
			present = append(present, id)
		}
		return tree, present, nil
	}

	awaiting := map[treeKey]struct{}{}
	for key := range previouslyChosen {
		tree, present, err := groupTree(key)
		if err != nil {
			return nil, err
		}
		for _, id := range present {
			if _, marked := noCompact[id]; !marked {
				awaiting[tree] = struct{}{}
				break
			}
		}
	}
	res.numAwaitingTrees = len(awaiting)

	var missing []ulid.ULID
	for _, key := range slices.Sorted(maps.Keys(groups)) {
		if _, ok := previouslyChosen[key]; ok {
			continue
		}
		// A missing block may have already been deleted or been part of a partial compaction. Leave such blocks out of the group.
		for _, id := range groups[key] {
			if _, ok := metas[id]; !ok {
				missing = append(missing, id)
			}
		}
		tree, present, err := groupTree(key)
		if err != nil {
			return nil, err
		}
		// A single block isn't planned for compaction on its own
		if len(present) < 2 {
			continue
		}
		res.numGroupsRemaining++

		if _, ok := awaiting[tree]; ok {
			continue
		}
		// A tree with a chosen group is awaiting compaction until the group compacts
		awaiting[tree] = struct{}{}

		g := &chosenGroup{key: key, blocks: present}
		for _, id := range trees[tree] {
			if _, marked := noCompact[id]; marked || slices.Contains(present, id) {
				continue
			}
			g.toMark = append(g.toMark, id)
		}
		slices.SortFunc(g.toMark, ulid.ULID.Compare)
		res.chosen = append(res.chosen, g)
	}

	if len(missing) > 0 {
		slices.SortFunc(missing, ulid.ULID.Compare)
		level.Warn(logger).Log("msg", "left blocks not in the bucket index out of their groups", "blocks", len(missing), "examples", idList(missing[:min(len(missing), 5)]))
	}
	return res, nil
}

// verify runs the compaction planner against the state after the chosen groups are unmarked and their blocks
// to mark are marked. Each chosen group must compact on its own, and the blocks it compacts into must not be
// planned for any further compaction.
func verify(grouper *compactor.SplitAndMergeGrouper, metas map[ulid.ULID]*block.Meta, noCompact map[ulid.ULID]struct{}, c *groupChoice) error {
	if len(c.chosen) == 0 {
		return nil
	}

	toMark := map[ulid.ULID]struct{}{}
	chosenByBlock := map[ulid.ULID]*chosenGroup{}
	for _, g := range c.chosen {
		for _, id := range g.toMark {
			toMark[id] = struct{}{}
		}
		for _, id := range g.blocks {
			chosenByBlock[id] = g
		}
	}

	// The blocks compaction sees once the files are applied: chosen groups unmarked, toMark marked
	compactable := map[ulid.ULID]*block.Meta{}
	for id, m := range metas {
		_, marked := noCompact[id]
		_, chosen := chosenByBlock[id]
		_, marking := toMark[id]
		if (!marked || chosen) && !marking {
			compactable[id] = m
		}
	}

	jobs, err := grouper.Groups(compactable)
	if err != nil {
		return fmt.Errorf("plan compaction with chosen groups unmarked: %w", err)
	}
	planned := map[*chosenGroup]struct{}{}
	for _, job := range jobs {
		var g *chosenGroup
		for _, id := range job.IDs() {
			if g = chosenByBlock[id]; g != nil {
				break
			}
		}
		if g == nil {
			continue
		}
		if !sameIDs(job.IDs(), g.blocks) {
			return fmt.Errorf("group %s: expected a job with its %d blocks, but planned job %s with %d blocks", g.key, len(g.blocks), job.Key(), len(job.IDs()))
		}
		planned[g] = struct{}{}
	}
	for _, g := range c.chosen {
		if _, ok := planned[g]; !ok {
			return fmt.Errorf("group %s: no compaction job planned", g.key)
		}
	}

	// Replace each chosen group with the block it compacts into
	compactedBlocks := map[ulid.ULID]*chosenGroup{}
	for _, g := range c.chosen {
		first := metas[g.blocks[0]]
		out := &block.Meta{
			BlockMeta: first.BlockMeta,
			Thanos: block.ThanosMeta{
				Labels:     first.Thanos.Labels,
				Downsample: first.Thanos.Downsample,
			},
		}
		out.ULID = ulid.Make()
		for _, id := range g.blocks {
			m := metas[id]
			out.MinTime = min(out.MinTime, m.MinTime)
			out.MaxTime = max(out.MaxTime, m.MaxTime)
			out.Compaction.Level = max(out.Compaction.Level, m.Compaction.Level)
			delete(compactable, id)
		}
		out.Compaction.Level++
		compactable[out.ULID] = out
		compactedBlocks[out.ULID] = g
	}

	jobs, err = grouper.Groups(compactable)
	if err != nil {
		return fmt.Errorf("plan compaction with chosen groups compacted: %w", err)
	}
	for _, job := range jobs {
		for _, id := range job.IDs() {
			if g := compactedBlocks[id]; g != nil {
				return fmt.Errorf("group %s: compacted block would be compacted again by job %s", g.key, job.Key())
			}
		}
	}

	return nil
}

func idStrings(ids []ulid.ULID) []string {
	s := make([]string, len(ids))
	for i, id := range ids {
		s[i] = id.String()
	}
	return s
}

// sameIDs reports whether a and b hold the same IDs, in any order
func sameIDs(a, b []ulid.ULID) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.SortFunc(a, ulid.ULID.Compare)
	slices.SortFunc(b, ulid.ULID.Compare)
	return slices.Equal(a, b)
}

func idList(ids []ulid.ULID) string {
	if len(ids) == 0 {
		return "-"
	}
	return strings.Join(idStrings(ids), ",")
}

// newOverrides returns default limits with the tenant's compactor sharding applied
func newOverrides(shardCount, oooShardCount, splitGroups int) *validation.Overrides {
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.CompactorSplitAndMergeShards = shardCount
	limits.CompactorOOOSplitAndMergeShards = oooShardCount
	limits.CompactorSplitGroups = splitGroups
	return validation.NewOverrides(limits, nil)
}
