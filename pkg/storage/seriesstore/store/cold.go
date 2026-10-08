// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// Series that left the emulated head live in immutable files like the Go ingester's blocks: their
// labels, chunk references and postings stay on disk, memory-mapped, so millions of series that
// only queries of older ranges read cost no heap. The chunks themselves stay in the chunk files.

var coldMagic = []byte("MIMIRCB1")

// frozenSeries is a series leaving the head, with every chunk it had in the chunk files.
type frozenSeries struct {
	tenant          string
	labels          labels.Labels
	chunks          chunkList
	nativeHistogram bool
}

type coldTenantIndex struct {
	minTime, maxTime int64
	// Byte offsets into the file of the series offsets table and the postings table.
	seriesTable   int
	seriesCount   int
	postingsTable int
	postingsCount int
	// Each series' `labels.StableHash`, by which sharded queries pick series: computed on the first
	// sharded query rather than from the labels for every series of every one.
	shardHashesOnce sync.Once
	shardHashes     []uint64
	// Each series' labels hash, by which label lookups tell which emulated Prometheus blocks hold
	// it: computed once rather than by rebuilding every series' labels on every lookup.
	labelHashesOnce sync.Once
	labelHashes     []uint64
	// Each metric name, sorted, with its series: built on the first query by a name matcher other
	// than an equality, from the series' own labels, since the postings only hold values' hashes.
	metricNamesOnce sync.Once
	metricNames     []coldMetricName
	// The series with each label queries asked for: the block never changes.
	withLabelLock sync.Mutex
	withLabel     map[string][]uint32
	// The series of the names each name matcher other than an equality accepted.
	nameMatchesLock sync.Mutex
	nameMatches     map[nameMatcherKey][]uint32
}

type coldMetricName struct {
	name   string
	series []uint32
}

// coldBlock is one cold block of a store shard.
type coldBlock struct {
	id               uint64
	minTime, maxTime int64
	path             string
	data             []byte
	mapped           bool
	// File-local label name ids to names, and back.
	names    []string
	localIDs map[string]uint32
	tenants  map[string]*coldTenantIndex
	// The tenant and its table when the block has only one.
	onlyName string
	only     *coldTenantIndex
}

func coldBlockPath(directory string, id uint64) string {
	return filepath.Join(directory, fmt.Sprintf("cold-%016x", id))
}

// buildColdBlock writes series to a new block in directory, or keeps it in memory without one.
func buildColdBlock(directory string, id uint64, series []frozenSeries) (*coldBlock, error) {
	sort.SliceStable(series, func(a, b int) bool {
		if series[a].tenant != series[b].tenant {
			return series[a].tenant < series[b].tenant
		}
		return labels.Compare(series[a].labels, series[b].labels) < 0
	})
	var names []string
	local := map[string]uint32{}
	for index := range series {
		series[index].labels.Range(func(name, _ string) {
			if _, ok := local[name]; !ok {
				local[name] = uint32(len(names))
				names = append(names, name)
			}
		})
	}
	out := append([]byte(nil), coldMagic...)
	out = putUvarint(out, uint64(len(names)))
	for _, name := range names {
		out = putUvarint(out, uint64(len(name)))
		out = append(out, name...)
	}
	// Tenants: id, time range and where their tables are, patched once written.
	type tenantRange struct{ start, end int }
	var ranges []tenantRange
	for start := 0; start < len(series); {
		end := start
		for end < len(series) && series[end].tenant == series[start].tenant {
			end++
		}
		ranges = append(ranges, tenantRange{start, end})
		start = end
	}
	out = putUvarint(out, uint64(len(ranges)))
	headers := make([]int, len(ranges))
	for index, r := range ranges {
		tenant := series[r.start].tenant
		out = putUvarint(out, uint64(len(tenant)))
		out = append(out, tenant...)
		headers[index] = len(out)
		// min, max, series table, series count, postings table, postings count.
		out = append(out, make([]byte, 48)...)
	}
	blockMin, blockMax := int64(math.MaxInt64), int64(math.MinInt64)
	type posting struct {
		name  uint32
		hash  uint64
		index uint32
	}
	var labelBuffer []byte
	for rangeIndex, r := range ranges {
		offsets := make([]uint64, 0, r.end-r.start)
		var postings []posting
		minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
		for index, frozen := range series[r.start:r.end] {
			offsets = append(offsets, uint64(len(out)))
			labelBuffer = labelBuffer[:0]
			frozen.labels.Range(func(name, value string) {
				labelBuffer = putUvarint(labelBuffer, uint64(local[name]))
				labelBuffer = putUvarint(labelBuffer, uint64(len(value)))
				labelBuffer = append(labelBuffer, value...)
				postings = append(postings, posting{local[name], labels.ValueHash(value), uint32(index)})
			})
			out = putUvarint(out, uint64(len(labelBuffer)))
			out = append(out, labelBuffer...)
			out = putUvarint(out, uint64(len(frozen.chunks)))
			out = append(out, frozen.chunks...)
			histogram := byte(0)
			if frozen.nativeHistogram {
				histogram = 1
			}
			out = append(out, histogram)
			it := frozen.chunks.iter()
			for chunk, more := it.next(); more; chunk, more = it.next() {
				minTime = min(minTime, chunk.MinTime)
				maxTime = max(maxTime, chunk.MaxTime)
			}
		}
		seriesTable := len(out)
		for _, offset := range offsets {
			out = binary.LittleEndian.AppendUint64(out, offset)
		}
		slices.SortFunc(postings, func(a, b posting) int {
			switch {
			case a.name != b.name:
				if a.name < b.name {
					return -1
				}
				return 1
			case a.hash != b.hash:
				if a.hash < b.hash {
					return -1
				}
				return 1
			case a.index != b.index:
				if a.index < b.index {
					return -1
				}
				return 1
			}
			return 0
		})
		// Entries of (name, value hash, list offset), then the lists of series indexes.
		type entry struct {
			name uint32
			hash uint64
			list []uint32
		}
		var entries []entry
		for _, p := range postings {
			if n := len(entries); n > 0 && entries[n-1].name == p.name && entries[n-1].hash == p.hash {
				entries[n-1].list = append(entries[n-1].list, p.index)
				continue
			}
			entries = append(entries, entry{p.name, p.hash, []uint32{p.index}})
		}
		postingsTable := len(out)
		var listOffset uint32
		for _, e := range entries {
			out = binary.LittleEndian.AppendUint32(out, e.name)
			out = binary.LittleEndian.AppendUint64(out, e.hash)
			out = binary.LittleEndian.AppendUint32(out, listOffset)
			listOffset += 4 + 4*uint32(len(e.list))
		}
		for _, e := range entries {
			out = binary.LittleEndian.AppendUint32(out, uint32(len(e.list)))
			for _, index := range e.list {
				out = binary.LittleEndian.AppendUint32(out, index)
			}
		}
		fields := [6]uint64{uint64(minTime), uint64(maxTime), uint64(seriesTable), uint64(len(offsets)), uint64(postingsTable), uint64(len(entries))}
		for slot, field := range fields {
			binary.LittleEndian.PutUint64(out[headers[rangeIndex]+slot*8:], field)
		}
		blockMin = min(blockMin, minTime)
		blockMax = max(blockMax, maxTime)
	}
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(out))
	var (
		block *coldBlock
		err   error
	)
	if directory == "" {
		block, err = parseColdBlock(id, out, false, "")
	} else {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, fmt.Errorf("create cold block directory %s: %w", directory, err)
		}
		path := coldBlockPath(directory, id)
		temporary := path + ".tmp"
		if err := writeSynced(temporary, out); err != nil {
			return nil, fmt.Errorf("create cold block %s: %w", temporary, err)
		}
		if err := os.Rename(temporary, path); err != nil {
			return nil, err
		}
		block, err = openColdBlock(path, id)
	}
	if err != nil {
		return nil, err
	}
	if block.minTime != blockMin || block.maxTime != blockMax {
		return nil, fmt.Errorf("cold block %d time range mismatch", id)
	}
	return block, nil
}

func writeSynced(path string, data []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func openColdBlock(path string, id uint64) (*coldBlock, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("open cold block %s: cold block too short", path)
	}
	// Cold blocks are written once, before they are mapped, and never modified.
	data, err := unix.Mmap(int(file.Fd()), 0, int(info.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("map %s: %w", path, err)
	}
	block, err := parseColdBlock(id, data, true, path)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, fmt.Errorf("open cold block %s: %w", path, err)
	}
	return block, nil
}

func parseColdBlock(id uint64, data []byte, mapped bool, path string) (block *coldBlock, err error) {
	if len(data) < len(coldMagic)+4 {
		return nil, errors.New("cold block too short")
	}
	body, checksum := data[:len(data)-4], data[len(data)-4:]
	if string(body[:8]) != string(coldMagic) {
		return nil, errors.New("cold block has invalid magic")
	}
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(checksum) {
		return nil, errors.New("cold block checksum mismatch")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			block, err = nil, fmt.Errorf("corrupt cold block: %v", recovered)
		}
	}()
	cursor := body[8:]
	count := takeUvarint(&cursor)
	names := make([]string, 0, count)
	for range count {
		size := takeUvarint(&cursor)
		// Interned, so the names never reference the mapping.
		names = append(names, labels.Name(labels.Intern(string(cursor[:size]))))
		cursor = cursor[size:]
	}
	localIDs := make(map[string]uint32, len(names))
	for index, name := range names {
		localIDs[name] = uint32(index)
	}
	tenants := map[string]*coldTenantIndex{}
	minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
	tenantCount := takeUvarint(&cursor)
	for range tenantCount {
		size := takeUvarint(&cursor)
		tenant := string(cursor[:size])
		cursor = cursor[size:]
		field := func(slot int) uint64 { return binary.LittleEndian.Uint64(cursor[slot*8:]) }
		index := &coldTenantIndex{
			minTime:       int64(field(0)),
			maxTime:       int64(field(1)),
			seriesTable:   int(field(2)),
			seriesCount:   int(field(3)),
			postingsTable: int(field(4)),
			postingsCount: int(field(5)),
		}
		cursor = cursor[48:]
		minTime = min(minTime, index.minTime)
		maxTime = max(maxTime, index.maxTime)
		tenants[tenant] = index
	}
	block = &coldBlock{id: id, minTime: minTime, maxTime: maxTime, path: path, data: data, mapped: mapped, names: names, localIDs: localIDs, tenants: tenants}
	for name, table := range tenants {
		if len(tenants) == 1 {
			block.onlyName, block.only = name, table
		}
	}
	return block, nil
}

func (b *coldBlock) close() error {
	if b.mapped {
		b.mapped = false
		return unix.Munmap(b.data)
	}
	return nil
}

// overlaps reports whether the tenant has samples in [start, end] here.
func (b *coldBlock) overlaps(tenant string, start, end int64) bool {
	index := b.tenant(tenant)
	return index != nil && index.minTime <= end && index.maxTime >= start
}

// tenant returns the tenant's table in the block, or nil. A tenant's engine has blocks of its own, so a
// string comparison finds it, where a lookup in the map is a hash and a cache miss for every block of every query.
func (b *coldBlock) tenant(tenant string) *coldTenantIndex {
	if b.only != nil {
		if b.onlyName == tenant {
			return b.only
		}
		return nil
	}
	return b.tenants[tenant]
}

func (b *coldBlock) seriesCount(tenant string) int {
	if index, ok := b.tenants[tenant]; ok {
		return index.seriesCount
	}
	return 0
}

func (b *coldBlock) seriesIn(table *coldTenantIndex, index int) coldSeries {
	at := table.seriesTable + index*8
	offset := int(binary.LittleEndian.Uint64(b.data[at:]))
	cursor := b.data[offset:]
	labelsLen := int(takeUvarint(&cursor))
	stored := cursor[:labelsLen]
	cursor = cursor[labelsLen:]
	chunksLen := int(takeUvarint(&cursor))
	return coldSeries{block: b, encoded: unsafe.String(unsafe.SliceData(stored), len(stored)), chunks: cursor[:chunksLen:chunksLen]}
}

// withLabel returns the indexes of the tenant's series with label name, from every posting list
// under it: the table is sorted by name, so they are one range of it.
func (b *coldBlock) withLabel(table *coldTenantIndex, name string) []uint32 {
	table.withLabelLock.Lock()
	defer table.withLabelLock.Unlock()
	if series, ok := table.withLabel[name]; ok {
		return series
	}
	if countersEnabled {
		coldLabelLists.Add(1)
	}
	series := b.withLabelUncached(table, name)
	if table.withLabel == nil {
		table.withLabel = map[string][]uint32{}
	}
	table.withLabel[strings.Clone(name)] = series
	return series
}

func (b *coldBlock) entry(table *coldTenantIndex, index int) (name uint32, hash uint64, offset uint32) {
	at := table.postingsTable + index*16
	return binary.LittleEndian.Uint32(b.data[at:]), binary.LittleEndian.Uint64(b.data[at+4:]), binary.LittleEndian.Uint32(b.data[at+12:])
}

func (b *coldBlock) list(table *coldTenantIndex, offset uint32, into []uint32) []uint32 {
	at := table.postingsTable + table.postingsCount*16 + int(offset)
	count := int(binary.LittleEndian.Uint32(b.data[at:]))
	for index := range count {
		into = append(into, binary.LittleEndian.Uint32(b.data[at+4+index*4:]))
	}
	return into
}

func (b *coldBlock) withLabelUncached(table *coldTenantIndex, name string) []uint32 {
	local, ok := b.localIDs[name]
	if !ok {
		return []uint32{}
	}
	low := sort.Search(table.postingsCount, func(index int) bool {
		entryName, _, _ := b.entry(table, index)
		return entryName >= local
	})
	series := []uint32{}
	for index := low; index < table.postingsCount; index++ {
		entryName, _, offset := b.entry(table, index)
		if entryName != local {
			break
		}
		series = b.list(table, offset, series)
	}
	slices.Sort(series)
	return slices.Compact(series)
}

// nameMatchesOf returns the tenant's series whose name matcher, a name matcher other than an
// equality, accepts, sorted: once per block and matcher, since the block never changes.
func (b *coldBlock) nameMatchesOf(table *coldTenantIndex, matcher *compiledMatcher) []uint32 {
	names := b.metricNamesOf(table)
	if matcher.kind == kindRegex {
		if values, ok := matcher.re.acceptedValues(); ok {
			var series []uint32
			for _, value := range values {
				index, found := sort.Find(len(names), func(i int) int { return strings.Compare(value, names[i].name) })
				if found {
					series = append(series, names[index].series...)
				}
			}
			slices.Sort(series)
			return series
		}
	}
	key := keyOf(matcher)
	table.nameMatchesLock.Lock()
	if series, ok := table.nameMatches[key]; ok {
		table.nameMatchesLock.Unlock()
		return series
	}
	table.nameMatchesLock.Unlock()
	if countersEnabled {
		coldNameMatches.Add(1)
	}
	var series []uint32
	for _, name := range names {
		if matcher.matchesValue(name.name) {
			series = append(series, name.series...)
		}
	}
	slices.Sort(series)
	table.nameMatchesLock.Lock()
	// Bounded, so arbitrary queries can't grow it.
	if table.nameMatches == nil || len(table.nameMatches) >= 256 {
		table.nameMatches = map[nameMatcherKey][]uint32{}
	}
	table.nameMatches[key] = series
	table.nameMatchesLock.Unlock()
	return series
}

// metricNamesOf returns the tenant's metric names, sorted, each with its series.
func (b *coldBlock) metricNamesOf(table *coldTenantIndex) []coldMetricName {
	table.metricNamesOnce.Do(func() {
		byName := map[string][]uint32{}
		for index := range table.seriesCount {
			series := b.seriesIn(table, index)
			name, _ := series.lookup(metricNameLabel)
			if list, ok := byName[name]; ok {
				byName[name] = append(list, uint32(index))
			} else {
				byName[strings.Clone(name)] = []uint32{uint32(index)}
			}
		}
		names := make([]coldMetricName, 0, len(byName))
		for name, series := range byName {
			names = append(names, coldMetricName{name, series})
		}
		slices.SortFunc(names, func(a, b coldMetricName) int { return strings.Compare(a.name, b.name) })
		table.metricNames = names
	})
	return table.metricNames
}

// postingRef finds the posting list of name="value": where it is and how many series it has, which needs no copy of it.
// The name never appearing here, or the value, is a list of none.
func (b *coldBlock) postingRef(table *coldTenantIndex, name, value string) (offset uint32, count int) {
	local, ok := b.localIDs[name]
	if !ok {
		return 0, 0
	}
	hash := labels.ValueHash(value)
	low := sort.Search(table.postingsCount, func(index int) bool {
		entryName, entryHash, _ := b.entry(table, index)
		return entryName > local || entryName == local && entryHash >= hash
	})
	if low == table.postingsCount {
		return 0, 0
	}
	entryName, entryHash, offset := b.entry(table, low)
	if entryName != local || entryHash != hash {
		return 0, 0
	}
	return offset, b.listLen(table, offset)
}

// listLen is how many series a posting list has.
func (b *coldBlock) listLen(table *coldTenantIndex, offset uint32) int {
	return int(binary.LittleEndian.Uint32(b.data[table.postingsTable+table.postingsCount*16+int(offset):]))
}

// posting returns the series with name="value", or none when the name never appears here, so
// nothing does.
func (b *coldBlock) posting(table *coldTenantIndex, name, value string) []uint32 {
	offset, count := b.postingRef(table, name, value)
	if count == 0 {
		return nil
	}
	return b.list(table, offset, make([]uint32, 0, count))
}

// candidates returns indexes of the tenant's series that may match matchers: the smallest posting
// list of their equality matchers or, without one, of the series with the label of a matcher that
// rejects the empty value, or every series. Callers still check the matchers.
func (b *coldBlock) candidates(tenant string, matchers []compiledMatcher) []uint32 {
	table := b.tenant(tenant)
	if table == nil {
		return nil
	}
	// The lists are measured, and only the smallest is read: the others were copied just to be left out.
	var best []uint32
	bestSize := -1
	var bestLists []uint32
	hasBest := false
	for index := range matchers {
		matcher := &matchers[index]
		var offsets []uint32
		size := 0
		switch {
		case matcher.kind == kindEqual && matcher.value != "":
			offset, count := b.postingRef(table, matcher.name, matcher.value)
			if count > 0 {
				offsets = append(offsets, offset)
			}
			size = count
		case matcher.kind == kindRegex && !matcher.re.isMatch(""):
			// A regex that accepts few values, not the empty one, takes their postings.
			values, ok := matcher.re.acceptedValues()
			if !ok {
				continue
			}
			for _, value := range values {
				if offset, count := b.postingRef(table, matcher.name, value); count > 0 {
					offsets = append(offsets, offset)
					size += count
				}
			}
		default:
			continue
		}
		if !hasBest || size < bestSize {
			hasBest, bestSize, bestLists = true, size, offsets
		}
	}
	if hasBest {
		best = make([]uint32, 0, bestSize)
		for _, offset := range bestLists {
			best = b.list(table, offset, best)
		}
		if len(bestLists) > 1 {
			slices.Sort(best)
			best = slices.Compact(best)
		}
		if len(best) == 0 {
			best = nil
		}
	}
	if !hasBest {
		// A name regex takes the series of the names it accepts, each name checked once.
		for index := range matchers {
			matcher := &matchers[index]
			if name, ok := matcher.labelName(); ok && name == metricNameLabel && matcher.kind != kindEqual {
				list := b.nameMatchesOf(table, matcher)
				if !hasBest || len(list) < len(best) {
					best, hasBest = list, true
				}
			}
		}
		for index := range matchers {
			matcher := &matchers[index]
			if name, ok := matcher.labelName(); ok && name != metricNameLabel && matcher.kind != kindEqual && !matcher.matchesValue("") {
				list := b.withLabel(table, name)
				if !hasBest || len(list) < len(best) {
					best, hasBest = list, true
				}
			}
		}
	}
	if hasBest {
		return best
	}
	all := make([]uint32, table.seriesCount)
	for index := range all {
		all[index] = uint32(index)
	}
	return all
}

// labelsHash returns the labels hash of the series at index, like the head's series hash.
func (b *coldBlock) labelsHash(table *coldTenantIndex, index int) uint64 {
	table.labelHashesOnce.Do(func() {
		hashes := make([]uint64, table.seriesCount)
		var pairs [][2]string
		for series := range hashes {
			s := b.seriesIn(table, series)
			hashes[series] = s.labelsInto(&pairs).Hash()
		}
		table.labelHashes = hashes
	})
	return table.labelHashes[index]
}

// rangeNameIDs calls visit with the block-local id of each of the series' label names.
func (s *coldSeries) rangeNameIDs(visit func(id uint64)) {
	rest := s.encoded
	for len(rest) > 0 {
		visit(takeUvarintString(&rest))
		size := takeUvarintString(&rest)
		rest = rest[size:]
	}
}

// shardHash returns the query shard hash of the series at index.
func (b *coldBlock) shardHash(table *coldTenantIndex, index int) uint64 {
	table.shardHashesOnce.Do(func() {
		if countersEnabled {
			coldShardHashings.Add(1)
		}
		hashes := make([]uint64, table.seriesCount)
		for series := range hashes {
			s := b.seriesIn(table, series)
			hashes[series] = labels.StableHash(&s)
		}
		table.shardHashes = hashes
	})
	return table.shardHashes[index]
}

// coldSeries is a series of a cold block.
type coldSeries struct {
	block   *coldBlock
	encoded string
	chunks  []byte
}

// Range calls visit for each label in order.
func (s *coldSeries) Range(visit func(name, value string)) {
	rest := s.encoded
	for len(rest) > 0 {
		name := s.block.names[takeUvarintString(&rest)]
		size := takeUvarintString(&rest)
		visit(name, rest[:size])
		rest = rest[size:]
	}
}

func (s *coldSeries) lookup(name string) (string, bool) {
	local, ok := s.block.localIDs[name]
	if !ok {
		return "", false
	}
	rest := s.encoded
	for len(rest) > 0 {
		id := uint32(takeUvarintString(&rest))
		size := takeUvarintString(&rest)
		if id == local {
			return rest[:size], true
		}
		rest = rest[size:]
	}
	return "", false
}

// Get returns the value of name, empty when absent.
func (s *coldSeries) Get(name string) string {
	value, _ := s.lookup(name)
	return value
}

// labels returns the series' labels, copied out of the block.
func (s *coldSeries) labels() labels.Labels {
	var pairs [][2]string
	return s.labelsInto(&pairs)
}

// labelsInto is labels with a buffer for the pairs that callers keep between series: growing one for each was most of
// what a lookup of cold series allocated.
func (s *coldSeries) labelsInto(pairs *[][2]string) labels.Labels {
	*pairs = (*pairs)[:0]
	rest := s.encoded
	for len(rest) > 0 {
		name := s.block.names[takeUvarintString(&rest)]
		size := takeUvarintString(&rest)
		*pairs = append(*pairs, [2]string{name, rest[:size]})
		rest = rest[size:]
	}
	return labels.FromSorted(*pairs)
}

func (s *coldSeries) chunkIter() chunkIter {
	return chunkIter{rest: s.chunks}
}

func (s *coldSeries) hasData(start, end int64) bool {
	it := s.chunkIter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if chunk.MinTime <= end && chunk.MaxTime >= start {
			return true
		}
	}
	return false
}

// coldState is the cold blocks of a store shard: series that left the emulated head, like Go's
// blocks.
type coldState struct {
	directory string
	blocks    []*coldBlock
	nextID    uint64
}

func newColdState(directory string) (*coldState, error) {
	if directory != "" {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, fmt.Errorf("create cold directory %s: %w", directory, err)
		}
	}
	return &coldState{directory: directory}, nil
}

// matching calls visit for the tenant's cold series that match matchers with data in
// [start, end], block by block.
func (c *coldState) matching(tenant string, matchers []compiledMatcher, start, end int64, visit func(*coldSeries)) {
	c.matchingIndexed(tenant, matchers, start, end, func(_ *coldTenantIndex, _ int, series *coldSeries) { visit(series) })
}

// matchingIndexed is matching, with each series' tenant table and index in its block.
func (c *coldState) matchingIndexed(tenant string, matchers []compiledMatcher, start, end int64, visit func(*coldTenantIndex, int, *coldSeries)) {
	for _, block := range c.blocks {
		if !block.overlaps(tenant, start, end) {
			continue
		}
		table := block.tenant(tenant)
		for _, index := range block.candidates(tenant, matchers) {
			series := block.seriesIn(table, int(index))
			if series.hasData(start, end) && matches(&series, matchers) {
				visit(table, int(index), &series)
			}
		}
	}
}

// discardColdBlock removes a block no query has seen.
func discardColdBlock(block *coldBlock) {
	if block.path != "" {
		if err := os.Remove(block.path); err != nil {
			fmt.Fprintf(os.Stderr, "phase=cold_block_remove_error path=%s error=%v\n", block.path, err)
		}
	}
	_ = block.close()
}

// pruneBefore removes the blocks all older than cutoff.
func (c *coldState) pruneBefore(cutoff int64) {
	kept := c.blocks[:0]
	for _, block := range c.blocks {
		if block.maxTime >= cutoff {
			kept = append(kept, block)
			continue
		}
		if block.path != "" {
			if err := os.Remove(block.path); err != nil {
				fmt.Fprintf(os.Stderr, "phase=cold_block_remove_error path=%s error=%v\n", block.path, err)
			}
		}
		// Queries hold the shard lock while they read a block, so nothing reads it now.
		_ = block.close()
	}
	clear(c.blocks[len(kept):])
	c.blocks = kept
}

func (c *coldState) close() error {
	var errs []error
	for _, block := range c.blocks {
		errs = append(errs, block.close())
	}
	return errors.Join(errs...)
}
