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
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// Series that left the emulated head live in immutable files like the Go ingester's blocks: their
// labels, chunk references and postings stay on disk, memory-mapped, so millions of series that
// only queries of older ranges read cost no heap. The chunks themselves stay in the chunk files.

var coldMagic = []byte("MIMIRCB2")

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
	// The byte offset of each series' labels hash and query shard hash.
	hashesTable int
	// Each metric name, sorted, with its series: built on the first query by a name matcher other
	// than an equality, from the series' own labels, since the postings only hold values' hashes.
	metricNamesOnce sync.Once
	metricNames     []coldMetricName
	// The series with each label queries asked for: the block never changes.
	withLabelLock sync.Mutex
	withLabel     map[string][]uint32
	// The values of each label label values lookups asked for, with their series: the block never changes.
	valueIndexesLock sync.Mutex
	valueIndexes     map[string]*coldValueIndex
	// The series of the names each name matcher other than an equality accepted.
	nameMatchesLock sync.Mutex
	nameMatches     map[nameMatcherKey][]uint32
	// The candidates of the selectors that came in again, by their key: the block never changes, and most selectors
	// have none in most blocks. In two generations: once the current one is full, it becomes the previous one, whose
	// selectors that come in again move back to the current one, so a burst of new selectors leaves those in use.
	candidatesLock     sync.RWMutex
	candidatesCache    map[uint64]cachedCandidates
	candidatesPrevious map[uint64]cachedCandidates
	// By block-local name id, where its entries are in the postings, found once: a lookup of a value then searches only
	// its name's, and the ones of every entry of a name start from them. Each is (start+1)<<32 | end, zero until found.
	nameRangesOnce sync.Once
	nameRanges     []atomic.Uint64
	// A filter of the block's metric names: a query for a name this block has no series of skips it without a search in
	// its postings, which on a block of a cold file is a cache miss per step.
	nameFilterOnce sync.Once
	nameFilter     []uint64
}

type coldMetricName struct {
	name   string
	series []uint32
}

// coldBlock is one cold block of a store shard.
type coldBlock struct {
	id               uint64
	minTime, maxTime int64
	// One past the reference of its last out-of-order chunk, or 0, which retention keeps it for.
	outOfOrderEnd uint64
	path          string
	data          []byte
	mapped        bool
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
	pairs := 0
	for index := range series {
		series[index].labels.Range(func(name, _ string) {
			pairs++
			if _, ok := local[name]; !ok {
				local[name] = uint32(len(names))
				names = append(names, name)
			}
		})
	}
	// Sized for the series' labels and chunks, which are most of the block, and its tables: growing it as they were
	// appended copied it over and over, which was a tenth of what an ingester allocated. A label is a posting, of at
	// most an entry of 16 bytes and a list's 4 and an index's 4, and a series is an offset and two hashes.
	size := len(coldMagic) + binary.MaxVarintLen64 + 24*pairs + 24*len(series)
	for _, name := range names {
		size += binary.MaxVarintLen64 + len(name)
	}
	for index := range series {
		size += len(series[index].labels) + len(series[index].chunks) + 2*binary.MaxVarintLen64 + 1
	}
	out := make([]byte, 0, size)
	out = append(out, coldMagic...)
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
		// min, max, series table, series count, postings table, postings count, hashes table.
		out = append(out, make([]byte, 56)...)
	}
	blockMin, blockMax := int64(math.MaxInt64), int64(math.MinInt64)
	type posting struct {
		name  uint32
		hash  uint64
		index uint32
	}
	var labelBuffer []byte
	postings := make([]posting, 0, pairs)
	for rangeIndex, r := range ranges {
		offsets := make([]uint64, 0, r.end-r.start)
		postings = postings[:0]
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
		out = slices.Grow(out, 8*len(offsets)+16*(r.end-r.start))
		for _, offset := range offsets {
			out = binary.LittleEndian.AppendUint64(out, offset)
		}
		// Each series' labels hash and query shard hash, which queries need of every series they look at.
		hashesTable := len(out)
		for _, frozen := range series[r.start:r.end] {
			out = binary.LittleEndian.AppendUint64(out, frozen.labels.Hash())
			out = binary.LittleEndian.AppendUint64(out, labels.StableHash(frozen.labels))
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
		// Entries of (name, value hash, list offset), then the lists of series indexes: each entry is a run of the sorted
		// postings, its list their indexes.
		var entries []int
		for at, p := range postings {
			if at == 0 || postings[at-1].name != p.name || postings[at-1].hash != p.hash {
				entries = append(entries, at)
			}
		}
		runEnd := func(entry int) int {
			if entry+1 < len(entries) {
				return entries[entry+1]
			}
			return len(postings)
		}
		postingsTable := len(out)
		out = slices.Grow(out, 16*len(entries)+4*len(entries)+4*len(postings))
		var listOffset uint32
		for entry, at := range entries {
			out = binary.LittleEndian.AppendUint32(out, postings[at].name)
			out = binary.LittleEndian.AppendUint64(out, postings[at].hash)
			out = binary.LittleEndian.AppendUint32(out, listOffset)
			listOffset += 4 + 4*uint32(runEnd(entry)-at)
		}
		for entry, at := range entries {
			out = binary.LittleEndian.AppendUint32(out, uint32(runEnd(entry)-at))
			for _, p := range postings[at:runEnd(entry)] {
				out = binary.LittleEndian.AppendUint32(out, p.index)
			}
		}
		fields := [7]uint64{uint64(minTime), uint64(maxTime), uint64(seriesTable), uint64(len(offsets)), uint64(postingsTable), uint64(len(entries)), uint64(hashesTable)}
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
		block, err = parseColdBlock(id, out, false, "", false)
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
		// Not checksummed: out was, and reading the file back would read every page of it a second time, which also
		// makes the kernel count them as the container's active working set.
		block, err = openColdBlock(path, id, false)
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

// openColdBlock maps the block at path, checking its checksum when verify says to.
func openColdBlock(path string, id uint64, verify bool) (*coldBlock, error) {
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
	block, err := parseColdBlock(id, data, true, path, verify)
	if err != nil {
		_ = unix.Munmap(data)
		return nil, fmt.Errorf("open cold block %s: %w", path, err)
	}
	return block, nil
}

func parseColdBlock(id uint64, data []byte, mapped bool, path string, verify bool) (block *coldBlock, err error) {
	if len(data) < len(coldMagic)+4 {
		return nil, errors.New("cold block too short")
	}
	body, checksum := data[:len(data)-4], data[len(data)-4:]
	if string(body[:8]) != string(coldMagic) {
		return nil, errors.New("cold block has invalid magic")
	}
	if verify && crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(checksum) {
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
			hashesTable:   int(field(6)),
		}
		cursor = cursor[56:]
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
	block.forEachOutOfOrder(func(ref uint64) { block.outOfOrderEnd = max(block.outOfOrderEnd, ref+1) })
	return block, nil
}

// forEachOutOfOrder calls visit with the reference of each out-of-order chunk of the block's series.
func (b *coldBlock) forEachOutOfOrder(visit func(ref uint64)) {
	for _, table := range b.tenants {
		for index := range table.seriesCount {
			series := b.seriesIn(table, index)
			it := series.chunkIter()
			for chunk, more := it.next(); more; chunk, more = it.next() {
				if chunk.OutOfOrder {
					visit(uint64(chunk.Ref))
				}
			}
		}
	}
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
	return coldSeries{block: b, encoded: unsafe.String(unsafe.SliceData(stored), len(stored)), chunks: cursor[:chunksLen:chunksLen], nativeHistogram: cursor[chunksLen] == 1}
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

// metricNameHash is the hash of the metric name an equality matcher selects, if there is one.
func metricNameHash(matchers []compiledMatcher) (uint64, bool) {
	for index := range matchers {
		if matcher := &matchers[index]; matcher.kind == kindEqual && matcher.name == metricNameLabel && matcher.value != "" {
			return labels.ValueHash(matcher.value), true
		}
	}
	return 0, false
}

// mayHaveName is false when no series of the tenant in this block has the metric name of the hash.
func (b *coldBlock) mayHaveName(table *coldTenantIndex, hash uint64) bool {
	table.nameFilterOnce.Do(func() { table.nameFilter = b.buildNameFilter(table) })
	bits := uint64(len(table.nameFilter)) * 64
	if bits == 0 {
		return false
	}
	// The hash is xxhash's: any of its bits are uniform, so two parts of it make the two probes.
	first, second := hash%bits, (hash>>32|hash<<32)%bits
	return table.nameFilter[first/64]&(1<<(first%64)) != 0 && table.nameFilter[second/64]&(1<<(second%64)) != 0
}

// buildNameFilter sets the bits of the hashes of the metric names in the postings: the entries of a label are adjacent
// in the table, as it is sorted by the label's local id.
func (b *coldBlock) buildNameFilter(table *coldTenantIndex) []uint64 {
	local, ok := b.localIDs[metricNameLabel]
	if !ok {
		return nil
	}
	first, _ := b.nameRange(table, local)
	var hashes []uint64
	for index := first; index < table.postingsCount; index++ {
		name, hash, _ := b.entry(table, index)
		if name != local {
			break
		}
		hashes = append(hashes, hash)
	}
	if len(hashes) == 0 {
		return nil
	}
	// Sixteen bits for a name keeps false positives at about one in a hundred.
	words := max(1, (len(hashes)*16+63)/64)
	filter := make([]uint64, words)
	bits := uint64(words) * 64
	for _, hash := range hashes {
		first, second := hash%bits, (hash>>32|hash<<32)%bits
		filter[first/64] |= 1 << (first % 64)
		filter[second/64] |= 1 << (second % 64)
	}
	return filter
}

// nameRange returns where the entries of the name of block-local id local are in the table's postings, [start, end).
func (b *coldBlock) nameRange(table *coldTenantIndex, local uint32) (start, end int) {
	table.nameRangesOnce.Do(func() { table.nameRanges = make([]atomic.Uint64, len(b.names)) })
	if int(local) >= len(table.nameRanges) {
		return 0, 0
	}
	if packed := table.nameRanges[local].Load(); packed != 0 {
		return int(packed>>32) - 1, int(uint32(packed))
	}
	start = sort.Search(table.postingsCount, func(index int) bool {
		name, _, _ := b.entry(table, index)
		return name >= local
	})
	end = start + sort.Search(table.postingsCount-start, func(index int) bool {
		name, _, _ := b.entry(table, start+index)
		return name > local
	})
	table.nameRanges[local].Store(uint64(start+1)<<32 | uint64(end))
	return start, end
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
	low, _ := b.nameRange(table, local)
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
	start, end := b.nameRange(table, local)
	low := start + sort.Search(end-start, func(index int) bool {
		_, entryHash, _ := b.entry(table, start+index)
		return entryHash >= hash
	})
	if low == end {
		return 0, 0
	}
	_, entryHash, offset := b.entry(table, low)
	if entryHash != hash {
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
// maxCachedCandidates is how many candidates a selector's list can have to be kept, and maxCachedSelectorsPerBlock how many
// selectors each generation of a block's kept lists has.
const (
	maxCachedCandidates        = 64
	maxCachedSelectorsPerBlock = 128
)

// candidatesCached is candidates, kept by the selector's key for the next time it comes in.
// cachedCandidates is a selector's candidates in a tenant's table of a block, with the selector's matchers, which tell it
// apart from another of the same id.
type cachedCandidates struct {
	parts []selectorPart
	list  []uint32
}

// candidatesCached is candidates, kept by the selector's id for the next time: selector is the matchers the id is of.
func (b *coldBlock) candidatesCached(tenant string, id uint64, selector, matchers []compiledMatcher) []uint32 {
	table := b.tenant(tenant)
	if table == nil {
		return nil
	}
	list, hit := b.candidatesCachedIn(table, tenant, &coldSelector{id: id, matchers: selector}, matchers)
	if hit {
		countColdCandidates(table, 1, 0)
	} else {
		countColdCandidates(table, 0, 1)
	}
	return list
}

// coldSelector is a query's selector as the blocks' candidates caches keep it. The query makes its parts once, and the
// entries it adds to the blocks it reads share them: telling the selector apart from others of its id compared every
// block's entry with the selector, and an entry with the parts of one found the same needs no comparison.
type coldSelector struct {
	id       uint64
	matchers []compiledMatcher
	parts    []selectorPart
	verified *selectorPart
}

// same reports whether parts are the selector's.
func (s *coldSelector) same(parts []selectorPart) bool {
	if len(parts) > 0 && unsafe.SliceData(parts) == s.verified {
		return true
	}
	if !sameSelector(parts, s.matchers) {
		return false
	}
	if len(parts) > 0 {
		s.verified = unsafe.SliceData(parts)
	}
	return true
}

// keptParts returns the parts the selector's entries keep, made once.
func (s *coldSelector) keptParts() []selectorPart {
	if s.parts == nil {
		s.parts = selectorParts(s.matchers)
		if len(s.parts) > 0 {
			s.verified = unsafe.SliceData(s.parts)
		}
	}
	return s.parts
}

// candidatesCachedIn is candidatesCached of the tenant's table of the block, and whether they were kept.
func (b *coldBlock) candidatesCachedIn(table *coldTenantIndex, tenant string, selector *coldSelector, matchers []compiledMatcher) ([]uint32, bool) {
	table.candidatesLock.RLock()
	cached, ok := table.candidatesCache[selector.id]
	previous := false
	if !ok {
		cached, ok = table.candidatesPrevious[selector.id]
		previous = ok
	}
	table.candidatesLock.RUnlock()
	if ok && selector.same(cached.parts) {
		if previous {
			table.candidatesLock.Lock()
			table.keepCandidates(selector.id, cached)
			table.candidatesLock.Unlock()
		}
		return cached.list, true
	}
	list := b.candidates(tenant, matchers)
	if !ok && len(list) <= maxCachedCandidates {
		table.candidatesLock.Lock()
		table.keepCandidates(selector.id, cachedCandidates{parts: selector.keptParts(), list: list})
		table.candidatesLock.Unlock()
	}
	return list, false
}

// keepCandidates keeps a selector's candidates in the current generation, with the table's candidates lock held.
func (table *coldTenantIndex) keepCandidates(id uint64, cached cachedCandidates) {
	if table.candidatesCache == nil || len(table.candidatesCache) >= maxCachedSelectorsPerBlock {
		// Not sized for a generation: most blocks keep a few selectors, which a small map finds in one probe.
		table.candidatesPrevious, table.candidatesCache = table.candidatesCache, map[uint64]cachedCandidates{}
	}
	table.candidatesCache[id] = cached
}

// coldCandidatesStripes is how many sets of counters the lookups of the cold blocks' kept candidates add to, picked by
// what counts them: one set all the cores add to bounces between them. Lookups add once for all the blocks they read, as
// an atomic add for each block was a tenth of a lookup of one series.
const coldCandidatesStripes = 64

var coldCandidatesCounts [coldCandidatesStripes]struct {
	hits, misses atomic.Int64
	_            [48]byte
}

func countColdCandidates[T any](by *T, hits, misses int64) {
	counts := &coldCandidatesCounts[(uintptr(unsafe.Pointer(by))>>6)%coldCandidatesStripes]
	if hits > 0 {
		counts.hits.Add(hits)
	}
	if misses > 0 {
		counts.misses.Add(misses)
	}
}

// ColdCandidatesCacheCounts returns how many lookups of the cold blocks' kept candidates of a selector found them, and
// how many didn't, since the process started.
func ColdCandidatesCacheCounts() (hits, misses int64) {
	for stripe := range coldCandidatesCounts {
		hits += coldCandidatesCounts[stripe].hits.Load()
		misses += coldCandidatesCounts[stripe].misses.Load()
	}
	return hits, misses
}

// coldValueIndex is a label's distinct values in a tenant's table of a block, each with the indexes of its series, or
// nothing when the label has too many values to look each up.
type coldValueIndex struct {
	values   []string
	series   [][]uint32
	tooMany  bool
	building sync.Once
}

// maxColdValueIndexes is how many labels' values a tenant's table of a block keeps: at this many they are all dropped.
const maxColdValueIndexes = 16

// valueIndex returns the label's values in the tenant's table with their series, built on the first lookup from every
// series' labels, or nil when the label has too many values.
func (b *coldBlock) valueIndex(table *coldTenantIndex, name string) *coldValueIndex {
	table.valueIndexesLock.Lock()
	index, ok := table.valueIndexes[name]
	if !ok {
		if table.valueIndexes == nil || len(table.valueIndexes) >= maxColdValueIndexes {
			table.valueIndexes = map[string]*coldValueIndex{}
		}
		index = &coldValueIndex{}
		table.valueIndexes[name] = index
	}
	table.valueIndexesLock.Unlock()
	index.building.Do(func() {
		positions := map[string]int{}
		for at := range table.seriesCount {
			series := b.seriesIn(table, at)
			value, ok := series.lookup(name)
			if !ok {
				continue
			}
			position, seen := positions[value]
			if !seen {
				if len(index.values) >= maxValuesByPostings {
					// Field by field: the index holds the Once that is running this.
					index.values, index.series, index.tooMany = nil, nil, true
					return
				}
				// Copied out of the block, which a merge may unmap while the index is still referenced.
				value = strings.Clone(value)
				position = len(index.values)
				positions[value] = position
				index.values = append(index.values, value)
				index.series = append(index.series, nil)
			}
			index.series[position] = append(index.series[position], uint32(at))
		}
	})
	if index.tooMany {
		return nil
	}
	return index
}

func (b *coldBlock) candidates(tenant string, matchers []compiledMatcher) []uint32 {
	list, _ := b.candidatesSatisfying(tenant, matchers)
	return list
}

// candidatesSatisfying is candidates, with the index of a matcher every candidate is known to satisfy, or -1: a name
// regex's candidates are the series of the names it accepts, which a caller needn't check it against again.
func (b *coldBlock) candidatesSatisfying(tenant string, matchers []compiledMatcher) ([]uint32, int) {
	satisfied := -1
	table := b.tenant(tenant)
	if table == nil {
		return nil, satisfied
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
					best, hasBest, satisfied = list, true, index
				}
			}
		}
		for index := range matchers {
			matcher := &matchers[index]
			if name, ok := matcher.labelName(); ok && name != metricNameLabel && matcher.kind != kindEqual && !matcher.matchesValue("") {
				list := b.withLabel(table, name)
				if !hasBest || len(list) < len(best) {
					best, hasBest, satisfied = list, true, -1
				}
			}
		}
	}
	if hasBest {
		return best, satisfied
	}
	all := make([]uint32, table.seriesCount)
	for index := range all {
		all[index] = uint32(index)
	}
	return all, -1
}

// labelsHash returns the labels hash of the series at index, like the head's series hash.
func (b *coldBlock) labelsHash(table *coldTenantIndex, index int) uint64 {
	return binary.LittleEndian.Uint64(b.data[table.hashesTable+index*16:])
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
	return binary.LittleEndian.Uint64(b.data[table.hashesTable+index*16+8:])
}

// coldSeries is a series of a cold block.
type coldSeries struct {
	block   *coldBlock
	encoded string
	chunks  []byte
	// Whether the series' last samples were native histograms.
	nativeHistogram bool
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
	// How many times blocks changed, which a select's index of the shards holding a name is as of.
	changes atomic.Uint64
	// The blocks' earliest and latest sample times as of their last change, when spanned: most queries are of the last
	// minutes, which no block has, and finding that out block by block read every block's memory for each query.
	spanned          bool
	minTime, maxTime int64
	nextID           uint64
	// What removes the files of the blocks merges and retention drop, when the last head snapshot may still
	// reference them; nil removes them right away.
	remove func(path string)
	// The blocks merges and retention dropped, which are closed once no read of a snapshot of the blocks can still
	// read them. Writers never change a slice of blocks they published, so a snapshot's stays as it was.
	retired []*coldBlock
	// Read held by the reads of a snapshot, which release the shard's lock before reading the blocks: closing the
	// retired blocks only tries it, so it waits for no read and no read waits for it.
	readers sync.RWMutex
}

// changed records that the blocks changed, with their span.
func (c *coldState) changed() {
	c.spanned, c.minTime, c.maxTime = true, math.MaxInt64, math.MinInt64
	for _, block := range c.blocks {
		c.minTime, c.maxTime = min(c.minTime, block.minTime), max(c.maxTime, block.maxTime)
	}
	c.changes.Add(1)
}

// overlaps reports whether some block may have samples in [start, end].
func (c *coldState) overlaps(start, end int64) bool {
	return len(c.blocks) > 0 && (!c.spanned || c.minTime <= end && c.maxTime >= start)
}

// snapshot returns the blocks as they are, for a read that releases the shard's lock before it reads them, until it
// calls endSnapshot. Callers hold the shard's lock, so what the blocks are is as of the shard's series they read.
func (c *coldState) snapshot() coldView {
	c.readers.RLock()
	return c.blocks
}

// endSnapshot ends a read of a snapshot.
func (c *coldState) endSnapshot() {
	c.readers.RUnlock()
}

// retire closes the block once no read of a snapshot can still read it. Callers hold the shard's lock.
func (c *coldState) retire(block *coldBlock) {
	c.retired = append(c.retired, block)
	c.closeRetired()
}

// closeRetired closes the retired blocks if no read of a snapshot is in progress, or leaves them for a later call.
// Callers hold the shard's lock.
func (c *coldState) closeRetired() {
	if len(c.retired) == 0 || !c.readers.TryLock() {
		return
	}
	defer c.readers.Unlock()
	for _, block := range c.retired {
		_ = block.close()
	}
	clear(c.retired)
	c.retired = c.retired[:0]
}

// removeFile removes a dropped block's file, or hands it to remove.
func (c *coldState) removeFile(path string) {
	if c.remove != nil {
		c.remove(path)
		return
	}
	if err := os.Remove(path); err != nil {
		fmt.Fprintf(os.Stderr, "phase=cold_block_remove_error path=%s error=%v\n", path, err)
	}
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
	coldView(c.blocks).matching(tenant, matchers, start, end, visit)
}

// coldView is a shard's cold blocks as of a moment, which a read of a snapshot reads without the shard's lock.
type coldView []*coldBlock

// matching is coldState.matching over the blocks.
func (blocks coldView) matching(tenant string, matchers []compiledMatcher, start, end int64, visit func(*coldSeries)) {
	blocks.matchingIndexed(tenant, matchers, start, end, func(_ *coldTenantIndex, _ int, series *coldSeries) { visit(series) })
}

// matchingIndexed is matching, with each series' tenant table and index in its block.
func (blocks coldView) matchingIndexed(tenant string, matchers []compiledMatcher, start, end int64, visit func(*coldTenantIndex, int, *coldSeries)) {
	nameHash, byName := metricNameHash(matchers)
	// One for every series, as visit gets its address and so it lives on the heap: made at the first candidate, as most
	// calls skip every block.
	var series *coldSeries
	for _, block := range blocks {
		if !block.overlaps(tenant, start, end) {
			continue
		}
		table := block.tenant(tenant)
		if byName && !block.mayHaveName(table, nameHash) {
			continue
		}
		for _, index := range block.candidates(tenant, matchers) {
			if series == nil {
				series = new(coldSeries)
			}
			*series = block.seriesIn(table, int(index))
			if series.hasData(start, end) && matches(series, matchers) {
				visit(table, int(index), series)
			}
		}
	}
}

var (
	// A block with fewer series than this is small, and the blocks that head compactions write every minute are: a query
	// visits every block of every shard, so hundreds of them cost more than the series in them do.
	coldSmallSeries = 2048
	// How many adjacent small blocks are merged into one.
	coldMergeFanout = 8
)

// seriesTotal is how many series the block has, of all its tenants.
func (b *coldBlock) seriesTotal() int {
	total := 0
	for _, table := range b.tenants {
		total += table.seriesCount
	}
	return total
}

// mergeSmallBlocks merges runs of adjacent small blocks into one block each, with the shard locked: as no query reads
// a block meanwhile, the blocks that were merged are removed as soon as the merged one is written. A series in more
// than one block gets the chunks of all of them. A failure leaves the blocks as they were.
func (c *coldState) mergeSmallBlocks() {
	for {
		start, run := -1, 0
		for index, block := range c.blocks {
			if block.seriesTotal() >= coldSmallSeries {
				run = 0
				continue
			}
			if run == 0 {
				start = index
			}
			run++
			if run == coldMergeFanout {
				break
			}
		}
		if run < coldMergeFanout {
			return
		}
		if err := c.merge(start, start+coldMergeFanout); err != nil {
			fmt.Fprintf(os.Stderr, "phase=cold_block_merge_error error=%v\n", err)
			return
		}
	}
}

// merge replaces the blocks from to before with one that has their series.
func (c *coldState) merge(from, to int) error {
	id := c.nextID
	block, err := buildMergedBlock(c.directory, id, c.blocks[from:to])
	if err != nil {
		return err
	}
	c.nextID++
	c.replace(from, to, block)
	return nil
}

// buildMergedBlock writes the block with the series of blocks, which only reads them: a series in more than one gets
// the chunks of all of them, the later blocks' after. The block is nil without any series.
func buildMergedBlock(directory string, id uint64, blocks []*coldBlock) (*coldBlock, error) {
	type key struct {
		tenant string
		labels labels.Labels
	}
	var frozen []frozenSeries
	positions := map[key]int{}
	var pairs [][2]string
	for _, block := range blocks {
		for tenant, table := range block.tenants {
			for index := range table.seriesCount {
				series := block.seriesIn(table, index)
				stored := series.labelsInto(&pairs)
				chunks := chunkList(slices.Clone(series.chunks))
				at, seen := positions[key{tenant, stored}]
				if !seen {
					positions[key{tenant, stored}] = len(frozen)
					frozen = append(frozen, frozenSeries{tenant: tenant, labels: stored, chunks: chunks, nativeHistogram: series.nativeHistogram})
					continue
				}
				// The same series left the head more than once: the chunks of the later block come after.
				known := map[uint64]struct{}{}
				metas := frozen[at].chunks.toSlice()
				for _, meta := range metas {
					known[uint64(meta.Ref)] = struct{}{}
				}
				for _, meta := range chunks.toSlice() {
					if _, ok := known[uint64(meta.Ref)]; !ok {
						metas = append(metas, meta)
					}
				}
				frozen[at].chunks = chunkListFromMetas(metas)
				frozen[at].nativeHistogram = series.nativeHistogram
			}
		}
	}
	return buildColdBlock(directory, id, frozen)
}

// replace puts block, or nothing when it is nil, in the place of the blocks from to before, which it removes.
func (c *coldState) replace(from, to int, block *coldBlock) {
	removed := c.blocks[from:to]
	// A new slice: reads of a snapshot keep reading the one they have.
	blocks := make([]*coldBlock, 0, len(c.blocks)-len(removed)+1)
	blocks = append(blocks, c.blocks[:from]...)
	if block != nil {
		blocks = append(blocks, block)
	}
	c.blocks = append(blocks, c.blocks[to:]...)
	c.changed()
	layoutChanges.Add(1)
	for _, old := range removed {
		if old.path != "" {
			c.removeFile(old.path)
		}
		c.retire(old)
	}
}

// A query visits every cold block of every shard of the tenant, so a shard's blocks are merged down to
// coldTargetBlocks, the adjacent ones of the smallest total size first, up to coldMaxMergeBytes a merge, which bounds
// what a merge rewrites.
const (
	coldTargetBlocks  = 4
	coldMaxMergeBytes = 512 << 20
)

// pickMerge returns the blocks from to before to merge next, if the shard has more than its target.
func (c *coldState) pickMerge() (from, to int, ok bool) {
	excess := len(c.blocks) - coldTargetBlocks
	if excess <= 0 {
		return 0, 0, false
	}
	for width := min(excess+1, coldMergeFanout); width >= 2; width-- {
		best, bestSize := -1, 0
		for start := 0; start+width <= len(c.blocks); start++ {
			size := 0
			for _, block := range c.blocks[start : start+width] {
				size += len(block.data)
			}
			if size <= coldMaxMergeBytes && (best < 0 || size < bestSize) {
				best, bestSize = start, size
			}
		}
		if best >= 0 {
			return best, best + width, true
		}
	}
	return 0, 0, false
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
	c.prune(&retained{cutoff: cutoff}, 0)
}

// prune removes the blocks of store shard shard with nothing the retention keeps.
func (c *coldState) prune(r *retained, shard int) {
	var kept, removed []*coldBlock
	for _, block := range c.blocks {
		if block.maxTime >= r.cutoff || r.outOfOrderFrom != nil && block.outOfOrderEnd > r.outOfOrderFrom[shard] {
			kept = append(kept, block)
		} else {
			removed = append(removed, block)
		}
	}
	if len(removed) == 0 {
		return
	}
	// A new slice: reads of a snapshot keep reading the one they have.
	c.blocks = kept
	c.changed()
	layoutChanges.Add(1)
	for _, block := range removed {
		if block.path != "" {
			c.removeFile(block.path)
		}
		c.retire(block)
	}
}

func (c *coldState) close() error {
	// No read is left: the engine is closed.
	c.readers.Lock()
	defer c.readers.Unlock()
	var errs []error
	for _, block := range c.blocks {
		errs = append(errs, block.close())
	}
	for _, block := range c.retired {
		errs = append(errs, block.close())
	}
	c.retired = nil
	return errors.Join(errs...)
}
