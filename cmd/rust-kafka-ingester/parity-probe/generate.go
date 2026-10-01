// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type generationConfig struct {
	seed         uint64
	names        int
	maxSeries    int
	maxSelectors int
}

// What selectors are generated from: the tenant's metric names with their series, and for some
// of them their labels' values.
type tenantData struct {
	// Metric names with their series in the reference's head.
	nameSeries map[string]uint64
	// For each picked name, its label names (without __name__) and their values.
	labelValues map[string]map[string][]string
	// Values of labels, across names, with their series: candidates for nameless selectors.
	rareValues map[string]map[string]uint64
}

// A label name absent from every series, for matchers on missing labels.
const missingLabel = "parity_probe_missing"

type matcher struct {
	name, op, value string
}

func (m matcher) String() string {
	return m.name + m.op + strconv.Quote(m.value)
}

func selector(matchers ...matcher) string {
	parts := make([]string, len(matchers))
	for i, m := range matchers {
		parts[i] = m.String()
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// pickNames returns up to n names with between 1 and maxSeries series, deterministically for a seed.
func pickNames(nameSeries map[string]uint64, n, maxSeries int, random *rand.Rand) []string {
	var candidates []string
	for name, series := range nameSeries {
		if series > 0 && series <= uint64(maxSeries) {
			candidates = append(candidates, name)
		}
	}
	sort.Strings(candidates)
	random.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	return candidates[:min(n, len(candidates))]
}

// familyPrefix is the start of a name up to its first underscore, when the names sharing it hold
// few enough series to select at once.
func familyPrefix(name string, nameSeries map[string]uint64, maxSeries int) (string, bool) {
	index := strings.Index(name, "_")
	if index <= 0 {
		return "", false
	}
	prefix := name[:index+1]
	var total uint64
	for other, series := range nameSeries {
		if strings.HasPrefix(other, prefix) {
			total += series
		}
	}
	return prefix, total <= uint64(3*maxSeries)
}

var plainValue = regexp.MustCompile(`^[A-Za-z0-9_:.\-]*$`)

// generate builds selectors from the tenant's data: equality, alternations and prefixes of real
// names, label matchers of every kind on real values and on missing labels, case-insensitive and
// special-character values, nameless selectors on rare values, and combinations.
func generate(data tenantData, picked []string, cfg generationConfig, random *rand.Rand) []string {
	var out []string
	add := func(matchers ...matcher) { out = append(out, selector(matchers...)) }
	for index, name := range picked {
		nameEq := matcher{"__name__", "=", name}
		add(nameEq)
		peers := []string{regexp.QuoteMeta(name)}
		for _, other := range picked {
			if other != name && len(peers) < 3 {
				peers = append(peers, regexp.QuoteMeta(other))
			}
		}
		add(matcher{"__name__", "=~", strings.Join(peers, "|")})
		if prefix, ok := familyPrefix(name, data.nameSeries, cfg.maxSeries); ok {
			add(matcher{"__name__", "=~", regexp.QuoteMeta(prefix) + ".*"})
		}
		add(nameEq, matcher{missingLabel, "=", ""})
		add(nameEq, matcher{missingLabel, "!=", ""})
		labelNames := sortedKeys(data.labelValues[name])
		for _, label := range labelNames {
			values := data.labelValues[name][label]
			if len(values) == 0 {
				continue
			}
			pick := random.IntN(len(values))
			first, second := values[pick], values[(pick+1)%len(values)]
			alternation := regexp.QuoteMeta(first) + "|" + regexp.QuoteMeta(second)
			add(nameEq, matcher{label, "=", first})
			add(nameEq, matcher{label, "!=", first})
			add(nameEq, matcher{label, "=~", alternation})
			add(nameEq, matcher{label, "!~", alternation})
			add(nameEq, matcher{label, "=", ""})
			add(nameEq, matcher{label, "!=", ""})
			add(nameEq, matcher{label, "=~", ".+"})
			add(nameEq, matcher{label, "=~", ".*"})
			if runes := []rune(first); len(runes) >= 2 {
				half := len(runes) / 2
				add(nameEq, matcher{label, "=~", regexp.QuoteMeta(string(runes[:half])) + ".*"})
				add(nameEq, matcher{label, "=~", ".*" + regexp.QuoteMeta(string(runes[half:]))})
				// Escapes are punctuation, which upper-casing leaves alone.
				add(nameEq, matcher{label, "=~", "(?i)" + strings.ToUpper(regexp.QuoteMeta(first))})
			}
			for _, value := range values {
				if !plainValue.MatchString(value) {
					add(nameEq, matcher{label, "=", value})
					add(nameEq, matcher{label, "=~", regexp.QuoteMeta(value)})
					break
				}
			}
		}
		// Two and three matchers at once, on a name alternation too.
		if len(labelNames) >= 2 {
			a, b := labelNames[0], labelNames[1]
			va := data.labelValues[name][a]
			if len(va) > 0 {
				add(nameEq, matcher{a, "=~", regexp.QuoteMeta(va[0]) + "|nonexistent"}, matcher{b, "!=", ""})
				add(matcher{"__name__", "=~", strings.Join(peers, "|")}, matcher{a, "!~", "nonexistent"}, matcher{b, "=~", ".+"})
			}
		}
		if len(out) >= cfg.maxSelectors && index > 0 {
			break
		}
	}
	// Nameless selectors, on values few series have.
	for _, label := range sortedKeys(data.rareValues) {
		var values []string
		for value, series := range data.rareValues[label] {
			if series > 0 && series <= uint64(cfg.maxSeries) {
				values = append(values, value)
			}
		}
		sort.Strings(values)
		if len(values) == 0 {
			continue
		}
		pick := random.IntN(len(values))
		first, second := values[pick], values[(pick+1)%len(values)]
		add(matcher{label, "=", first})
		add(matcher{label, "=~", regexp.QuoteMeta(first) + "|" + regexp.QuoteMeta(second)})
	}
	slices.Sort(out)
	out = slices.Compact(out)
	random.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out[:min(cfg.maxSelectors, len(out))]
}

// withShard adds a query shard matcher to a selector.
func withShard(selector, shard string) string {
	return strings.TrimSuffix(selector, "}") + `, __query_shard__="` + shard + `"}`
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
