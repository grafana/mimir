// SPDX-License-Identifier: AGPL-3.0-only

// Package trackers holds active series custom trackers and cost attribution trackers.
package trackers

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

type MatchOp int

const (
	MatchEqual MatchOp = iota
	MatchNotEqual
	MatchRegex
	MatchNotRegex
)

type LabelMatcher struct {
	Name  string
	Op    MatchOp
	Value string
	regex *regexp.Regexp
}

func (m *LabelMatcher) Matches(value string) bool {
	switch m.Op {
	case MatchEqual:
		return value == m.Value
	case MatchNotEqual:
		return value != m.Value
	case MatchRegex:
		return m.regex.MatchString(value)
	default:
		return !m.regex.MatchString(value)
	}
}

// ParseMatchers parses an Alertmanager-style matcher list such as `{a="b", c=~"d|e"}`, the syntax
// of Mimir's custom tracker definitions. Braces are optional and values may be unquoted.
func ParseMatchers(input string) ([]LabelMatcher, error) {
	text := strings.TrimSpace(input)
	if inner, ok := strings.CutPrefix(text, "{"); ok {
		inner, ok = strings.CutSuffix(inner, "}")
		if !ok {
			return nil, fmt.Errorf("unbalanced braces in %q", input)
		}
		text = inner
	}
	runes := []rune(text)
	i := 0
	peek := func() (rune, bool) {
		if i < len(runes) {
			return runes[i], true
		}
		return 0, false
	}
	skip := func(f func(rune) bool) {
		for c, ok := peek(); ok && f(c); c, ok = peek() {
			i++
		}
	}
	var matchers []LabelMatcher
	for {
		skip(func(c rune) bool { return unicode.IsSpace(c) || c == ',' })
		if _, ok := peek(); !ok {
			break
		}
		start := i
		skip(func(c rune) bool {
			return unicode.IsLetter(c) || unicode.IsDigit(c) || c == '_' || c == '.' || c == ':'
		})
		name := string(runes[start:i])
		if name == "" {
			return nil, fmt.Errorf("expected a label name in %q", input)
		}
		skip(unicode.IsSpace)
		opStart := i
		skip(func(c rune) bool { return c == '=' || c == '!' || c == '~' })
		op := string(runes[opStart:i])
		skip(unicode.IsSpace)
		var value strings.Builder
		if c, ok := peek(); ok && c == '"' {
			i++
			for {
				c, ok := peek()
				if !ok {
					return nil, fmt.Errorf("unterminated quoted value in %q", input)
				}
				i++
				if c == '"' {
					break
				}
				if c == '\\' {
					e, ok := peek()
					if !ok {
						return nil, fmt.Errorf("unterminated escape in %q", input)
					}
					i++
					switch e {
					case 'n':
						value.WriteRune('\n')
					case 't':
						value.WriteRune('\t')
					default:
						value.WriteRune(e)
					}
					continue
				}
				value.WriteRune(c)
			}
		} else {
			valueStart := i
			skip(func(c rune) bool { return c != ',' })
			value.WriteString(strings.TrimRightFunc(string(runes[valueStart:i]), unicode.IsSpace))
		}
		matcher := LabelMatcher{Name: name, Value: value.String()}
		switch op {
		case "=":
			matcher.Op = MatchEqual
		case "!=":
			matcher.Op = MatchNotEqual
		case "=~", "!~":
			matcher.Op = MatchRegex
			if op == "!~" {
				matcher.Op = MatchNotRegex
			}
			regex, err := regexp.Compile("^(?:" + matcher.Value + ")$")
			if err != nil {
				return nil, err
			}
			matcher.regex = regex
		default:
			return nil, fmt.Errorf("unknown match operator %q in %q", op, input)
		}
		matchers = append(matchers, matcher)
	}
	if len(matchers) == 0 {
		return nil, fmt.Errorf("no matchers in %q", input)
	}
	return matchers, nil
}

type valueKey struct{ name, value string }

// CustomTrackers are sorted by name. A series matches a tracker when it matches all its matchers.
type CustomTrackers struct {
	names    []string
	sources  []string
	matchers [][]LabelMatcher
	// Trackers that need a given label value, so a series only evaluates trackers that can match.
	byValue   map[valueKey][]uint16
	unindexed []uint16
}

// Equal compares like Go's matcher config comparison: the same trackers, whatever else the
// overrides change.
func (t *CustomTrackers) Equal(other *CustomTrackers) bool {
	if t == nil || other == nil {
		return t.Len() == other.Len()
	}
	return slices.Equal(t.names, other.names) && slices.Equal(t.sources, other.sources)
}

// NewCustomTrackers builds trackers from name to matcher source.
func NewCustomTrackers(sources map[string]string) (*CustomTrackers, error) {
	if len(sources) > 0xffff {
		return nil, fmt.Errorf("too many custom trackers")
	}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	trackers := &CustomTrackers{byValue: map[valueKey][]uint16{}}
	for i, name := range names {
		source := sources[name]
		matchers, err := ParseMatchers(source)
		if err != nil {
			return nil, fmt.Errorf("can't build active series matcher %s: %w", name, err)
		}
		index := uint16(i)
		if label, values, ok := indexValues(matchers); ok {
			for _, value := range values {
				key := valueKey{label, value}
				trackers.byValue[key] = append(trackers.byValue[key], index)
			}
		} else {
			trackers.unindexed = append(trackers.unindexed, index)
		}
		trackers.names = append(trackers.names, name)
		trackers.sources = append(trackers.sources, source)
		trackers.matchers = append(trackers.matchers, matchers)
	}
	return trackers, nil
}

// ParseFlag parses `<name>:<matcher>[;<name>:<matcher>]*`.
func ParseFlag(flag string) ([][2]string, error) {
	if strings.TrimSpace(flag) == "" {
		return nil, nil
	}
	var pairs [][2]string
	for _, pair := range strings.Split(flag, ";") {
		name, matcher, ok := strings.Cut(pair, ":")
		if !ok {
			return nil, fmt.Errorf("value should be <name>:<matcher>, but colon was not found in %q", pair)
		}
		name, matcher = strings.TrimSpace(name), strings.TrimSpace(matcher)
		if name == "" || matcher == "" {
			return nil, fmt.Errorf("one side of %q is empty", pair)
		}
		pairs = append(pairs, [2]string{name, matcher})
	}
	return pairs, nil
}

// CustomTrackersFromValue reads a YAML or JSON map of tracker name to matcher.
func CustomTrackersFromValue(value any) (*CustomTrackers, error) {
	switch value := value.(type) {
	case nil:
		return NewCustomTrackers(nil)
	case map[string]any:
		sources := make(map[string]string, len(value))
		for name, matcher := range value {
			source, ok := matcher.(string)
			if !ok {
				return nil, fmt.Errorf("matcher for %s must be a string", name)
			}
			sources[name] = source
		}
		return NewCustomTrackers(sources)
	default:
		return nil, fmt.Errorf("custom trackers must be a map, got %v", value)
	}
}

// Merged returns t with other's trackers added, replacing any with the same name.
func (t *CustomTrackers) Merged(other *CustomTrackers) *CustomTrackers {
	sources := map[string]string{}
	for i, name := range t.Names() {
		sources[name] = t.sources[i]
	}
	for i, name := range other.Names() {
		sources[name] = other.sources[i]
	}
	merged, err := NewCustomTrackers(sources)
	if err != nil {
		panic(fmt.Sprintf("merging valid trackers: %v", err))
	}
	return merged
}

func (t *CustomTrackers) IsEmpty() bool { return t.Len() == 0 }

func (t *CustomTrackers) Len() int {
	if t == nil {
		return 0
	}
	return len(t.names)
}

func (t *CustomTrackers) Names() []string {
	if t == nil {
		return nil
	}
	return t.names
}

// Sources returns each tracker's matcher source, in the order of Names.
func (t *CustomTrackers) Sources() []string {
	if t == nil {
		return nil
	}
	return t.sources
}

// Matching returns the indices of the trackers labels matches, in ascending order.
func (t *CustomTrackers) Matching(ls labels.LabelSet) []uint16 {
	if t.IsEmpty() {
		return nil
	}
	candidates := append([]uint16(nil), t.unindexed...)
	if len(t.byValue) > 0 {
		ls.Range(func(name, value string) {
			// The lookup key doesn't escape, so it doesn't allocate.
			if indices, ok := t.byValue[valueKey{name, value}]; ok {
				candidates = append(candidates, indices...)
			}
		})
	}
	slices.Sort(candidates)
	candidates = slices.Compact(candidates)
	matched := candidates[:0]
	for _, index := range candidates {
		all := true
		for i := range t.matchers[index] {
			matcher := &t.matchers[index][i]
			if !matcher.Matches(ls.Get(matcher.Name)) {
				all = false
				break
			}
		}
		if all {
			matched = append(matched, index)
		}
	}
	return matched
}

// indexValues returns the label values a series must have for the tracker to match, when one
// matcher pins them: an equality, or a regex that is a plain alternation of literals like `a|b|c`.
func indexValues(matchers []LabelMatcher) (string, []string, bool) {
	for _, matcher := range matchers {
		if matcher.Op == MatchEqual && matcher.Value != "" {
			return matcher.Name, []string{matcher.Value}, true
		}
	}
	for _, matcher := range matchers {
		if matcher.Op != MatchRegex {
			continue
		}
		inner := matcher.Value
		literal := inner != ""
		for _, c := range inner {
			if !(c < 0x80 && (unicode.IsLetter(c) || unicode.IsDigit(c)) || c == '_' || c == ':' || c == '|' || c == '-') {
				literal = false
				break
			}
		}
		values := strings.Split(inner, "|")
		if literal && !slices.Contains(values, "") {
			return matcher.Name, values, true
		}
	}
	return "", nil, false
}

type AttributionLabel struct {
	Input  string
	Output string
}

type CostAttributionTracker struct {
	Name   string
	Labels []AttributionLabel
	// Internal trackers are exposed with the ingester's own metrics, others on the cost
	// attribution registry path.
	Internal bool
}

type CostAttributionTrackers struct {
	Trackers []CostAttributionTracker
}

// CostAttributionTrackersFromValue reads a YAML or JSON map of tracker name to its labels.
func CostAttributionTrackersFromValue(value any) (CostAttributionTrackers, error) {
	var trackers CostAttributionTrackers
	switch value := value.(type) {
	case nil:
		return trackers, nil
	case map[string]any:
		for name, config := range value {
			config, _ := config.(map[string]any)
			rawLabels, ok := config["labels"].([]any)
			if !ok {
				return trackers, fmt.Errorf("tracker %s has no labels", name)
			}
			tracker := CostAttributionTracker{Name: name}
			for _, label := range rawLabels {
				label, _ := label.(map[string]any)
				input, ok := label["input"].(string)
				if !ok {
					return trackers, fmt.Errorf("tracker %s has a label without input", name)
				}
				output, _ := label["output"].(string)
				if output == "" {
					output = input
				}
				tracker.Labels = append(tracker.Labels, AttributionLabel{Input: input, Output: output})
			}
			tracker.Internal, _ = config["internal"].(bool)
			trackers.Trackers = append(trackers.Trackers, tracker)
		}
	default:
		return trackers, fmt.Errorf("cost attribution trackers must be a map, got %v", value)
	}
	sort.Slice(trackers.Trackers, func(i, j int) bool { return trackers.Trackers[i].Name < trackers.Trackers[j].Name })
	return trackers, nil
}

func (t CostAttributionTrackers) IsEmpty() bool { return len(t.Trackers) == 0 }

// Merged returns t with other's trackers added, replacing any with the same name.
func (t CostAttributionTrackers) Merged(other CostAttributionTrackers) CostAttributionTrackers {
	byName := map[string]CostAttributionTracker{}
	for _, tracker := range t.Trackers {
		byName[tracker.Name] = tracker
	}
	for _, tracker := range other.Trackers {
		byName[tracker.Name] = tracker
	}
	merged := CostAttributionTrackers{}
	for _, tracker := range byName {
		merged.Trackers = append(merged.Trackers, tracker)
	}
	sort.Slice(merged.Trackers, func(i, j int) bool { return merged.Trackers[i].Name < merged.Trackers[j].Name })
	return merged
}

// Equal compares trackers by value.
func (t CostAttributionTrackers) Equal(other CostAttributionTrackers) bool {
	return slices.EqualFunc(t.Trackers, other.Trackers, func(a, b CostAttributionTracker) bool {
		return a.Name == b.Name && a.Internal == b.Internal && slices.Equal(a.Labels, b.Labels)
	})
}

const (
	MissingValue  = "__missing__"
	OverflowValue = "__overflow__"
)

// Key returns the attribution values of labels, `__missing__` for absent labels.
func (t *CostAttributionTracker) Key(ls labels.LabelSet) []string {
	key := make([]string, len(t.Labels))
	for i, label := range t.Labels {
		value := ls.Get(label.Input)
		if value == "" {
			value = MissingValue
		}
		key[i] = value
	}
	return key
}
