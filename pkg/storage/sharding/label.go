// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	// ShardLabel is a reserved label referencing a shard on read path.
	ShardLabel = "__query_shard__"
)

// ShardSelector holds information about the configured query shard.
type ShardSelector struct {
	ShardIndex uint64
	ShardCount uint64
	ByLabels   []string
}

// LabelValue returns the label value to use to select this shard.
func (shard ShardSelector) LabelValue() string {
	value := FormatShardIDLabelValue(shard.ShardIndex, shard.ShardCount)
	if len(shard.ByLabels) == 0 {
		return value
	}

	byLabels := slices.Clone(shard.ByLabels)
	slices.Sort(byLabels)
	for i := range byLabels {
		byLabels[i] = url.PathEscape(byLabels[i])
	}
	return value + "_by_" + strings.Join(byLabels, ",")
}

// Label generates the ShardSelector as a label.
func (shard ShardSelector) Label() labels.Label {
	return labels.Label{
		Name:  ShardLabel,
		Value: shard.LabelValue(),
	}
}

// Matcher converts ShardSelector to Matcher.
func (shard ShardSelector) Matcher() *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchEqual, ShardLabel, shard.LabelValue())
}

// ShardFromMatchers extracts a ShardSelector and the index it was pulled from the matcher list.
func ShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, idx int, err error) {
	for i, matcher := range matchers {
		if matcher.Name == ShardLabel && matcher.Type == labels.MatchEqual {
			shard, err := ParseQueryShardLabelValue(matcher.Value)
			if err != nil {
				return nil, i, err
			}
			return shard, i, nil
		}
	}
	return nil, 0, nil
}

// RemoveShardFromMatchers returns the input matchers without the label matcher on the query shard (if any).
func RemoveShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, filtered []*labels.Matcher, err error) {
	shard, idx, err := ShardFromMatchers(matchers)
	if err != nil || shard == nil {
		return nil, matchers, err
	}

	// Create a new slice with the shard matcher removed.
	filtered = make([]*labels.Matcher, 0, len(matchers)-1)
	filtered = append(filtered, matchers[:idx]...)
	filtered = append(filtered, matchers[idx+1:]...)

	return shard, filtered, nil
}

// FormatShardIDLabelValue expects 0-based shardID, but uses 1-based shard in the output string.
func FormatShardIDLabelValue(shardID, shardCount uint64) string {
	return fmt.Sprintf("%d_of_%d", shardID+1, shardCount)
}

// ParseShardIDLabelValue returns original (0-based) shard index and shard count parsed from formatted value.
func ParseShardIDLabelValue(val string) (index, shardCount uint64, _ error) {
	// If we fail to parse shardID, we better not consider this block fully included in successors.
	matches := strings.Split(val, "_")
	if len(matches) != 3 || matches[1] != "of" {
		return 0, 0, errors.Errorf("invalid shard ID: %q", val)
	}

	index, err := strconv.ParseUint(matches[0], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("invalid shard ID: %q: %v", val, err)
	}
	count, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return 0, 0, errors.Errorf("invalid shard ID: %q: %v", val, err)
	}

	if index == 0 || count == 0 || index > count {
		return 0, 0, errors.Errorf("invalid shard ID: %q", val)
	}

	return index - 1, count, nil
}

// ParseQueryShardLabelValue parses a query shard selector, including optional subset labels.
// ParseShardIDLabelValue intentionally remains limited to classic shard IDs used by compactor metadata.
func ParseQueryShardLabelValue(value string) (*ShardSelector, error) {
	parts := strings.SplitN(value, "_by_", 2)
	index, count, err := ParseShardIDLabelValue(parts[0])
	if err != nil {
		return nil, err
	}

	shard := &ShardSelector{ShardIndex: index, ShardCount: count}
	if len(parts) == 1 {
		return shard, nil
	}
	if parts[1] == "" {
		return nil, errors.Errorf("invalid shard ID: %q", value)
	}

	for _, encoded := range strings.Split(parts[1], ",") {
		name, err := url.PathUnescape(encoded)
		if err != nil || !model.UTF8Validation.IsValidLabelName(name) {
			return nil, errors.Errorf("invalid shard ID: %q", value)
		}
		shard.ByLabels = append(shard.ByLabels, name)
	}
	if !slices.IsSorted(shard.ByLabels) {
		return nil, errors.Errorf("invalid shard ID: %q", value)
	}
	for i := 1; i < len(shard.ByLabels); i++ {
		if shard.ByLabels[i] == shard.ByLabels[i-1] {
			return nil, errors.Errorf("invalid shard ID: %q", value)
		}
	}

	return shard, nil
}
