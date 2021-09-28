// SPDX-License-Identifier: AGPL-3.0-only

package querysharding

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	// ShardLabel is a reserved label referencing a shard.
	ShardLabel = "__query_shard__"

	// ShardLabelFmt is the format of the ShardLabel value.
	ShardLabelFmt = "%d_of_%d"
)

// ShardSelector holds information about the configured query shard.
type ShardSelector struct {
	ShardIndex uint64
	ShardCount uint64
}

// LabelValue returns the label value to use to select this shard.
func (shard ShardSelector) LabelValue() string {
	return fmt.Sprintf(ShardLabelFmt, shard.ShardIndex+1, shard.ShardCount)
}

// Label generates the ShardSelector as a label.
func (shard ShardSelector) Label() labels.Label {
	return labels.Label{
		Name:  ShardLabel,
		Value: shard.LabelValue(),
	}
}

// parseShard parses the input label value and extracts the shard information.
func parseShard(input string) (parsed ShardSelector, err error) {
	matches := strings.Split(input, "_")
	if len(matches) != 3 || matches[1] != "of" {
		return parsed, errors.Errorf("invalid query sharding label value: %s", input)
	}

	index, err := strconv.ParseUint(matches[0], 10, 64)
	if err != nil {
		return parsed, err
	}
	count, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return parsed, err
	}

	if index > count {
		return parsed, errors.Errorf("query shards out of bounds: %d > %d", index, count)
	}
	if index <= 0 {
		return parsed, errors.Errorf("query shards out of bounds: %d <= 0", index)
	}
	return ShardSelector{
		ShardIndex: index - 1,
		ShardCount: count,
	}, err
}

// ShardFromMatchers extracts a ShardSelector and the index it was pulled from the matcher list.
func ShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, idx int, err error) {
	for i, matcher := range matchers {
		if matcher.Name == ShardLabel && matcher.Type == labels.MatchEqual {
			shard, err := parseShard(matcher.Value)
			if err != nil {
				return nil, i, err
			}
			return &shard, i, nil
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
