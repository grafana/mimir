// SPDX-License-Identifier: AGPL-3.0-only

package querysharding

import (
	"fmt"
	"regexp"
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

var (
	// ShardLabelRE is a regexp used to parse ShardLabel value.
	ShardLabelRE = regexp.MustCompile("^[0-9]+_of_[0-9]+$")
)

// ShardSelector holds information about the configured query shard.
type ShardSelector struct {
	ShardIndex uint64
	ShardCount uint64
}

// LabelValue returns the label value to use to select this shard.
func (shard ShardSelector) LabelValue() string {
	return fmt.Sprintf(ShardLabelFmt, shard.ShardIndex, shard.ShardCount)
}

// Label generates the ShardSelector as a label.
func (shard ShardSelector) Label() labels.Label {
	return labels.Label{
		Name:  ShardLabel,
		Value: shard.LabelValue(),
	}
}

// ParseShard parses the input label value and extracts the shard information.
func ParseShard(input string) (parsed ShardSelector, err error) {
	if !ShardLabelRE.MatchString(input) {
		return parsed, errors.Errorf("invalid query sharding label value: [%s]", input)
	}

	matches := strings.Split(input, "_")
	x, err := strconv.Atoi(matches[0])
	if err != nil {
		return parsed, err
	}
	of, err := strconv.Atoi(matches[2])
	if err != nil {
		return parsed, err
	}

	if x >= of {
		return parsed, errors.Errorf("query shards out of bounds: [%d] >= [%d]", x, of)
	}
	return ShardSelector{
		ShardIndex: uint64(x),
		ShardCount: uint64(of),
	}, err
}

// ShardFromMatchers extracts a ShardSelector and the index it was pulled from the matcher list.
func ShardFromMatchers(matchers []*labels.Matcher) (shard *ShardSelector, idx int, err error) {
	for i, matcher := range matchers {
		if matcher.Name == ShardLabel && matcher.Type == labels.MatchEqual {
			shard, err := ParseShard(matcher.Value)
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
