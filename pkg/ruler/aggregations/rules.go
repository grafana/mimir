package aggregations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"

	mimirpb "github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

var aggregationTypeMapping = map[string]bool{
	"sum": true,
	"avg": true,
	"max": true,
	"min": true,
}

var timeAggregationTypeMapping = map[string]bool{
	"last":          true,
	"sum_over_time": true,
	"avg_over_time": true,
	"max_over_time": true,
	"min_over_time": true,
}

type Rule struct {
	Metric          string   `json:"metric"`
	Labels          []string `json:"labels"`
	Aggregation     string   `json:"aggregation,omitempty"`
	TimeAggregation string   `json:"time_aggregation,omitempty"`
}

type Rules struct {
	Encoded []byte
	Decoded []Rule
}

func (r Rules) Validate() error {
	for i, rule := range r.Decoded {
		if rule.getTransforms() == nil {
			return fmt.Errorf("rule %d for metric %q is unsupported", i, rule.Metric)
		}
	}
	return nil
}

func (r *Rules) Sort() {
	sort.SliceStable(r.Decoded, func(i, j int) bool {
		return r.Decoded[i].Metric < r.Decoded[j].Metric
	})

	for ruleIdx := range r.Decoded {
		sort.Strings(r.Decoded[ruleIdx].Labels)
	}
}

// Reencode updates Encoded version of the rules from Decoded field.
func (r *Rules) Reencode() error {
	enc, err := json.MarshalIndent(r.Decoded, "", "  ")
	if err != nil {
		return err
	}
	r.Encoded = enc
	return nil
}

func LoadRulesFromFile(filename string) (Rules, error) {
	if filename == "-" {
		return LoadRules(os.Stdin)
	}

	f, err := os.Open(filename)
	if err != nil {
		return Rules{}, err
	}
	defer func() {
		_ = f.Close()
	}()

	return LoadRules(f)
}

func LoadRules(reader io.Reader) (Rules, error) {
	var r Rules

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return r, err
	}

	r.Encoded = b
	if err := json.Unmarshal(b, &r.Decoded); err != nil {
		return r, err
	}
	r.Sort()
	return r, nil
}

// getTransforms returns the transformations for the given rule or nil if it's not supported.
func (r Rule) getTransforms() []*rulespb.RuleDesc {
	// TODO: Backwards compatibility. Remove once all rules are updated.
	if r.Aggregation == "" && r.TimeAggregation == "" {
		r.Aggregation, r.TimeAggregation = "sum", "counter"
	}

	if r.Aggregation == "sum" && r.TimeAggregation == "counter" {
		sort.Strings(r.Labels)
		lblsToAggregateOn := bytes.Buffer{}
		sep := ""
		for _, l := range r.Labels {
			lblsToAggregateOn.WriteString(sep)
			lblsToAggregateOn.WriteString(strconv.Quote(l))
			sep = ", "
		}
		return []*rulespb.RuleDesc{
			{
				Expr:   fmt.Sprintf(`aggregate_counters(%s{__ephemeral__="both"}[5m], 60000, %s)`, r.Metric, lblsToAggregateOn.String()),
				Record: r.Metric,
				Labels: []mimirpb.LabelAdapter{
					{
						Name:  "__dropped_labels__",
						Value: strings.Join(r.Labels, ","),
					},
				},
			},
		}
	}

	return nil
}

func RulesEqual(r1, r2 Rules) bool {
	if len(r1.Decoded) != len(r2.Decoded) {
		return false
	}

	for idx := range r1.Decoded {
		if r1.Decoded[idx].Metric != r2.Decoded[idx].Metric {
			return false
		}
		if len(r1.Decoded[idx].Labels) != len(r2.Decoded[idx].Labels) {
			return false
		}
		for labelIdx := range r1.Decoded[idx].Labels {
			if r1.Decoded[idx].Labels[labelIdx] != r2.Decoded[idx].Labels[labelIdx] {
				return false
			}
		}
	}

	return true
}
