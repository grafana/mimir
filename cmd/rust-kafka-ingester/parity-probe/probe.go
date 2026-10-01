// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const requestTimeout = 5 * time.Minute

// How many generated selectors also drive the label and series lookups, which are slower.
const lookupSelectors = 12

func parseSelector(selector string) ([]*labels.Matcher, error) {
	return parser.NewParser(parser.Options{}).ParseMetricSelector(selector)
}

func queryRequest(selector string, start, end time.Time) (*client.QueryRequest, error) {
	matchers, err := parseSelector(selector)
	if err != nil {
		return nil, err
	}
	request, err := client.ToQueryRequest(model.TimeFromUnixNano(start.UnixNano()), model.TimeFromUnixNano(end.UnixNano()), matchers)
	if err != nil {
		return nil, err
	}
	request.StreamingChunksBatchSize = 64
	return request, nil
}

func labelMatchers(selector string) (*client.LabelMatchers, error) {
	if selector == "" {
		return nil, nil
	}
	matchers, err := parseSelector(selector)
	if err != nil {
		return nil, err
	}
	converted, err := client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	return &client.LabelMatchers{Matchers: converted}, nil
}

func probeTenant(cfg config, tenant string, ref target, targets []target, report *report) {
	random := rand.New(rand.NewPCG(cfg.generation.seed, 0))
	data, err := collect(cfg, tenant, ref.api, random)
	check(err)
	picked := sortedKeys(data.labelValues)
	selectors := append(append([]string{}, cfg.explicitSelectors...), generate(data, picked, cfg.generation, random)...)
	fmt.Printf("tenant=%s names_picked=%v selectors=%d\n", tenant, picked, len(selectors))
	for _, s := range selectors {
		_, err := parseSelector(s)
		check(err)
	}

	queryStreamChecks(cfg, tenant, ref, targets, selectors, report)

	ranges := []struct {
		name       string
		start, end time.Time
	}{{"window", cfg.start, cfg.end}, {"long", cfg.longStart, cfg.end}}
	lookupMatchers := append([]string{""}, selectors[:min(lookupSelectors, len(selectors))]...)
	for _, r := range ranges {
		for _, m := range lookupMatchers {
			matchers, err := labelMatchers(m)
			check(err)
			request := &client.LabelNamesRequest{StartTimestampMs: ms(r.start), EndTimestampMs: ms(r.end), Matchers: matchers}
			setCheck(report, ref, targets, "LabelNames", fmt.Sprintf("tenant=%s range=%s matchers=%s", tenant, r.name, m), cfg.verbose, func(api client.IngesterClient) ([]string, error) {
				response, err := api.LabelNames(timeout(tenant), request)
				if err != nil {
					return nil, err
				}
				return cloneAll(response.LabelNames), nil
			})
		}
		for _, label := range append([]string{"__name__"}, lookupLabels(data, picked)...) {
			label := label
			for _, m := range []string{"", matcherForLabel(data, picked, label)} {
				matchers, err := labelMatchers(m)
				check(err)
				request := &client.LabelValuesRequest{LabelName: label, StartTimestampMs: ms(r.start), EndTimestampMs: ms(r.end), Matchers: matchers}
				setCheck(report, ref, targets, "LabelValues", fmt.Sprintf("tenant=%s range=%s label=%s matchers=%s", tenant, r.name, label, m), cfg.verbose, func(api client.IngesterClient) ([]string, error) {
					response, err := api.LabelValues(timeout(tenant), request)
					if err != nil {
						return nil, err
					}
					return cloneAll(response.LabelValues), nil
				})
			}
		}
		for _, m := range lookupMatchers[1:] {
			matchers, err := parseSelector(m)
			check(err)
			request, err := client.ToMetricsForLabelMatchersRequest(model.TimeFromUnixNano(r.start.UnixNano()), model.TimeFromUnixNano(r.end.UnixNano()), nil, matchers)
			check(err)
			setCheck(report, ref, targets, "MetricsForLabelMatchers", fmt.Sprintf("tenant=%s range=%s selector=%s", tenant, r.name, m), cfg.verbose, func(api client.IngesterClient) ([]string, error) {
				response, err := api.MetricsForLabelMatchers(timeout(tenant), request)
				if err != nil {
					return nil, err
				}
				out := make([]string, 0, len(response.Metric))
				for _, metric := range response.Metric {
					out = append(out, mimirpb.FromLabelAdaptersToLabels(metric.Labels).String())
				}
				return out, nil
			})
		}
	}

	for _, name := range picked {
		labelNames := sortedKeys(data.labelValues[name])
		labelNames = append(labelNames[:min(3, len(labelNames))], "__name__")
		matchers, err := labelMatchers(selector(matcher{"__name__", "=", name}))
		check(err)
		for _, method := range []client.CountMethod{client.IN_MEMORY, client.ACTIVE} {
			request := &client.LabelValuesCardinalityRequest{LabelNames: labelNames, Matchers: matchers.Matchers, CountMethod: method}
			countCheck(report, ref, targets, endpointFor("LabelValuesCardinality", method), fmt.Sprintf("tenant=%s name=%s labels=%v method=%s", tenant, name, labelNames, method), toleranceFor(method), cfg.verbose, func(api client.IngesterClient) (map[string]uint64, error) {
				return cardinality(api, tenant, request)
			})
		}
	}
	for _, method := range []client.CountMethod{client.IN_MEMORY, client.ACTIVE} {
		countCheck(report, ref, targets, endpointFor("UserStats", method), fmt.Sprintf("tenant=%s method=%s", tenant, method), toleranceFor(method), cfg.verbose, func(api client.IngesterClient) (map[string]uint64, error) {
			stats, err := api.UserStats(timeout(tenant), &client.UserStatsRequest{CountMethod: method})
			if err != nil {
				return nil, err
			}
			return map[string]uint64{"num_series": stats.NumSeries}, nil
		})
	}
	countCheck(report, ref, targets, "AllUserStats", fmt.Sprintf("tenant=%s", tenant), 0, cfg.verbose, func(api client.IngesterClient) (map[string]uint64, error) {
		stats, err := api.AllUserStats(timeout(tenant), &client.UserStatsRequest{})
		if err != nil {
			return nil, err
		}
		for _, s := range stats.Stats {
			if s.UserId == tenant {
				return map[string]uint64{"num_series": s.Data.NumSeries}, nil
			}
		}
		return map[string]uint64{}, nil
	})
	report.note(fmt.Sprintf("active series counts (the ACTIVE endpoints) may differ by %.0f%%: each ingester drops idle series on its own update ticker, Go's TSDB ingester up to a minute after their idle timeout", activeTolerance*100))
	report.note("ingestion rates (UserStats/AllUserStats ingestion_rate, api_ingestion_rate, rule_ingestion_rate) are moving averages each ingester samples on its own ticker, so they aren't compared")
}

// collect reads what selectors are generated from, from the reference only.
func collect(cfg config, tenant string, api client.IngesterClient, random *rand.Rand) (tenantData, error) {
	data := tenantData{labelValues: map[string]map[string][]string{}, rareValues: map[string]map[string]uint64{}}
	nameCounts, err := cardinality(api, tenant, &client.LabelValuesCardinalityRequest{LabelNames: []string{"__name__"}, CountMethod: client.IN_MEMORY})
	if err != nil {
		return data, err
	}
	data.nameSeries = map[string]uint64{}
	for key, count := range nameCounts {
		data.nameSeries[strings.TrimPrefix(key, "__name__=")] = count
	}
	labelsSeen := map[string]bool{}
	for _, name := range pickNames(data.nameSeries, cfg.generation.names, cfg.generation.maxSeries, random) {
		matchers, err := labelMatchers(selector(matcher{"__name__", "=", name}))
		if err != nil {
			return data, err
		}
		names, err := api.LabelNames(timeout(tenant), &client.LabelNamesRequest{StartTimestampMs: ms(cfg.longStart), EndTimestampMs: ms(cfg.end), Matchers: matchers})
		if err != nil {
			return data, err
		}
		data.labelValues[name] = map[string][]string{}
		for _, label := range cloneAll(names.LabelNames) {
			if label == "__name__" {
				continue
			}
			values, err := api.LabelValues(timeout(tenant), &client.LabelValuesRequest{LabelName: label, StartTimestampMs: ms(cfg.longStart), EndTimestampMs: ms(cfg.end), Matchers: matchers})
			if err != nil {
				return data, err
			}
			data.labelValues[name][label] = cloneAll(values.LabelValues)
			labelsSeen[label] = true
		}
	}
	// Nameless selectors use a few of those labels, with each value's series across the tenant.
	rare := sortedKeys(labelsSeen)
	random.Shuffle(len(rare), func(i, j int) { rare[i], rare[j] = rare[j], rare[i] })
	for _, label := range rare[:min(3, len(rare))] {
		counts, err := cardinality(api, tenant, &client.LabelValuesCardinalityRequest{LabelNames: []string{label}, CountMethod: client.IN_MEMORY})
		if err != nil {
			return data, err
		}
		data.rareValues[label] = map[string]uint64{}
		for key, count := range counts {
			data.rareValues[label][strings.TrimPrefix(key, label+"=")] = count
		}
	}
	return data, nil
}

func lookupLabels(data tenantData, picked []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, name := range picked {
		for _, label := range sortedKeys(data.labelValues[name]) {
			if !seen[label] && len(out) < 4 {
				seen[label] = true
				out = append(out, label)
			}
		}
	}
	return out
}

// matcherForLabel is a name equality on a picked metric that has the label.
func matcherForLabel(data tenantData, picked []string, label string) string {
	for _, name := range picked {
		if _, ok := data.labelValues[name][label]; ok || label == "__name__" {
			return selector(matcher{"__name__", "=", name})
		}
	}
	return selector(matcher{"__name__", "=", picked[0]})
}

func cardinality(api client.IngesterClient, tenant string, request *client.LabelValuesCardinalityRequest) (map[string]uint64, error) {
	stream, err := api.LabelValuesCardinality(timeout(tenant), request)
	if err != nil {
		return nil, err
	}
	out := map[string]uint64{}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		for _, item := range response.Items {
			for value, count := range item.LabelValueSeries {
				out[strings.Clone(item.LabelName)+"="+strings.Clone(value)] += count
			}
		}
	}
}

func queryStreamChecks(cfg config, tenant string, ref target, targets []target, selectors []string, report *report) {
	fetch := func(api client.IngesterClient, selector string) (result, error) {
		request, err := queryRequest(selector, cfg.start, cfg.end)
		if err != nil {
			return result{}, err
		}
		return decode(timeout(tenant), api, request)
	}
	// Answers are compared twice before a difference counts, in case an ingester was a moment behind.
	run := func(endpoint, selector string) map[string]result {
		answers := map[string]result{}
		refAnswer, refErr := fetch(ref.api, selector)
		answers[ref.name] = refAnswer
		if refAnswer.unsorted > 0 {
			report.mismatch(ref.name, "QueryStream order", fmt.Sprintf("tenant=%s selector=%s unsorted=%d", tenant, selector, refAnswer.unsorted))
		}
		for _, t := range targets {
			answer, err := fetch(t.api, selector)
			differences := differ(refAnswer, refErr, answer, err)
			if len(differences) > 0 {
				refAnswer, refErr = fetch(ref.api, selector)
				answer, err = fetch(t.api, selector)
				differences = differ(refAnswer, refErr, answer, err)
			}
			answers[t.name] = answer
			label := fmt.Sprintf("tenant=%s selector=%s", tenant, selector)
			if answer.unsorted > 0 {
				report.mismatch(t.name, "QueryStream order", fmt.Sprintf("%s unsorted=%d", label, answer.unsorted))
			}
			if len(differences) > 0 {
				report.mismatch(t.name, endpoint, fmt.Sprintf("%s reference_series=%d compared_series=%d\n    %s", label, len(refAnswer.series), len(answer.series), strings.Join(differences[:min(5, len(differences))], "\n    ")))
			} else {
				report.pass(t.name, endpoint)
				if cfg.verbose {
					fmt.Printf("ok %s %s %s series=%d chunks=%d newest=%s\n", t.name, endpoint, label, len(answer.series), answer.chunks, time.UnixMilli(answer.newest).UTC().Format(time.RFC3339))
				}
			}
		}
		return answers
	}
	for _, s := range selectors {
		unsharded := run("QueryStream", s)
		for _, shard := range cfg.shards {
			run("QueryStream sharded", withShard(s, shard))
		}
		if cfg.unionShards <= 0 {
			continue
		}
		shardAnswers := map[string][]result{}
		for index := 1; index <= cfg.unionShards; index++ {
			for name, answer := range run("QueryStream sharded", withShard(s, fmt.Sprintf("%d_of_%d", index, cfg.unionShards))) {
				shardAnswers[name] = append(shardAnswers[name], answer)
			}
		}
		// Every implementation's shards together return its unsharded answer.
		for _, name := range append([]string{ref.name}, names(targets)...) {
			differences := compare(unsharded[name].series, union(shardAnswers[name]))
			label := fmt.Sprintf("tenant=%s selector=%s shards=%d", tenant, s, cfg.unionShards)
			if len(differences) > 0 {
				report.mismatch(name, "shard union", fmt.Sprintf("%s\n    %s", label, strings.Join(differences[:min(5, len(differences))], "\n    ")))
			} else {
				report.pass(name, "shard union")
			}
		}
	}
}

func differ(reference result, refErr error, compared result, err error) []string {
	if refErr != nil || err != nil {
		if refErr != nil && err != nil && status.Code(refErr) == status.Code(err) {
			return nil
		}
		return []string{fmt.Sprintf("errors differ: reference=%v compared=%v", refErr, err)}
	}
	return compare(reference.series, compared.series)
}

// setCheck compares a set-valued answer. Label and series lookups see the head as of now, which
// series may just have entered or left, so the reference is read before and after the compared
// ingester and an element only counts as a difference when the compared answer disagrees with both.
func setCheck(report *report, ref target, targets []target, endpoint, label string, verbose bool, fetch func(client.IngesterClient) ([]string, error)) {
	for _, t := range targets {
		before, beforeErr := fetch(ref.api)
		compared, err := fetch(t.api)
		after, afterErr := fetch(ref.api)
		if beforeErr != nil || err != nil || afterErr != nil {
			if beforeErr != nil && err != nil && status.Code(beforeErr) == status.Code(err) {
				report.pass(t.name, endpoint)
				continue
			}
			report.mismatch(t.name, endpoint, fmt.Sprintf("%s errors differ: reference=%v compared=%v", label, firstErr(beforeErr, afterErr), err))
			continue
		}
		differences, unstable := setDifferences(before, compared, after)
		if unstable > 0 {
			report.unstable(t.name, endpoint, unstable)
		}
		if len(differences) > 0 {
			report.mismatch(t.name, endpoint, fmt.Sprintf("%s reference=%d compared=%d\n    %s", label, len(before), len(compared), strings.Join(differences[:min(8, len(differences))], "\n    ")))
			continue
		}
		report.pass(t.name, endpoint)
		if verbose {
			fmt.Printf("ok %s %s %s values=%d\n", t.name, endpoint, label, len(compared))
		}
	}
}

func setDifferences(before, compared, after []string) ([]string, int) {
	inBefore, inCompared, inAfter := toSet(before), toSet(compared), toSet(after)
	all := map[string]bool{}
	for _, set := range []map[string]bool{inBefore, inCompared, inAfter} {
		for value := range set {
			all[value] = true
		}
	}
	var differences []string
	unstable := 0
	for value := range all {
		if inBefore[value] != inAfter[value] {
			unstable++
		}
		switch {
		case inCompared[value] && !inBefore[value] && !inAfter[value]:
			differences = append(differences, "only in compared: "+value)
		case !inCompared[value] && inBefore[value] && inAfter[value]:
			differences = append(differences, "only in reference: "+value)
		}
	}
	sort.Strings(differences)
	return differences, unstable
}

// countCheck compares counts, which may move between reads: a compared count passes when it's
// between the reference's counts read before and after it.
func countCheck(report *report, ref target, targets []target, endpoint, label string, tolerance float64, verbose bool, fetch func(client.IngesterClient) (map[string]uint64, error)) {
	for _, t := range targets {
		before, beforeErr := fetch(ref.api)
		compared, err := fetch(t.api)
		after, afterErr := fetch(ref.api)
		if beforeErr != nil || err != nil || afterErr != nil {
			if beforeErr != nil && err != nil && status.Code(beforeErr) == status.Code(err) {
				report.pass(t.name, endpoint)
				continue
			}
			report.mismatch(t.name, endpoint, fmt.Sprintf("%s errors differ: reference=%v compared=%v", label, firstErr(beforeErr, afterErr), err))
			continue
		}
		differences := countDifferences(before, compared, after, tolerance)
		if len(differences) > 0 {
			report.mismatch(t.name, endpoint, fmt.Sprintf("%s\n    %s", label, strings.Join(differences[:min(8, len(differences))], "\n    ")))
			continue
		}
		report.pass(t.name, endpoint)
		if verbose {
			fmt.Printf("ok %s %s %s keys=%d\n", t.name, endpoint, label, len(compared))
		}
	}
}

// tolerance widens the reference's range by that fraction of it, at least by one.
func countDifferences(before, compared, after map[string]uint64, tolerance float64) []string {
	keys := map[string]bool{}
	for _, m := range []map[string]uint64{before, compared, after} {
		for key := range m {
			keys[key] = true
		}
	}
	var differences []string
	for key := range keys {
		low, high := min(before[key], after[key]), max(before[key], after[key])
		if tolerance > 0 {
			slack := max(uint64(float64(high)*tolerance), 1)
			low, high = low-min(low, slack), high+slack
		}
		if c := compared[key]; c < low || c > high {
			differences = append(differences, fmt.Sprintf("%s: reference=%d..%d compared=%d", key, low, high, c))
		}
	}
	sort.Strings(differences)
	return differences
}

func toSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func cloneAll(values []string) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = strings.Clone(value)
	}
	return out
}

func ms(t time.Time) int64 { return t.UnixMilli() }

// timeout bounds a request on a stuck ingester; its timer releases the context afterwards.
func timeout(tenant string) context.Context {
	ctx, cancel := context.WithTimeout(outgoing(tenant), requestTimeout)
	time.AfterFunc(requestTimeout, cancel)
	return ctx
}

// The share active series counts may differ by: series that just went idle are counted until the
// ingester's next update, which each implementation runs on its own schedule.
const activeTolerance = 0.01

func toleranceFor(method client.CountMethod) float64 {
	if method == client.ACTIVE {
		return activeTolerance
	}
	return 0
}

func endpointFor(endpoint string, method client.CountMethod) string {
	if method == client.ACTIVE {
		return endpoint + " (active)"
	}
	return endpoint
}
