// SPDX-License-Identifier: AGPL-3.0-only

package service

import (
	"cmp"
	"math"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func search(values []string, filter *client.SearchFilter, ordering client.SearchOrdering, limit int64, send func(*client.SearchResultBatch) error) error {
	if limit < 0 {
		return status.Error(codes.InvalidArgument, "limit must be >= 0")
	}
	if ordering < 0 || ordering > 2 {
		return status.Error(codes.InvalidArgument, "invalid search ordering")
	}
	if filter != nil {
		if filter.FuzzThreshold < 0 || filter.FuzzThreshold > 100 {
			return status.Error(codes.InvalidArgument, "fuzz threshold must be between 0 and 100")
		}
		if slices.Contains(filter.Terms, "") {
			return status.Error(codes.InvalidArgument, "search terms must not be empty")
		}
		if filter.FuzzAlg != 0 && filter.FuzzAlg != 1 {
			return status.Error(codes.InvalidArgument, "invalid fuzzy algorithm")
		}
	}
	results := make([]client.SearchResultBatch_Result, 0, len(values))
	for _, value := range values {
		if matched, ok := score(value, filter); ok {
			results = append(results, client.SearchResultBatch_Result{Value: value, Score: matched})
		}
	}
	slices.SortStableFunc(results, func(a, b client.SearchResultBatch_Result) int {
		switch ordering {
		case client.ORDER_BY_VALUE_DESC:
			return cmp.Compare(b.Value, a.Value)
		case client.ORDER_BY_SCORE_DESC:
			// Like Rust's partial_cmp, NaN scores compare equal.
			if b.Score > a.Score {
				return 1
			}
			if b.Score < a.Score {
				return -1
			}
			return cmp.Compare(a.Value, b.Value)
		default:
			return cmp.Compare(a.Value, b.Value)
		}
	})
	if limit > 0 && int64(len(results)) > limit {
		results = results[:limit]
	}
	for start := 0; start < len(results); start += searchBatchSize {
		end := min(start+searchBatchSize, len(results))
		if err := send(&client.SearchResultBatch{Results: results[start:end]}); err != nil {
			return err
		}
	}
	return nil
}

// score is the best score of the filter's terms for value, and false when none matches.
func score(value string, filter *client.SearchFilter) (float64, bool) {
	if filter == nil || len(filter.Terms) == 0 {
		return 1, true
	}
	candidate := value
	if filter.CaseInsensitive {
		candidate = strings.ToLower(value)
	}
	threshold := float64(filter.FuzzThreshold) / 100
	best, matched := 0.0, false
	for _, term := range filter.Terms {
		if filter.CaseInsensitive {
			term = strings.ToLower(term)
		}
		var termScore float64
		var ok bool
		if filter.FuzzAlg == 1 {
			termScore, ok = containsScore(term, candidate)
			if !ok {
				termScore = jaroWinkler(term, candidate)
				ok = filter.FuzzThreshold > 0 && termScore >= threshold
			}
		} else {
			termScore = subsequenceScore(term, candidate)
			ok = termScore > 0 && termScore >= threshold
		}
		if ok && (!matched || termScore > best) {
			best, matched = termScore, true
		}
	}
	return best, matched
}

// containsScore scores a term found in value by how early it starts, in bytes.
func containsScore(term, value string) (float64, bool) {
	index := strings.Index(value, term)
	if index < 0 {
		return 0, false
	}
	if index == 0 {
		return 1, true
	}
	return 1 - 0.9*float64(index)/float64(len(value)-len(term)), true
}

func subsequenceScore(patternText, text string) float64 {
	if patternText == text || strings.HasPrefix(text, patternText) {
		return 1
	}
	pattern, runes := []rune(patternText), []rune(text)
	if len(pattern) == 0 || len(pattern) > len(runes) {
		return 0
	}
	best := math.Inf(-1)
	for start := 0; start <= len(runes)-len(pattern); start++ {
		if runes[start] != pattern[0] {
			continue
		}
		patternIndex, index := 0, start
		score := -(float64(start) / float64(len(runes)))
		previous, hasPrevious := 0, false
		for index < len(runes) && patternIndex < len(pattern) {
			if runes[index] == pattern[patternIndex] {
				runStart := index
				for index < len(runes) && patternIndex < len(pattern) && runes[index] == pattern[patternIndex] {
					index++
					patternIndex++
				}
				if hasPrevious {
					score -= float64(runStart-previous-1) / float64(len(runes))
				}
				run := index - runStart
				score += float64(run * run)
				previous, hasPrevious = index-1, true
			} else {
				index++
			}
		}
		if patternIndex == len(pattern) {
			score -= float64(len(runes)-index) / (2 * float64(len(runes)))
			best = math.Max(best, score)
		}
	}
	if math.IsInf(best, 0) || math.IsNaN(best) {
		return 0
	}
	return min(max(best/float64(len(pattern)*len(pattern)), 0), 1) * 0.999
}

func jaroWinkler(firstText, secondText string) float64 {
	if firstText == secondText {
		return 1
	}
	first, second := []rune(firstText), []rune(secondText)
	if len(first) == 0 || len(second) == 0 {
		return 0
	}
	if len(first) > len(second) {
		first, second = second, first
	}
	distance := max(len(second)/2-1, 0)
	firstMatches := make([]bool, len(first))
	secondMatches := make([]bool, len(second))
	matches := 0.0
	for i := range first {
		for j := max(i-distance, 0); j <= min(i+distance, len(second)-1); j++ {
			if !secondMatches[j] && first[i] == second[j] {
				firstMatches[i], secondMatches[j] = true, true
				matches++
				break
			}
		}
	}
	if matches == 0 {
		return 0
	}
	var matchedSecond []rune
	for j, matched := range secondMatches {
		if matched {
			matchedSecond = append(matchedSecond, second[j])
		}
	}
	transpositions, k := 0.0, 0
	for i, matched := range firstMatches {
		if !matched {
			continue
		}
		if first[i] != matchedSecond[k] {
			transpositions++
		}
		k++
	}
	jaro := (matches/float64(len(first)) + matches/float64(len(second)) + (matches-transpositions/2)/matches) / 3
	prefix := 0
	for i := 0; i < min(4, len(first), len(second)) && first[i] == second[i]; i++ {
		prefix++
	}
	return jaro + float64(prefix)*0.1*(1-jaro)
}
