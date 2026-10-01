// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"sort"
	"strings"
)

type tally struct {
	checks, mismatches, unstable int
}

// report counts checks and mismatches per compared ingester and endpoint, and prints mismatches
// as they're found.
type report struct {
	byTarget map[string]map[string]*tally
	notes    map[string]bool
}

func newReport() *report {
	return &report{byTarget: map[string]map[string]*tally{}, notes: map[string]bool{}}
}

func (r *report) get(name, endpoint string) *tally {
	if r.byTarget[name] == nil {
		r.byTarget[name] = map[string]*tally{}
	}
	if r.byTarget[name][endpoint] == nil {
		r.byTarget[name][endpoint] = &tally{}
	}
	return r.byTarget[name][endpoint]
}

func (r *report) pass(name, endpoint string) {
	r.get(name, endpoint).checks++
}

func (r *report) mismatch(name, endpoint, detail string) {
	t := r.get(name, endpoint)
	t.checks++
	t.mismatches++
	fmt.Printf("MISMATCH %s %s %s\n", name, endpoint, detail)
}

// unstable counts answers that changed between the two reference reads around a compared one.
func (r *report) unstable(name, endpoint string, count int) {
	r.get(name, endpoint).unstable += count
}

func (r *report) note(text string) {
	r.notes[text] = true
}

func (r *report) failed() bool {
	for _, endpoints := range r.byTarget {
		for _, t := range endpoints {
			if t.mismatches > 0 {
				return true
			}
		}
	}
	return false
}

func (r *report) print(compared []string) {
	var names []string
	for name := range r.byTarget {
		names = append(names, name)
	}
	// Compared ingesters first, in flag order, then the reference's own shard checks.
	order := map[string]int{}
	for i, name := range compared {
		order[name] = i
	}
	sort.SliceStable(names, func(i, j int) bool {
		oi, iok := order[names[i]]
		oj, jok := order[names[j]]
		if iok != jok {
			return iok
		}
		if iok {
			return oi < oj
		}
		return names[i] < names[j]
	})
	fmt.Println()
	fmt.Printf("%-14s %-26s %8s %10s %9s\n", "INGESTER", "ENDPOINT", "CHECKS", "MISMATCHES", "UNSTABLE")
	for _, name := range names {
		endpoints := make([]string, 0, len(r.byTarget[name]))
		for endpoint := range r.byTarget[name] {
			endpoints = append(endpoints, endpoint)
		}
		sort.Strings(endpoints)
		for _, endpoint := range endpoints {
			t := r.byTarget[name][endpoint]
			fmt.Printf("%-14s %-26s %8d %10d %9d\n", name, endpoint, t.checks, t.mismatches, t.unstable)
		}
	}
	notes := make([]string, 0, len(r.notes))
	for note := range r.notes {
		notes = append(notes, note)
	}
	sort.Strings(notes)
	if len(notes) > 0 {
		fmt.Println("\nnotes:\n  " + strings.Join(notes, "\n  "))
	}
}
