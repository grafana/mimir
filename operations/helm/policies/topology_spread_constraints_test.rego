package main

import future.keywords

test_topology_spread_constraints_use_correct_labels if {
	no_violations with input as [{"contents": {
		"metadata": {
			"name": "resource",
			"namespace": "x",
		},
		"kind": "Deployment",
		"spec": {"template": {
			"metadata": {"labels": {
				"name": "query-frontend",
				"some-other-label": "query-frontend",
			}},
			"spec": {"topologySpreadConstraints": [{
				"labelSelector": {"matchLabels": {"name": "query-frontend"}},
				"maxSkew": 1,
				"topologyKey": "kubernetes.io/hostname",
				"whenUnsatisfiable": "ScheduleAnyway",
			}]},
		}},
	}}]
}

test_topology_spread_constraints_use_wrong_labels if {
	some_violations with input as [{"contents": {
		"metadata": {
			"name": "resource",
			"namespace": "x",
		},
		"kind": "Deployment",
		"spec": {
			"metadata": {"labels": {"some-other-label": "query-frontend"}},
			"template": {"spec": {"topologySpreadConstraints": [{
				"labelSelector": {"matchLabels": {"name": "query-frontend"}},
				"maxSkew": 1,
				"topologyKey": "kubernetes.io/hostname",
				"whenUnsatisfiable": "ScheduleAnyway",
			}]}},
		},
	}}]
}
