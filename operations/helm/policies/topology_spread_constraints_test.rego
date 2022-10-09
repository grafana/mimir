package main

import future.keywords

test_topology_spread_constraints_use_wrong_labels if {
	deny[reason] with input as [{"contents": {
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

	contains(reason, "TopologySpreadConstraints use labels not presnet on pods")
}
