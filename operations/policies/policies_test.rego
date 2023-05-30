package main

import future.keywords

test_no_namespace_not_allowed if {
	deny[reason] with input as [{"contents": {"metadata": {"name": "resource"}}}]
	contains(reason, "Resource doesn't have a namespace")
}

test_empty_namespace_not_allowed if {
	deny[reason] with input as [object.union(passing_deployment, {"contents": {"metadata": {"name": "resource", "namespace": ""}}})]
	contains(reason, "Resource doesn't have a namespace")
}

test_null_namespace_not_allowed if {
	deny[reason] with input as [object.union(passing_deployment, {"contents": {"metadata": {"name": "resource", "namespace": null}}})]
	contains(reason, "Resource doesn't have a namespace")
}

test_namespace_forbidden_on_psp if {
	deny[reason] with input as [{"contents": {
		"kind": "PodSecurityPolicy",
		"metadata": {"name": "resource", "namespace": "example"},
	}}]

	contains(reason, "Resource has a namespace, but shouldn't")
}

passing_psp := {"contents": {
	"kind": "PodSecurityPolicy",
	"metadata": {"name": "resource"},
}}

test_passing_psp_without_namespace if {
	count(deny) == 0 with input as [passing_psp]
}

passing_container := {
	"image": "grafana/mimir",
	"securityContext": {
		"readOnlyRootFilesystem": true,
		"allowPrivilegeEscalation": false,
		"capabilities": {"drop": ["ALL"]},
	},
}

passing_deployment := {"contents": {
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
		"spec": {
			"securityContext": {
				"fsGroup": 10001,
				"runAsGroup": 10001,
				"runAsNonRoot": true,
				"runAsUser": 10001,
				"seccompProfile": {"type": "RuntimeDefault"},
			},
			"topologySpreadConstraints": [{
				"labelSelector": {"matchLabels": {"name": "query-frontend"}},
				"maxSkew": 1,
				"topologyKey": "kubernetes.io/hostname",
				"whenUnsatisfiable": "ScheduleAnyway",
			}],
			"containers": [passing_container],
		},
	}},
}}

test_passing_deployment if {
	count(deny) == 0 with input as [passing_deployment]
}
