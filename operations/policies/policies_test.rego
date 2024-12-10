package main

import future.keywords

no_denies if {
	trace(sprintf("Denies: %v", [deny]))
	count(deny) == 0
}

test_no_namespace_not_allowed if {
	deny[reason] with input as [{"contents": {"metadata": {"name": "resource"}}}]
	contains(reason, "Resource doesn't have a namespace")
}

test_no_namespace_allowed if {
	no_denies with input as [{"contents": {"kind": "Namespace", "metadata": {"name": "the-namespace"}}}]
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

test_empty_node_selector_not_allowed if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"nodeSelector": {}}}}}},
	)]

	contains(reason, "Resource has empty nodeSelector, but shouldn't")
}

test_empty_affinity_not_allowed if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"affinity": {}}}}}},
	)]

	contains(reason, "Resource has empty affinity, but shouldn't")
}

test_empty_init_containers_not_allowed if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"initContainers": []}}}}},
	)]

	contains(reason, "Resource has empty initContainers, but shouldn't")
}

test_empty_tolerations_not_allowed if {
	deny[reason] with input as [object.union(
		passing_deployment,
		{"contents": {"spec": {"template": {"spec": {"tolerations": []}}}}},
	)]

	contains(reason, "Resource has empty tolerations, but shouldn't")
}

passing_psp := {"contents": {
	"kind": "PodSecurityPolicy",
	"metadata": {"name": "resource"},
}}

test_passing_psp_without_namespace if {
	no_denies with input as [passing_psp]
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
	no_denies with input as [passing_deployment]
}
