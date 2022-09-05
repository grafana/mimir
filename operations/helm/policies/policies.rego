package main

import future.keywords.every

has_key(x, k) {
	_ = x[k]
}

deny["Resource doesn't have a namespace"] {
	namespace := input[_].contents.metadata.namespace
	not regex.match(".+", namespace)
}

deny["Resource doesn't have a namespace"] {
	metadata := input[_].contents.metadata
	not has_key(metadata, "namespace")
}

can_use_topology_spread_constraints(kind) {
	kind in ["StatefulSet", "Deployment"]
}

pod_template_has_labels(template, labels) {
	pod_labels := template.metadata.labels
	every label in labels {
		some pod_label in pod_labels
		pod_label == label
	}
}

deny["TopologySpreadConstraints use labels not presnet on pods"] {
	can_use_topology_spread_constraints(input[i].contents.kind)

	pod_template := input[i].contents.spec.template
	topology_spread_constraints_labels := input[i].contents.spec.template.spec.topologySpreadConstraints[j].labelSelector.matchLabels

	not pod_template_has_labels(pod_template, topology_spread_constraints_labels)
}
