package main

import future.keywords.every

has_key(x, k) {
	_ = x[k]
}

should_be_namespaced(contents) {
	# if we don't know the kind, then it should have a namespace
	not has_key(contents, "kind")
}

should_be_namespaced(contents) {
	not contents.kind in ["PodSecurityPolicy"]
}

metadata_has_namespace(metadata) {
	has_key(metadata, "namespace")
	regex.match(".+", metadata.namespace)
}

deny[msg] {
	obj := input[i].contents
	msg := sprintf("Resource doesn't have a namespace %v", [obj])

	should_be_namespaced(obj)
	not metadata_has_namespace(obj.metadata)
}

deny[msg] {
	obj := input[i].contents
	msg := sprintf("Resource has a namespace, but shouldn't %v", [obj])

	not should_be_namespaced(obj)
	metadata_has_namespace(obj.metadata)
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
