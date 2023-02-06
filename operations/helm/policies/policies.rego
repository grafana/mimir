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

deny[msg] {
	obj := input[i].contents
	msg := sprintf("TopologySpreadConstraints use labels not present on pods: %v", [obj])
	can_use_topology_spread_constraints(obj.kind)

	pod_template := obj.spec.template
	topology_spread_constraints_labels := obj.spec.template.spec.topologySpreadConstraints[j].labelSelector.matchLabels

	not pod_template_has_labels(pod_template, topology_spread_constraints_labels)
}

is_mimir_or_gem_image(image) {
	startswith(image, "grafana/mimir")
}

is_mimir_or_gem_image(image) {
	startswith(image, "grafana/enterprise-metrics")
}

deny[msg] {
	obj := input[i].contents
	msg := sprintf("Mimir or GEM containers do not have the restricted security context: %v", [obj])

	obj.kind in ["StatefulSet", "Deployment"]
	container := obj.spec.template.spec.containers[j]
	is_mimir_or_gem_image(container.image)
	required_security_context := {
		"allowPrivilegeEscalation": false,
		"readOnlyRootFilesystem": true,
		"capabilities": {"drop": ["ALL"]},
	}

	some field, value in required_security_context
	object.get(container.securityContext, field, "") != value
}

deny[msg] {
	obj := input[i].contents
	msg := sprintf("The Mimir or GEM Pod doesn't have the restricted security context: %v", [obj])

	obj.kind in ["StatefulSet", "Deployment"]
	pod_spec := obj.spec.template.spec
	is_mimir_or_gem_image(pod_spec.containers[j].image)
	required_security_context := {
		"fsGroup": 10001,
		"runAsGroup": 10001,
		"runAsUser": 10001,
		"runAsNonRoot": true,
		"seccompProfile": {"type": "RuntimeDefault"},
	}

	some field, value in required_security_context
	object.get(pod_spec.securityContext, field, "") != value
}
