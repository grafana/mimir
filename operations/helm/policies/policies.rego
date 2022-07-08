package main

has_key(x, k) {
	_ = x[k]
}

deny["Resource doesn't have a namespace"] {
	not regex.match(".+", input.metadata.namespace)
}

deny["Resource doesn't have a namespace"] {
	not has_key(input.metadata, "namespace")
}
