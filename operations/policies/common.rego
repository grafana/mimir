package main

object_display_name[i] := display_name {
	contents := input[i].contents
	contents != null
	kind := object.get(contents, "kind", "<unknown>")
	name := object.get(object.get(contents, "metadata", {}), "name", "<unknown>")
	display_name := sprintf("%v/%v", [kind, name])
}

has_key(x, k) {
	_ = x[k]
}
