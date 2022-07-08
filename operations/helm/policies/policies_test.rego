package main

import future.keywords

no_violations if {
	trace(sprintf("%v", [deny]))
	count(deny) == 0
}

some_violaions if {
	trace(sprintf("%v", [deny]))
	count(deny) > 0
}

test_no_namespace_not_allowed if {
	some_violaions with input as {"metadata": {"name": "resource"}}
}

test_empty_namespace_not_allowed if {
	some_violaions with input as {"metadata": {"name": "resource", "namespace": ""}}
}

test_null_namespace_not_allowed if {
	some_violaions with input as {"metadata": {"name": "resource", "namespace": null}}
}

test_namespace_allowed if {
	no_violations with input as {"metadata": {"name": "resource", "namespace": "example"}}
}
