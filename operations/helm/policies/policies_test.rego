package main

import future.keywords

no_violations {
	trace(sprintf("%v", [deny]))
	count(deny) == 0
}

some_violaions {
	trace(sprintf("%v", [deny]))
	count(deny) > 0
}

test_no_namespace_not_allowed {
	some_violaions with input as {"metadata": {"name": "resource"}}
}

test_empty_namespace_not_allowed {
	some_violaions with input as {"metadata": {"name": "resource", "namespace": ""}}
}

test_null_namespace_not_allowed {
	some_violaions with input as {"metadata": {"name": "resource", "namespace": null}}
}

test_namespace_allowed {
	no_violations with input as {"metadata": {"name": "resource", "namespace": "example"}}
}
