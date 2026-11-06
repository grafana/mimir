package main

gateway_nginx_config(test_name) := config {
	filename := sprintf("../helm/tests/%s-generated/mimir-distributed/templates/gateway/nginx-configmap.yaml", [test_name])
	parsed := parse_combined_config_files([filename])
	config := parsed[_].contents.data["nginx.conf"]
}

count_matches(pattern, config) := count(regex.find_n(pattern, config, -1))

test_gateway_clusterip_distributor_uses_static_upstream {
	config := gateway_nginx_config("test-traffic-distribution-values")
	contains(config, "upstream distributor {")
	contains(config, "server test-traffic-distribution-values-mimir-distributor.citestns.svc.cluster.local.:8080;")
	count_matches(`set \$distributor distributor;`, config) == 3
	count_matches(`proxy_pass +http://\$distributor\$request_uri;`, config) == 3
	not contains(config, "-distributor-headless")
}

test_gateway_headless_distributor_keeps_runtime_resolution {
	config := gateway_nginx_config("test-oss-values")
	not contains(config, "upstream distributor {")
	count_matches(`set \$distributor test-oss-values-mimir-distributor-headless\.citestns\.svc\.cluster\.local\.:8080;`, config) == 3
	count_matches(`proxy_pass +http://\$distributor\$request_uri;`, config) == 3
}

test_gateway_enabled_components_use_static_upstreams {
	config := gateway_nginx_config("test-oss-values")
	contains(config, "upstream ruler {")
	contains(config, "server test-oss-values-mimir-ruler.citestns.svc.cluster.local.:8080;")
	contains(config, "upstream query-frontend {")
	contains(config, "server test-oss-values-mimir-query-frontend.citestns.svc.cluster.local.:8080;")
	contains(config, "upstream compactor {")
	contains(config, "server test-oss-values-mimir-compactor.citestns.svc.cluster.local.:8080;")
	count_matches(`set \$ruler ruler;`, config) == 4
	count_matches(`set \$query_frontend query-frontend;`, config) == 2
	count_matches(`set \$compactor compactor;`, config) == 1
}

test_gateway_alertmanager_keeps_runtime_resolution {
	config := gateway_nginx_config("test-oss-values")
	not contains(config, "upstream alertmanager {")
	count_matches(`set \$alertmanager test-oss-values-mimir-alertmanager-headless\.citestns\.svc\.cluster\.local\.;`, config) == 4
}

test_gateway_disabled_components_have_no_upstreams {
	config := gateway_nginx_config("test-gateway-disabled-components-values")
	count_matches(`upstream [a-z-]+ \{`, config) == 0
	count_matches(`set \$distributor test-gateway-disabled-components-values-mimir-distributor-headless\.citestns\.svc\.cluster\.local\.:8080;`, config) == 3
	count_matches(`set \$ruler test-gateway-disabled-components-values-mimir-ruler\.citestns\.svc\.cluster\.local\.:8080;`, config) == 4
	count_matches(`set \$query_frontend test-gateway-disabled-components-values-mimir-query-frontend\.citestns\.svc\.cluster\.local\.:8080;`, config) == 2
	count_matches(`set \$compactor test-gateway-disabled-components-values-mimir-compactor\.citestns\.svc\.cluster\.local\.:8080;`, config) == 1
}
