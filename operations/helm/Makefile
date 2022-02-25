chart:
	jb init || jb install
	jb install ../mimir
	jb install https://github.com/jsonnet-libs/k8s-libsonnet/1.21@main
	echo "(import 'github.com/jsonnet-libs/k8s-libsonnet/1.21/main.libsonnet')" > k.libsonnet
	tk show --dangerous-allow-redirect chart.jsonnet | helmify -v chart/mimir

jbupdate:
	jb update

update: jbupdate chart

.PHONY: chart
