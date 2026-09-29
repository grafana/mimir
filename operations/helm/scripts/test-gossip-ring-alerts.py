#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise the chart's rendered gossip-ring alerts and KSM scrape filter.

Run from the repository root: python3 operations/helm/scripts/test-gossip-ring-alerts.py
Requires helm, yq and promtool; no cluster or third-party Python packages.
"""

import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

CHART = Path("operations/helm/charts/mimir-distributed")
VALUES = CHART / "ci/offline/metamonitoring-values.yaml"
ALERT = "MimirGossipMembersEndpointsOutOfSync"


def run(*args):
    result = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise AssertionError(result.stdout)
    return result.stdout


def load_yaml(path):
    return json.loads(run("yq", "-o=json", "-I=0", ".", str(path)))


def render(directory, release, namespace="citestns", fullname=None):
    args = ["helm", "template", release, str(CHART), "-f", str(VALUES),
            "--namespace", namespace, "--output-dir", str(directory)]
    if fullname:
        args += ["--set", "fullnameOverride=" + fullname]
    subprocess.run(args, check=True, capture_output=True, text=True)
    templates = directory / "mimir-distributed/templates"
    rules = load_yaml(templates / "metamonitoring/mixin-alerts.yaml")
    monitor = load_yaml(templates / "metamonitoring/kube-state-metrics-servmon.yaml")
    service = load_yaml(templates / "gossip-ring/gossip-ring-svc.yaml")
    group = next(g for g in rules["spec"]["groups"] if g["name"] == "gossip_alerts")
    alerts = [r for r in group["rules"] if r.get("alert") == ALERT]
    return alerts, monitor, service


def kept(monitor, labels):
    """Apply the rendered Prometheus metricRelabelings to a sample labelset."""
    for rule in monitor["spec"]["endpoints"][0]["metricRelabelings"]:
        value = rule.get("separator", ";").join(labels.get(name, "") for name in rule["sourceLabels"])
        if rule["action"] == "keep" and not re.fullmatch(rule["regex"], value):
            return False
    return True


def expected_alert(rule, labels):
    annotations = {name: value.replace("{{ $labels.cluster }}", labels["cluster"])
                   .replace("{{ $labels.namespace }}", labels["namespace"])
                   for name, value in rule["annotations"].items()}
    return {"exp_labels": {**labels, "severity": rule["labels"]["severity"]},
            "exp_annotations": annotations}


class GossipRingDeploymentTest(unittest.TestCase):
    def test_rendered_rules_select_the_release_service_and_ksm_keeps_it(self):
        for release, namespace, fullname in [
            ("first", "citestns", None),
            ("second", "another-ns", None),
            ("third", "citestns", "custom-mimir"),
        ]:
            with self.subTest(release=release), tempfile.TemporaryDirectory() as tmp:
                alerts, monitor, service = render(Path(tmp), release, namespace, fullname)
                endpoint = service["metadata"]["name"]
                original = load_yaml(Path("operations/mimir-mixin-compiled/alerts.yaml"))
                original_alerts = [rule for group in original["groups"] for rule in group["rules"]
                                   if rule.get("alert") == ALERT]
                self.assertEqual(2, len(alerts))
                self.assertEqual(2, len(original_alerts))
                self.assertEqual({"warning", "critical"}, {a["labels"]["severity"] for a in alerts})
                for rule, source in zip(alerts, original_alerts):
                    expr = rule["expr"]
                    self.assertEqual(2, expr.count('kube_endpoint_address{endpoint="' + endpoint + '", namespace="' + namespace + '"}'))
                    self.assertNotIn('endpoint="gossip-ring"', expr)
                    self.assertEqual(source["expr"].replace('kube_endpoint_address{endpoint="gossip-ring"}',
                                                            'kube_endpoint_address{endpoint="' + endpoint + '", namespace="' + namespace + '"}'), expr)
                    self.assertEqual('15m' if rule["labels"]["severity"] == 'warning' else '5m', rule["for"])
                sample = {"__name__": "kube_endpoint_address", "namespace": namespace, "endpoint": endpoint}
                self.assertTrue(kept(monitor, sample))
                self.assertFalse(kept(monitor, {**sample, "endpoint": "other-mimir-gossip-ring"}))
                self.assertFalse(kept(monitor, {**sample, "namespace": "different-ns"}))
                self.assertFalse(kept(monitor, {**sample, "__name__": "unrelated_metric"}))
                self.assertTrue(kept(monitor, {"__name__": "kube_pod_info", "pod": endpoint.replace("gossip-ring", "ingester-0")}))

    def test_promtool_replays_both_severities_on_rendered_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            alerts, _, service = render(directory, "first")
            endpoint = service["metadata"]["name"]
            rules = directory / "rules.json"
            rules.write_text(json.dumps({"groups": [{"name": "gossip_alerts", "rules": alerts}]}))
            common = {"cluster": "test-cluster", "namespace": "citestns"}
            series = [{"series": 'cortex_build_info{cluster="test-cluster",namespace="citestns"}', "values": "1x40"}]
            for idx in range(10):
                series.append({"series": 'kube_endpoint_address{cluster="test-cluster",namespace="citestns",endpoint="%s",ip="10.0.0.%s"}' % (endpoint, idx), "values": "1x40"})
            for idx in range(7):
                series.append({"series": 'kube_pod_info{cluster="test-cluster",namespace="citestns",pod_ip="10.0.0.%s"}' % idx, "values": "1x40"})
            # Another release in the same namespace is very unhealthy but must not enter this alert.
            for idx in range(9):
                series.append({"series": 'kube_endpoint_address{cluster="test-cluster",namespace="citestns",endpoint="other-mimir-gossip-ring",ip="10.1.0.%s"}' % idx, "values": "1x40"})
            healthy_pods = [{"series": 'kube_pod_info{cluster="test-cluster",namespace="citestns",pod_ip="10.0.0.%s"}' % idx, "values": "1x40"} for idx in range(7, 10)]
            tests = {"rule_files": [str(rules)], "evaluation_interval": "1m", "tests": [{
                "interval": "1m", "input_series": series, "alert_rule_test": [{
                    "eval_time": "16m", "alertname": ALERT,
                    "exp_alerts": [expected_alert(alerts[0], common)],
            }]}, {
                "interval": "1m", "input_series": [series[0]] + series[1:11] + series[11:13],
                "alert_rule_test": [{"eval_time": "16m", "alertname": ALERT,
                                     "exp_alerts": [expected_alert(a, common) for a in alerts]}],
            }, {
                "interval": "1m", "input_series": series + healthy_pods,
                "alert_rule_test": [{"eval_time": "16m", "alertname": ALERT, "exp_alerts": []}],
            }, {
                "interval": "1m", "input_series": series,
                "alert_rule_test": [{"eval_time": "4m", "alertname": ALERT, "exp_alerts": []}],
            }]}
            fixture = directory / "test.json"
            fixture.write_text(json.dumps(tests))
            self.assertIn("SUCCESS", run("promtool", "test", "rules", str(fixture)))


if __name__ == "__main__":
    unittest.main()
