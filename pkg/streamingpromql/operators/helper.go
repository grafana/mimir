// SPDX-License-Identifier: AGPL-3.0-only

package operators

import "github.com/prometheus/prometheus/model/labels"

func dropMetricName(l labels.Labels, lb *labels.Builder) labels.Labels {
	lb.Reset(l)
	lb.Del(labels.MetricName)
	return lb.Labels()
}
