// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestTargetInfoLabels(t *testing.T) {
	identifying := map[string]string{"service.namespace": "shop", "service.name": "api", "service.instance.id": "a"}
	escaped := TargetInfoTranslation{}
	for name, tc := range map[string]struct {
		descriptive     map[string]string
		keepIdentifying bool
		translation     TargetInfoTranslation
		want            labels.Labels
		wantOK          bool
	}{
		"descriptive attributes, escaped": {
			descriptive: map[string]string{"host.name": "node-1", "k8s.pod.name": "pod-0"},
			translation: escaped,
			want:        labels.FromStrings("__name__", "target_info", "job", "shop/api", "instance", "a", "host_name", "node-1", "k8s_pod_name", "pod-0"),
			wantOK:      true,
		},
		"descriptive attributes, in UTF-8": {
			descriptive: map[string]string{"host.name": "node-1"},
			translation: TargetInfoTranslation{AllowUTF8: true},
			want:        labels.FromStrings("__name__", "target_info", "job", "shop/api", "instance", "a", "host.name", "node-1"),
			wantOK:      true,
		},
		// The conversion writes no target_info for a resource of only the attributes job and instance come from.
		"only identifying attributes": {
			translation: escaped,
		},
		"only identifying attributes, kept": {
			keepIdentifying: true,
			translation:     escaped,
			want:            labels.FromStrings("__name__", "target_info", "job", "shop/api", "instance", "a", "service_namespace", "shop", "service_name", "api", "service_instance_id", "a"),
			wantOK:          true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok, err := TargetInfoLabels(t.Context(), identifying, tc.descriptive, tc.keepIdentifying, tc.translation)
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.want, got)
			}
		})
	}
}
